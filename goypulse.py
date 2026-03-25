# requires: cryptography
import asyncio, base64, hashlib, hmac, json, math, os, random, re, sqlite3, time, zlib, threading, urllib.error, urllib.parse, urllib.request
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional, Set, Tuple
from telethon import events, utils as tl_utils
from telethon.tl.types import Message
from telethon.tl.functions.channels import CreateChannelRequest, JoinChannelRequest
from .. import loader, utils

try:
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ed25519, x25519
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM, ChaCha20Poly1305
    from cryptography.hazmat.primitives.kdf.hkdf import HKDF
    CRYPTO_READY = True
except Exception:
    InvalidSignature = Exception
    hashes = serialization = ed25519 = x25519 = AESGCM = ChaCha20Poly1305 = HKDF = None
    CRYPTO_READY = False
TOK_RE = re.compile(r"[a-zа-яё0-9_]+", re.I)
URL_RE = re.compile(r"https?://\S+")
STOP_W = {"и", "в", "во", "на", "не", "что", "это", "я", "ты", "он", "она", "оно", "мы", "вы", "они", "а", "но", "или", "да", "нет", "ну", "как", "так", "к", "ко", "из", "за", "по", "у", "от", "до", "же", "ли", "бы", "то", "для", "если", "уже", "тут", "там", "ведь", "вот", "даже", "лишь", "о", "об", "очень", "с", "со", "тоже", "только", "чем", "чтобы", "этом", "эти", "этого", "какой", "просто", "может", "раз", "два", "типа", "короче", "кст", "кстати", "вообще", "наверное", "вроде", "кажется", "однако", "хотя", "хоть", "между", "через", "около", "будто", "словно", "ровно", "почти", "вдруг", "разве", "неужели", "снова", "опять", "все", "всё", "вся", "весь", "всех", "всем", "всеми", "всею", "всея", "меня", "мне", "тебя", "тебе", "его", "ее", "её", "их", "наш", "ваш", "свой", "кто", "чей", "этот", "тот", "мой", "твой", "сам", "самый", "весь", "вся", "всё", "все", "зачем", "почему", "когда", "где", "куда", "откуда", "есть", "быть", "был", "была", "было", "были", "хочу", "хочет", "будет", "будут", "твоя", "мое", "моё", "the", "a", "an", "and", "or", "but", "is", "are", "am", "was", "were", "be", "been", "being", "in", "on", "at", "to", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "from", "up", "down", "of", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "can", "will", "just", "should", "now", "could", "would", "which", "who", "whom", "whose", "this", "that", "these", "those", "my", "your", "his", "her", "its", "our", "their", "mine", "yours", "hers", "ours", "theirs", "me", "him", "us", "them", "myself", "yourself", "himself", "herself", "itself", "ourselves", "themselves", "whose", "which", "what", "where", "when", "why", "how", "all", "another", "any", "anybody", "anyone", "anything", "both", "each", "either", "everybody", "everyone", "everything", "few", "many", "neither", "nobody", "none", "no one", "nothing", "one", "other", "others", "some", "somebody", "someone", "something", "such", "that", "these", "this", "those", "each", "which", "who", "whom", "whose", "as", "at", "by", "for", "from", "in", "into", "of", "off", "on", "onto", "out", "over", "to", "up", "with", "yet", "above", "below", "beside", "between", "beyond", "during", "except", "near", "past", "since", "through", "toward", "under", "until", "upon", "within", "without", "че", "чё", "чо", "кароч", "кароче", "типо", "ваще", "ващето", "вобще", "походу", "плз", "плиз", "спс", "пасиб", "пасиба", "пж", "дя", "нее", "неа", "нука", "мол", "типатого", "прям", "тока", "пока", "прив", "ку", "hi", "hey", "hello", "yo", "sup", "howdy", "morning", "afternoon", "evening", "night", "goodbye", "bye", "see ya", "later", "please", "thanks", "thank you", "thx", "welcome", "yw", "sorry", "excuse me", "yes", "yeah", "yep", "no", "nope", "nah", "maybe", "perhaps", "actually", "basically", "literally", "totally", "definitely", "absolutely", "probably", "sure", "ok", "okay", "alright", "fine", "cool", "great", "awesome", "good", "bad", "well", "very", "much", "many", "little", "few", "enough", "too", "so", "very", "really", "quite", "rather", "pretty", "somewhat", "almost", "nearly", "mostly", "partly", "half", "least", "most", "more", "less", "bit", "piece", "way", "far", "near", "long", "short", "high", "low", "big", "small", "new", "old", "young", "early", "late", "fast", "slow", "hard", "easy", "clear", "dark", "light", "heavy", "soft", "loud", "quiet", "strong", "weak", "rich", "poor", "hot", "cold", "warm", "cool", "dry", "wet", "clean", "dirty", "full", "empty", "open", "closed", "first", "last", "right", "wrong", "true", "false", "real", "fake"}
GRTS = {"привет", "хай", "здарова", "ку", "дарова", "салам", "куку", "добрый", "вечер", "утро", "шалом", "qq", "прив", "здрасьте", "вечерочек", "утречко", "салют", "здравствуй", "приветствую", "алоха", "дратути", "кусь", "сап", "йо", "кукушки", "здоров", "превед", " хаюшки", "добрейший", "конишуа", "бонжур", "гутен", "таг", "приветс", "дароу", "салам алейкум", "ассаламу", "hello", "hi", "hey", "greetings", "morning", "yo", "sup", "howdy", "hiya", "evening", "afternoon", "welcome", "aloha", "shalom", "hola", "bonjour", "ciao", "namaste", "heyyo", "hibro", "hi there", "wassup", "whats up", "good morning", "good evening", "good afternoon", "nice to see you", "hey there", "it's been a while", "long time no see", "lovely to meet you", "how's it going", "how are you", "what's new", "how's life", "how's things", "morning all", "hi everyone", "hello folks", "доброе", "утречко", "вечер", "в хату", "здравия", "желаю", "почтение", "приветствую всех", "хайль", "ave", "здорово", "привет", "доброе утро", "добрый день", "добрый вечер", "доброй ночи", "хайль", "здравия желаю", "низкий поклон", "моё почтение", "честь имею", "барев", "салам алейкум", "уалейкум ассалам", "нихао", "ола", "аннён", "дзякуй", "вечер в радость", "здорово бандиты", "здорово жиганы", "всем ку", "кукусики", "приветики", "даровчики", "приветствую", "добрейшего денёчка", "утреца", "доброй ночи", "салам пополам", "здаристи", "здрасьте мордасьте", "куку", "qq", "q-q", "ку", "hi", "hey", "hello", "greetings", "salutations", "morning", "evening", "afternoon", "good day", "howdy", "sup", "yo", "wassup", "welcome", "aloha", "bonjour", "ciao", "hola", "namaste", "shalom", "hiya", "hey there", "hi there", "long time no see", "lovely to meet you", "nice to meet you", "how's it going", "how are you", "how have you been", "what's up", "what's going on", "what's new", "good to see you", "pleasure to meet you", "it's an honor", "greetings everyone"}
RCTS = {"ахах", "лол", "жиза", "жестко", "имба", "топ", "пон", "кринж", "база", "хах", "треш", "ору", "рил", "мда", "пздц", "пиздец", "ебать", "бля", "блять", "чел", "капец", "шок", "ужас", "жесть", "согл", "базару", "факты", "хуйня", "дичь", "ор", "ржомба", "рофл", "кринге", "понял", "ясно", "хуй", "пизда", "охренеть", "охуеть", "писец", "бб", "ок", "окей", "окда", "спс", "спасибо", "сигма", "вумен", "скуф", "нормис", "альтушка", "масик", "тюбик", "штрих", "чечик", "имбово", "разрывная", "свэг", "чиназес", "легенда", "гигачад", "базированно", "кринжатина", "дэмн", "люто", "чертовски", "пиздато", "офигенно", "кайф", "кайфово", "найс", "ладно", "бебра", "абуз", "тильт", "флекс", "шейм", "горишь", "слит", "попуск", "агро", "душно", "душнила", "ахаха", "лолз", "мем", "рофлишь", "кринжую", "нереально", "круто", "четко", "красава", "красавчик", "харош", "хорош", "гений", "легендарно", "сильно", "жёстко", "безумно", "lmao", "rofl", "wtf", "omg", "bruh", "based", "cringe", "fr", "frfr", "ong", "tbh", "ngl", "idk", "idc", "stfu", "gtfo", "lmfao", "damn", "sheesh", "bet", "cap", "nocap", "dope", "lit", "trash", "awesome", "cool", "sick", "wild", "insane", "legit", "standard", "classic", "vibes", "mood", "shook", "dead", "skull", "че за", "шо за", "пиздец", "ппц", "ну и ну", "ох", "ах", "ого", "ух ты", "нифига", "ничоси", "воу", "эщкере", "вуху", "ура", "еее", "йоу", "хоспади", "господи", "боже мой", "матерь божья", "бляха муха", "сука", "епт", "епта", "ебаный рот", "пидорас", "гандон", "хуило", "еблан", "дебил", "даун", "лошара", "чушпан", "пацаны", "ребята", "братва", "брат", "братик", "бро", "кентуфурик", "кореш", "дружбан", "парень", "девушка", "тян", "кун", "лоля", "вайфу", "краш", "шип", "шипперить", "кринжануть", "рофляночка", "шуточка", "прикол", "кек", "кеке", "кекаю", "пушка", "бомба", "ракета", "космос", "вышка", "огонь", "горячо", "жара", "мощно", "крутяк", "заебись", "охуенно", "чётко", "збс", "гг", "wp", "gl", "hf", "ggwp", "ez", "ezez", "сидим", "кайфуем", "отдыхаем", "работаем", "учимся", "спать", "жрать", "бухать", "курить", "парить", "жидкость", "жижа", "под", "вейп", "электронка", "ашка", "одноразка", "кальян", "пиво", "водка", "виски", "вино", "коньяк", "вискарь", "текила", "ром", "джин", "шампусик", "шампанское", "лимонад", "кола", "пепси", "энергетик", "монстр", "редбулл", "адреналин", "флэш", "торч", "солевой", "наркоман", "алкаш", "бомж", "бич", "нищий", "богатый", "мажор", "нищеброд", "нищий", "лох", "терпила", "куколд", "омежка", "сигмач", "гигачад", "папич", "величайший", "видос", "стрим", "видосик", "видяха", "карта", "проц", "комп", "пк", "ноут", "клава", "мышка", "моник", "наушники", "уши", "микро", "вебка", "телефон", "смартфон", "айфон", "самсунг", "андроид", "дискорд", "тг", "телеграм", "вк", "инста", "тикток", "ютуб", "твич", "кик", "сайт", "интернет", "сеть", "вайфай", "скорость", "пинг" , "лаги", "фризы", "баги", "ошибки", "еррор", "хелп", "помогите", "спасите", "админ", "модер", "владелец", "овнер", "создатель", "хуйло", "красавец", "молодец", "умничка", "солнышко", "зайка", "котик", "киса", "лапочка", "милота", "кавай", "ня", "няшка", "ня кавай", "ураа", "еее", "крутотенюшка", "лучший", "лучшая", "the best", "classic", "standard", "norm", "normal", "okey", "fine", "good", "nice", "very good", "excellent", "perfect", "amazing", "wonderful", "terrible", "awful", "bad", "badly", "horrible", "shit", "holy shit", "omg", "wtf", "wth", "lmao", "lmfao", "lol", "rofl", "bruh", "damn", "sheesh", "fr", "frfr", "real", "really", "literally", "literally me", "me", "mood", "vibes", "vibe", "aesthetic", "standard", "basic", "premium", "lux", "luxury", "rich", "poor", "money", "cash", "dollars", "bucks", "rubles", "crypto", "bitcoin", "eth", "nft", "scam", "scammer", "legit", "safe", "scary", "fear", "horror", "spooky", "creepy", "weird", "strange", "bizarre", "odd", "funny", "hilarious", "joke", "prank", "troll", "trolling", "hater", "fan", "fandom", "stan", "simp", "incel", "femcel", "chad", "gigachad", "sigma", "alpha", "beta", "omega", "cuck", "cuckold", "soyboy", "npc", "main character", "hero", "villain", "boss", "noob", "pro", "hacker", "cheater", "admin", "mod", "staff", "user", "player", "game", "gaming", "stream", "steamer", "video", "content", "creator", "famous", "popular", "viral", "tranding", "tags", "reposts", "likes", "views", "subscribers", "subs", "goat", "legend", "icon", "masterpiece", "peak", "mid", "flop", "w", "l", "massive w", "huge l", "ratio", "canceled", "cancelled", "toxic", "wholesome", "cursed", "blessed", "blursed", "sus", "imposter", "amongus", "amogus", "vent", "sussy", "baka", "pog", "poggers", "pogchamp", "kekw", "omegalul", "pepeh", "kappa", "sadge", "monkas", "feelsbadman", "feelsgoodman", "clap", "ez", "ggwp", "get rekt", "rekt", "destroyed", "owned", "skill issue", "noob", "get gud", "cope", "seethe", "mald", "cry about it", "stay mad", "touch grass", "ratio", "owned", "powned", "pwned", "clapped", "dumped", "washed", "washed up", "fraud", "overrated", "underrated", "sleeper", "banger", "slaps", "fire", "heat", "cold", "frozen", "icy", "drip", "drippy", "swag", "yolo", "swag", "gucci", "prada", "hype", "hypebeast", "og", "real one", "homie", "bestie", "brother", "sister", "fam", "squad", "crew", "gang", "tribe", "folk", "folks", "peeps", "people"}
QW = ("что", "как", "почему", "зачем", "когда", "где", "кто", "кого", "кому", "кем", "чем", "откуда", "куда", "чей", "че", "чё", "чо", "хули", "всмысле", "какого", "хто", "шо", "какой", "какая", "какие", "херли", "какого", "хрена", "поч", "почему бы", "почем", "зачем это", "хто то", "куда это", "откуда это", "какие новости", "че за", "чё за", "шо за", "че там", "чё там", "шо там", "what", "how", "why", "when", "where", "who", "whom", "whose", "which", "how come", "what for", "whats up", "whats going on", "what about", "че почем", "чё почём", "шо почём", "сколько", "скока", "скоко", "почём", "за сколько", "на фига", "нафига", "на хуя", "нахуя", "какого хуя", "че за фигня", "че за дичь", "чё за треш", "как это", "что это", "кто это", "где это", "когда это", "почему так", "зачем ты", "что делаешь", "как дела", "чё каво", "чё кого", "шо там", "что нового", "какие планы", "ты где", "вы где", "мы где", "куда идем", "что купить", "сколько стоит", "поможешь", "сможешь", "хочешь", "знаешь", "помнишь", "слышал", "видел", "what", "how", "why", "when", "where", "who", "whom", "whose", "which", "how come", "what for", "whats up", "whats going on", "what about", "how many", "how much", "how long", "how far", "how often", "who is", "what is", "where is", "when is", "why is", "can you", "could you", "would you", "do you", "did you", "have you", "are you", "is it", "will you", "shall we", "may I", "what's that", "who's there", "whose turn", "any news", "any idea", "anyone know", "how to", "why not", "what if", "is there", "are there", "shall I", "should I", "could I")
EMO_M = {"смех": ["😂", "🤣", "💀", "😭", "хах", "ахах", "😹", "🤭", "пхпх", "хахаха", "ору", "🤣", "😅", "😆", "😸", "😂", "🤣", "💀", "😭", "хах", "ахах", "😹", "🤭", "пхпх", "хахаха", "ору", "🤣", "😅", "😆", "😸", "😁", "😃", "😄", "😅", "😆", "😅", "😂", "🤣", "😹", "😸", "😻", "😽", "🫠", "🙃", "🤪", "😝", "😜", "😛", "🤤", "😤", "🤯", "🥳", "😎", "🤡", "👺", "👻", "👽", "💩", "🔥", "💯", "💥", "⚡️", "✨", "🌟"], "агр": ["🤡", "🤬", "🗿", "👺", "мда", " трэш", "😤", "🤦‍♂️", "🤦‍♀️", "☠️", "🗑️", "😡", "👿", "🖕", "😠", "👿", "👹", "🖕", "🤬", "💢", "🤡", "🤬", "🗿", "👺", "мда", "трэш", "😤", "🤦‍♂️", "🤦‍♀️", "☠️", "🗑️", "😡", "👿", "🖕", "😠", "👿", "👹", "🖕", "🤬", "💢", "👎", "🤮", "💩", "🧨", "🔫", "🗡️", "🔪", "⛓️", "💣", "🚬", "🥀", "🔨", "⚒️", "🛠️", "⛏️", "🪚", "🪓", "🧱", "🪨", "🪵", "⛓️", "💣", "🧨", "💥", "🗡️", "⚔️", "🏹", "🛡️", "⚰️", "🪦", "⚱️", "🏺"], "нейтрал": ["👀", "🤔", "пон", "🚬", "ну ок", "ладно", "🤷‍♂️", "🤷‍♀️", "🙃", "🧐", "🥱", "🥴", "😶", "🌝", "🌚", "🫠", "😑", "😐", "👀", "🤔", "пон", "🚬", "ну ок", "ладно", "🤷‍♂️", "🤷‍♀️", "🙃", "🧐", "🥱", "🥴", "😶", "🌝", "🌚", "🫠", "😑", "😐", "🚶‍♂️", "🚶‍♀️", "🪴", "☁️", "🌊", "☕️", "🛋️", "💻", "📚", "🖊️", "📅", "📎", "💤", "💬", "💭", "🗯️", "♠️", "♣️", "♥️", "♦️", "🃏", "🎴", "🎭", "🎨", "🧵", "🧶", "🎹", "🎺", "🎸", "🎻", "🥁", "🪗", "🎧", "🎤", "🎬"], "шок": ["😱", "🤯", "😳", "😨", "🙀", "охуеть", "😲", "😯", "😧", "😮", "😵", "😵‍💫", "😱", "🤯", "😳", "😨", "🙀", "охуеть", "😲", "😯", "😧", "😮", "😵", "😵‍💫", "‼️", "❓", "🆘", "💥", "🔥", "💨", "🌊", "🌩️", "⛈️", "🌪️", "🌊", "🌋", "☄️", "⚡️", "💥", "🔥", "🧨", "💣", "🔫", "⛏️", "⚔️", "🛡️", "⚰️", "🪦", "👻", "👹", "👺", "💀", "👽", "💩", "🤡", "🧞‍♂️", "🧞‍♀️", "🧟‍♂️", "🧟‍♀️"]}


@dataclass
class MObj:
    mid: int
    txt: str
    tks: Tuple[str, ...]
    hm: bool = False
    mk: str = ""
    sender_id: int = 0
    cid: int = 0



@dataclass
class CSt:
    on: bool = False
    lim: int = 25000
    min_m: int = 25
    r_ch: int = 38
    m_ch: int = 25
    my_ch: int = 100
    cd_m: int = 4
    cd_x: int = 12
    msgs: Deque[MObj] = field(default_factory=lambda: deque(maxlen=8000))
    rec: Deque[MObj] = field(default_factory=lambda: deque(maxlen=150))
    tfq: Counter = field(default_factory=Counter)
    mkv: Dict[Tuple[str, str], Counter] = field(default_factory=lambda: defaultdict(Counter))
    mkv3: Dict[Tuple[str, str, str], Counter] = field(default_factory=lambda: defaultdict(Counter))
    mkv4: Dict[Tuple[str, str, str, str], Counter] = field(default_factory=lambda: defaultdict(Counter))
    mds: Deque[MObj] = field(default_factory=lambda: deque(maxlen=1000))
    md_cnt: Counter = field(default_factory=Counter)
    w_cnt: int = 0
    my_msgs: Deque[int] = field(default_factory=lambda: deque(maxlen=500))
    my_outs: Deque[str] = field(default_factory=lambda: deque(maxlen=50))
    ign: Set[int] = field(default_factory=set)
    usr_cd: Dict[int, float] = field(default_factory=dict)
    last_mid: int = 0
    parsed_cnt: int = 0
    cd_u: float = 0.0
    mute_u: float = 0.0
    lrn: bool = False
    last_usr: int = 0
    last_tone: str = "нейтрал"
    last_t: float = 0.0
    cid: int = 0


@loader.tds
class GoyPulseMod(loader.Module):
    """🧠 Нейро-автоответчик GoyPulse V9 by goy(@samsepi0l_ovf)"""
    strings = {
        "name": "GoyPulse V9",
        "brand": "GoyPulse V9 by goy(@samsepi0l_ovf)",
        "og": "🛡️ <b>[GoyPulse]</b> Только для групп.",
        "on": "⚡ <b>[GoyPulse]</b> Система активирована.\n<i>Теперь я обучаюсь и буду отвечать в этом чате.</i>",
        "off": "💤 <b>[GoyPulse]</b> Система деактивирована.\n<i>Я больше не буду отвечать здесь.</i>",
        "ref_st": "🧬 <b>[Обучение]</b> Анализ истории сообщений...{}",
        "ref_upd": "🧠 <b>[Обучение]</b> В процессе... <code>[{}{}]</code>\n\n📊 <b>Статистика:</b>\n├─ 💠 Словарь: <code>{}</code>\n├─ 🖼️ Медиа: <code>{}</code>\n├─ 📚 Слова: <code>{}</code>\n└─ ⚡ Скорость: <code>{}</code> msg/s\n\n⏳ <b>Осталось:</b> <code>{}</code>",
        "ref_dn": "✅ <b>[Обучение]</b> Успешно завершено!\n\n📈 <b>Итоги:</b>\n├─ 💬 Сообщений: <code>{}</code>\n├─ 🧠 Словарь: <code>{}</code>\n├─ 📚 Всего слов: <code>{}</code>\n└─ 🖼️ Медиа: <code>{}</code> {md_details}\n\n<i>GoyPulse готов к работе!</i>",
        "st": "📊 <b>Статус GoyPulse V9</b> | <code>by goy(@samsepi0l_ovf)</code>\n\n🛰️ <b>Состояние:</b> {on}\n📈 <b>База:</b> <code>{pc}</code> msg | 📚 <code>{wc}</code> слов\n🧠 <b>Словарь:</b> <code>{vk}</code> связок\n🖼️ <b>Медиа:</b> <code>{md}</code> | 🚫 <b>Игнор:</b> <code>{ig}</code>\n\n⚙️ <b>Конфигурация:</b>\n├─ 🎲 Шанс (обыч): <code>{c}%</code>\n├─ 🔄 Шанс (реплай): <code>{my}%</code>\n├─ 🎨 Шанс (медиа): <code>{mc}%</code>\n└─ ⏳ Задержка: <code>{cd}</code>\n\n🗣️ <b>Актуальные темы:</b>\n<code>{tw}</code>{warn}",
        "set": "⚙️ <b>[Настройки]</b> Параметр <code>{}</code> обновлен: <code>{}</code>",
        "mute": "🤫 <b>[Тсс!]</b> Бот отправлен отдыхать на <code>{}</code> мин.",
        "kill": "🛑 <b>[HALT]</b> Глобальная остановка всех модулей GoyPulse.",
        "info": "🧠 <b>[Аналитика]</b> Вибрации чата\n\n📡 <b>Активность:</b> <code>{act}</code>\n🎭 <b>Тональность:</b>\n{tonality}\n\n🔥 <b>Топ обсуждений:</b>\n<code>{tw}</code>{warn}",
        "ign_add": "🚷 <b>[Игнор]</b> Пользователь добавлен в черный список.",
        "ign_del": "✅ <b>[Игнор]</b> Пользователь удален из черного списка.",
        "clr": "🗑️ <b>[Очистка]</b> Память текущего чата полностью стерта.",
        "rst_ok": "🔄 <b>[Сброс]</b> Настройки и память сброшены успешно.",
        "log_err": "⚠️ <b>[ERROR]</b> Ошибка: <code>{}</code>",
        "log_ok": "✅ <b>[Stealth]</b> Команда выполнена в <code>{}</code>\n\n📄 <b>Ответ:</b>\n<code>{}</code>",
        "bp_up": "📦 <b>Бэкап создан</b>: <code>{}</code>",
        "bp_dn": "📥 <b>[Бэкап]</b> Данные успешно восстановлены!\n\n📊 <b>Сводка:</b>\n├ 📁 Чатов: <code>{chats}</code>\n├ 🧠 Слов: <code>{words}</code>\n├ 🔗 Связок: <code>{links}</code>\n└ 🚷 Игнор-лист: <code>{ign}</code>\n\n📜 <b>Восстановленные чаты:</b>\n{chat_list}",
        "bp_chk": "🛡️ Проверка структуры и целостности бэкапа...",
        "bp_vld": "✅ Проверка пройдена. Подготовлено <code>{chats}</code> чатов.",
        "bp_run": "⚙️ Восстановление данных в БД...",
        "bp_err": "❌ Ошибка бэкапа: <code>{}</code>",
        "bp_help": "💾 <b>Режимы .gpbackup</b> | <code>GoyPulse V9 by goy(@samsepi0l_ovf)</code>\n\n"
                   "<code>.gpbackup all</code> — бэкап всех чатов\n"
                   "<code>.gpbackup here</code> — бэкап текущего чата\n"
                   "<code>.gpbackup &lt;chat1&gt; [chat2 ...]</code> — выборочный бэкап\n"
                   "<code>.gpbackup share &lt;user&gt; &lt;targets...&gt;</code> — шифрованный бэкап для другого пользователя\n"
                   "<code>.gpbackup trust &lt;user|reply&gt;</code> — обмен публичными ключами\n"
                   "<code>.gpbackup status</code> — статус подсистемы",
        "bp_restore_force": "⚠️ Восстановление перезапишет данные в выбранных чатах.",
        "bp_restore_cancel": "⛔ Восстановление отменено.",
        "bp_no_crypto": "❌ Модуль cryptography недоступен. GPB2-шифрование недоступно.",
        "bp_trust_sent": "✅ Публичный ключ отправлен пользователю <code>{}</code>.",
        "bp_trust_imported": "✅ Ключ пользователя <code>{}</code> импортирован.",
        "bp_trust_missing": "❌ Нет доверенного ключа для пользователя <code>{}</code>.",
        "react_ok": "✨ <b>[Реакция]</b> Бот отреагировал на сообщение.",
        "h_pulse": "🔌 <b>[Usage] .gpulse [on|off]</b>\n\nВключает или полностью отключает обработку сообщений ботом в текущем чате.\n\n<b>Инструкция:</b>\n├ <code>.gpulse on</code> — Бот начинает слушать чат, обучаться и отвечать согласно настройкам.\n└ <code>.gpulse off</code> — Бот полностью игнорирует всё происходящее в этом чате.\n\n<i>Примечание: Если база пуста, бот напомнит о необходимости обучения (.gpref).</i>\n\n<code>GoyPulse V9 by goy(@samsepi0l_ovf)</code>",
        "h_set": "⚙️ <b>Usage: .gpset &lt;параметр&gt; &lt;значение&gt; [target_group]</b>\n\n"
                 "<b>Глобальные параметры (работают в любом чате):</b>\n"
                 "<code>bpon</code> <code>bpint</code> <code>react</code> <code>logerr</code> <code>logstl</code> <code>logbkp</code> <code>loglrn</code> <code>logans</code>\n\n"
                 "<b>Параметры группы (только в группе или с явным target_group):</b>\n"
                 "<code>lim</code> <code>min</code> <code>ch</code> <code>mch</code> <code>mych</code> <code>cdm</code> <code>cdx</code>\n\n"
                 "<b>Примеры:</b>\n"
                 "<code>.gpset bpint 30</code>\n"
                 "<code>.gpset ch 45</code>\n"
                 "<code>.gpset ch 45 -1001234567890</code>\n\n"
                 "<code>GoyPulse V9 by goy(@samsepi0l_ovf)</code>",
        "h_mute": "🔇 <b>[Usage] .gpmute <минуты></b>\n\nВременно отключает ответы бота, сохраняя процесс обучения и сбора статистики.\n\n<b>Примеры:</b>\n├ <code>.gpmute 30</code> — Замолчать на полчаса.\n├ <code>.gpmute 1440</code> — Замолчать на сутки.\n└ <code>.gpmute 0</code> — Снять ограничение немедленно.\n\n<code>GoyPulse V9 by goy(@samsepi0l_ovf)</code>",
        "h_ign": "🚷 <b>[Usage] .gpignore</b>\n\nДобавляет или удаляет пользователя из черного списка бота.\n\n<b>Как использовать:</b>\n1. Найдите сообщение пользователя.\n2. Ответьте на него (Reply) командой <code>.gpignore</code>.\n\n<i>Результат: Бот не будет обучаться на нем и не будет ему отвечать.</i>\n\n<code>GoyPulse V9 by goy(@samsepi0l_ovf)</code>",
        "h_gph": "🕵️ <b>[Usage] .gph <цель> <команда></b>\n\nВыполнение команд GoyPulse в любом чате анонимно.\n\n<b>Параметры:</b>\n├ <code>цель</code> — ID чата, юзернейм или слово <code>here</code>\n└ <code>команда</code> — Любая команда без точки (напр. <code>gpstat</code>)\n\n<b>Примеры:</b>\n├ <code>.gph -100... gpstat</code> — Статус чужого чата.\n├ <code>.gph @username gpinfo</code> — Вайб в личке.\n└ <code>.gph here gpclear</code> — Скрытая очистка.\n\n<code>GoyPulse V9 by goy(@samsepi0l_ovf)</code>",
        "upd_offer": "🛰️ <b>Доступно обновление GoyPulse</b>\n├ Текущая: <code>{cur}</code>\n├ Новая: <code>{new}</code>\n├ Mandatory: <code>{mandatory}</code>\n└ Источник: <code>GitHub</code>\n\n{note}\n<code>GoyPulse V9 by goy(@samsepi0l_ovf)</code>",
        "upd_lock": "🔒 <b>Ограниченный режим GoyPulse V9</b>\nПричина: <code>{reason}</code>\nРазрешены recovery/update пути: <code>.gpupdate</code>, <code>.gprestore</code>, <code>.gpbackup</code>.\n\n<code>by goy(@samsepi0l_ovf)</code>",
        "upd_none": "✅ <b>Обновлений нет</b> | <code>GoyPulse V9 by goy(@samsepi0l_ovf)</code>",
        "upd_fail": "❌ <b>Проверка обновлений отклонена</b>\n<code>{}</code>",
        "upd_ok": "✅ <b>Обновление применено</b>\nВерсия: <code>{}</code>\nФайл обновлён, перезагрузи модуль для активации.\n\n<code>GoyPulse V9 by goy(@samsepi0l_ovf)</code>",
        "upd_postpone": "⏳ Проверка отложена до <code>{}</code>.",
        "upd_skip": "🚫 Версия <code>{}</code> помечена как пропущенная.",
        "sub_offer": "📡 <b>Подписка на канал обновлений</b>\nКанал: <code>@goy_ai</code>\n\nПодписаться сейчас?\n<code>GoyPulse V9 by goy(@samsepi0l_ovf)</code>",
        "sub_yes": "✅ Подписка на <code>@goy_ai</code> выполнена.",
        "sub_no": "👌 Подписка пропущена.",
        "sub_fail": "⚠️ Не удалось подписаться автоматически: <code>{}</code>",

    }
    def __init__(self):
        cv, vi, vb = loader.ConfigValue, loader.validators.Integer(), loader.validators.Boolean()
        self.config = loader.ModuleConfig(
            cv("d_lim", 25000, lambda: "Лимит парсинга", validator=vi),
            cv("d_min", 25, lambda: "Мин. сообщений", validator=vi),
            cv("d_ch", 38, lambda: "Шанс ответа (%)", validator=vi),
            cv("d_mch", 25, lambda: "Шанс медиа (%)", validator=vi),
            cv("d_mych", 100, lambda: "Шанс реплая (%)", validator=vi),
            cv("d_cdm", 4, lambda: "Мин. пауза", validator=vi),
            cv("d_cdx", 12, lambda: "Макс. пауза", validator=vi),
            cv("bp_on", 1, lambda: "Автобэкап (1/0)", validator=vi),
            cv("bp_int", 30, lambda: "Интервал бэкапа (мин)", validator=vi),
            cv("react_ch", 15, lambda: "Шанс реакции (%)", validator=vi),
            cv("log_err", True, lambda: "Лог: Ошибки", validator=vb),
            cv("log_stl", True, lambda: "Лог: Скрытый режим", validator=vb),
            cv("log_bkp", True, lambda: "Лог: Бэкапы", validator=vb),
            cv("log_lrn", True, lambda: "Лог: Обучение", validator=vb),
            cv("log_ans", False, lambda: "Лог: Ответы бота", validator=vb)
        )
        self._c = None; self._db = None
        self._chs: Dict[int, CSt] = defaultdict(CSt)
        self._glob_stop = False

        self._df = "goypulse_v8_brain.json"
        self._db_path = "goypulse_v8.db"
        self._my_id = 0
        self._log_ch = 0
        self._db_conn = None
        self._sql_lock = None
        self._sv_task = None
        self._bp_task = None
        self._stop_event = None
        self._bp_lock = None
        self._pending_restore = {}
        self._pending_restore_lock = None
        self._kp_pub = ""
        self._kp_priv = ""
        self._bp_interval_min = 5
        self._bp_interval_max = 1440
        self._max_backup_input = 30 * 1024 * 1024
        self._max_backup_plain = 24 * 1024 * 1024
        self._max_backup_chats = 500
        self._max_chat_tokens = 400000
        self._max_markov_edges = 1200000
        self._module_version = "9.0.7"
        self._module_file_name = "goypulse.py"
        self._sub_channel = "@goy_ai"
        self._upd_manifest_url = "https://raw.githubusercontent.com/sepiol026-wq/goypulse/main/goypulse.manifest.json"
        self._upd_module_url = "https://raw.githubusercontent.com/sepiol026-wq/goypulse/main/goypulse.py"
        self._upd_default_pubkey = "8osOY0ITnQgNIYOc6D21afKHkGdoclAIKKIz0K3/bYM="
        self._upd_task = None
        self._upd_pending_manifest = {}
        self._upd_last_state = {}
        self._upd_mandatory_active = False
        self._tamper_mode = False
        self._restricted_log_after = 0.0
        self._prompted_updates = set()


    def _sql(self, q: str, p: tuple = (), fetch: bool = False, commit: bool = True):
        try:
            if self._db_conn:
                with self._sql_lock:
                    cur = self._db_conn.cursor()
                    cur.execute(q, p)
                    res = cur.fetchall() if fetch else None
                    is_trans = any(q.strip().upper().startswith(x) for x in ["BEGIN", "COMMIT", "ROLLBACK"])
                    if commit and not fetch and "SELECT" not in q.upper() and not is_trans:
                        self._db_conn.commit()
                    return res
            else:
                with sqlite3.connect(self._db_path) as conn:
                    cur = conn.cursor()
                    cur.execute(q, p)
                    res = cur.fetchall() if fetch else None
                    if commit: conn.commit()
                    return res


        except Exception as e:
            if self._c:
                self._c.loop.create_task(self._log(f"<b>[SQL ERR]</b> <code>{q}</code>\nArgs: <code>{p}</code>\nError: <code>{e}</code>", cat="err"))


    def _init_db(self):
                            
        try:
            conn = self._db_conn or sqlite3.connect(self._db_path)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=OFF")
            conn.execute("PRAGMA cache_size=-10000")
            if not self._db_conn: conn.close()
        except: pass

                                       
        self._sql("CREATE TABLE IF NOT EXISTS chats (cid INTEGER PRIMARY KEY, on_off INTEGER, lim INTEGER, min_m INTEGER, r_ch INTEGER, m_ch INTEGER, my_ch INTEGER, cd_m INTEGER, cd_x INTEGER, bp_on INTEGER, bp_int INTEGER, react_ch INTEGER, last_mid INTEGER, parsed_cnt INTEGER, w_cnt INTEGER, cd_u REAL, mute_u REAL, last_usr INTEGER, last_tone TEXT, last_t REAL)")

        
                                                 
        try:
            info = self._sql("PRAGMA table_info(chats)", fetch=True)
            if info:
                cols = [c[1] for c in info]
                expected = [
                    ("on_off", "INTEGER"), ("lim", "INTEGER"), ("min_m", "INTEGER"),
                    ("r_ch", "INTEGER"), ("m_ch", "INTEGER"), ("my_ch", "INTEGER"),
                    ("cd_m", "INTEGER"), ("cd_x", "INTEGER"), ("bp_on", "INTEGER"),
                    ("bp_int", "INTEGER"), ("react_ch", "INTEGER"), ("last_mid", "INTEGER"),
                    ("parsed_cnt", "INTEGER"), ("w_cnt", "INTEGER"), ("cd_u", "REAL"),
                    ("mute_u", "REAL"), ("last_usr", "INTEGER"), ("last_tone", "TEXT"),
                    ("last_t", "REAL")
                ]
                for col, ctype in expected:
                    if col not in cols:
                        self._sql(f"ALTER TABLE chats ADD COLUMN {col} {ctype}")
        except Exception as e:
            if self._c: self._c.loop.create_task(self._log(f"<b>[SCHEMA ERR]</b> <code>{e}</code>", cat="err"))


                                         
        self._sql("CREATE TABLE IF NOT EXISTS markov (cid INTEGER, d INTEGER, pref TEXT, nxt TEXT, cnt INTEGER, PRIMARY KEY(cid, d, pref, nxt))")
        self._sql("CREATE INDEX IF NOT EXISTS idx_markov ON markov(cid, d, pref)")
        self._sql("CREATE TABLE IF NOT EXISTS tokens (cid INTEGER, tk TEXT, cnt INTEGER, PRIMARY KEY(cid, tk))")
        
                       
        self._sql("CREATE TABLE IF NOT EXISTS ign (cid INTEGER, uid INTEGER, PRIMARY KEY(cid, uid))")
        
                                                                       
                                             
        self._sql("CREATE TABLE IF NOT EXISTS mem_msgs (cid INTEGER, mid INTEGER, txt TEXT, hm INTEGER, mk TEXT, mtype TEXT, sender_id INTEGER)")
        try:
            info = self._sql("PRAGMA table_info(mem_msgs)", fetch=True)
            if info and "sender_id" not in [c[1] for c in info]:
                self._sql("ALTER TABLE mem_msgs ADD COLUMN sender_id INTEGER")
        except Exception: pass
        self._sql("CREATE INDEX IF NOT EXISTS idx_mem_msgs ON mem_msgs(cid, mtype)")


        
                                        
        self._sql("CREATE TABLE IF NOT EXISTS my_msgs (cid INTEGER, mid INTEGER, PRIMARY KEY(cid, mid))")
        self._sql("CREATE TABLE IF NOT EXISTS my_outs (cid INTEGER, txt TEXT)")
        
                            
        self._sql("CREATE TABLE IF NOT EXISTS usr_cd (cid INTEGER, uid INTEGER, t REAL, PRIMARY KEY(cid, uid))")
        
                     
        self._sql("CREATE TABLE IF NOT EXISTS md_cnt (cid INTEGER, mk TEXT, cnt INTEGER, PRIMARY KEY(cid, mk))")

    async def _ans(self, m: any, text: str, log: bool = False) -> any:
        try:
                                                                   
            if not hasattr(m, 'reply') and hasattr(m, 'mid'):
                                                                        
                cid = getattr(m, 'cid', None) or getattr(m, 'chat_id', None) or getattr(self, '_last_cid', 0)

                if cid and self._c:
                    try:
                        msg = await self._c.send_message(cid, text, reply_to=m.mid)
                    except ValueError:
                        try:
                                                                 
                            ent = await self._c.get_entity(cid)
                            msg = await self._c.send_message(ent, text, reply_to=m.mid)
                        except Exception as ee: raise ee
                    
                    if log: await self._log(f"<b>[ANS]</b> {text}", cat="ans")
                    return msg

            try:
                r = await utils.answer(m, text)
            except ValueError:
                                                                 
                cid = getattr(m, 'chat_id', None)
                if cid and self._c:
                    ent = await self._c.get_entity(cid)
                    r = await self._c.send_message(ent, text, reply_to=getattr(m, 'id', None))
                else: raise

            msg = r[0] if isinstance(r, (list, tuple)) else (r or m)
            if log: await self._log(f"<b>[ANS]</b> {text}", cat="ans")
            return msg
        except Exception as e:
            try:
                if hasattr(m, 'reply'):
                    msg = await m.reply(text)
                    if log: await self._log(f"<b>[ANS]</b> {text}", cat="ans")
                    return msg
                else: raise e
            except Exception as e2:
                await self._log(f"<b>[ANS ERR]</b> <code>{e2}</code>", cat="err")
                return m


    def _nrm(self, t: str) -> str: return re.sub(r"\s+", " ", URL_RE.sub(" ", (t or "").strip().lower()))
    def _tks(self, t: str) -> Tuple[str, ...]: return tuple(x for x in TOK_RE.findall(self._nrm(t)) if len(x) > 1 and x not in STOP_W)
    def _ngs(self, t: Tuple[str, ...], n: int) -> List[Tuple[str, ...]]: return [tuple(t[i:i+n]) for i in range(len(t)-n+1)] if len(t) >= n else []
    def _iq(self, t: str) -> bool: return (n := self._nrm(t)).endswith("?") or (bool(n) and n.split()[0] in QW)
    def _ig(self, t: str) -> bool:
        tks = set(self._tks(t))
        return bool(tks & GRTS or any(any(x.startswith(g) for g in GRTS if len(g) > 3) for x in tks))
    def _ir(self, t: str) -> bool:
        tks = set(self._tks(t))
        return bool(tks & RCTS or any(any(x.startswith(r) for r in RCTS if len(r) > 3) for x in tks))
    def _jnk(self, t: str, tk: Tuple[str, ...]) -> bool:
        if len(tk) < 2 and not (self._ig(t) or self._ir(t) or self._iq(t) or len(t) < 5): return True
        if t.isupper() and len(t) > 12: return True
        if tk and len(set(tk)) <= len(tk) * 0.35 and len(tk) > 3: return True
        if not tk: return True
        if re.search(r'(.)\1{4,}', t): return True
        return False
    def _emo_cat(self, w: str) -> str:
        if w in {"шок", "охуеть", "пиздец", "ужас", "жесть", "wtf", "omg"}: return "шок"
        if w in {"ахах", "лол", "хаха", "ору", "пздц", "ржу", "лмоа", "ор", "хи", "пхпх"}: return "смех"
        if w in {"блять", "ебать", "чел", "клоун", "хуйня", "дичь", "кринж", "мда", "сука"}: return "агр"
        return "нейтрал"
    def _pbar(self, cur, tot, l=10):
        if tot == 0: return "░" * l
        p = min(1, cur / tot)
        f = int(p * l)
        return "█" * f + "░" * (l - f)
    def _b64e(self, b: bytes) -> str:
        return base64.b64encode(b).decode("ascii")

    def _b64d(self, s: str, max_len: int = 0) -> bytes:
        if not isinstance(s, str) or not s:
            raise ValueError("invalid base64 input")
        raw = base64.b64decode(s.encode("ascii"), validate=True)
        if max_len and len(raw) > max_len:
            raise ValueError("decoded data exceeds limit")
        return raw

    def _safe_decompress(self, data: bytes, max_out: int) -> bytes:
        zobj = zlib.decompressobj()
        out = zobj.decompress(data, max_out + 1)
        if len(out) > max_out:
            raise ValueError("decompressed payload too large")
        if zobj.unconsumed_tail:
            raise ValueError("compressed stream exceeds limits")
        out += zobj.flush(max_out + 1 - len(out))
        if len(out) > max_out:
            raise ValueError("decompressed payload too large")
        if zobj.unused_data:
            raise ValueError("trailing compressed data")
        return out

    def _sanitize_bp_interval(self, value: Any) -> int:
        try:
            v = int(value)
        except Exception:
            v = 30
        if v < self._bp_interval_min:
            v = self._bp_interval_min
        if v > self._bp_interval_max:
            v = self._bp_interval_max
        return v

    def _module_file_path(self) -> str:
        try:
            p = os.path.abspath(__file__)
            if os.path.isfile(p):
                return p
        except Exception:
            pass
        return os.path.abspath(self._module_file_name)

    def _sha256_bytes(self, data: bytes) -> str:
        return hashlib.sha256(data).hexdigest().lower()

    def _sha256_file(self, path: str) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest().lower()

    def _norm_ver(self, v: Any) -> Tuple[int, int, int]:
        nums = [int(x) for x in re.findall(r"\d+", str(v or "0"))[:3]]
        while len(nums) < 3:
            nums.append(0)
        return tuple(nums[:3])

    def _cmp_ver(self, a: Any, b: Any) -> int:
        av, bv = self._norm_ver(a), self._norm_ver(b)
        if av > bv:
            return 1
        if av < bv:
            return -1
        return 0

    def _upd_pubkey(self) -> str:
        return str(self.get("gpupd_pubkey", self._upd_default_pubkey) or "").strip()

    def _upd_manifest_url_resolved(self) -> str:
        return str(self.get("gpupd_manifest_url", self._upd_manifest_url) or "").strip()

    def _upd_canonical_manifest(self, manifest: dict) -> bytes:
        payload = {
            "module": str(manifest.get("module", self._module_file_name)),
            "version": str(manifest.get("version", "")),
            "module_url": str(manifest.get("module_url", manifest.get("url", self._upd_module_url))),
            "sha256": str(manifest.get("sha256", "")).lower(),
            "mandatory": bool(manifest.get("mandatory", False)),
            "min_version": str(manifest.get("min_version", "")),
            "ts": int(manifest.get("ts", 0) or 0),
        }
        return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")

    def _verify_update_manifest_signature(self, manifest: dict) -> Tuple[bool, str]:
        if not isinstance(manifest, dict):
            return False, "manifest type invalid"
        if not (CRYPTO_READY and ed25519):
            return False, "ed25519 unavailable"
        sig_b64 = str(manifest.get("signature", "") or "").strip()
        pub_b64 = self._upd_pubkey()
        if not sig_b64:
            return False, "signature missing"
        if not pub_b64:
            return False, "pubkey missing"
        try:
            sig = self._b64d(sig_b64, 512)
            pub = self._b64d(pub_b64, 128)
            if len(pub) != 32:
                return False, "pubkey length invalid"
            key = ed25519.Ed25519PublicKey.from_public_bytes(pub)
            key.verify(sig, self._upd_canonical_manifest(manifest))
            return True, ""
        except InvalidSignature:
            return False, "signature invalid"
        except Exception as e:
            return False, f"signature check failed: {e}"

    def _validate_update_manifest(self, manifest: dict) -> Tuple[bool, str, dict]:
        sig_ok, sig_err = self._verify_update_manifest_signature(manifest)
        if not sig_ok:
            return False, sig_err, {}
        version = str(manifest.get("version", "") or "").strip()
        sha256_hex = str(manifest.get("sha256", "") or "").strip().lower()
        module = str(manifest.get("module", self._module_file_name) or "").strip()
        module_url = str(manifest.get("module_url", manifest.get("url", self._upd_module_url)) or "").strip()
        mandatory = bool(manifest.get("mandatory", False))
        min_version = str(manifest.get("min_version", "") or "").strip()
        note = str(manifest.get("note", manifest.get("notes", "")) or "").strip()
        ts_val = int(manifest.get("ts", 0) or 0)
        if not version:
            return False, "version missing", {}
        if not re.fullmatch(r"[a-f0-9]{64}", sha256_hex):
            return False, "sha256 invalid", {}
        if module not in {self._module_file_name, os.path.basename(self._module_file_path())}:
            return False, "module mismatch", {}
        try:
            parsed = urllib.parse.urlparse(module_url)
        except Exception:
            return False, "module_url parse failed", {}
        if parsed.scheme != "https" or parsed.netloc not in {"raw.githubusercontent.com", "github.com"}:
            return False, "module_url rejected", {}
        out = {
            "version": version,
            "sha256": sha256_hex,
            "module": module,
            "module_url": module_url,
            "mandatory": mandatory,
            "min_version": min_version,
            "note": note,
            "ts": ts_val,
            "signature": str(manifest.get("signature", "") or ""),
        }
        return True, "", out

    def _http_get_bytes(self, url: str, max_len: int = 1024 * 1024) -> bytes:
        req = urllib.request.Request(url, headers={"User-Agent": "GoyPulseV9-UpdateClient"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = resp.read(max_len + 1)
        if len(data) > max_len:
            raise ValueError("remote payload too large")
        return data

    async def _fetch_remote_bytes(self, url: str, max_len: int = 1024 * 1024) -> bytes:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._http_get_bytes, url, max_len)

    async def _fetch_remote_json(self, url: str, max_len: int = 256 * 1024) -> dict:
        raw = await self._fetch_remote_bytes(url, max_len=max_len)
        data = json.loads(raw.decode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("manifest is not an object")
        return data

    def _update_offer_allowed(self, version: str, mandatory: bool, manual: bool = False) -> bool:
        if not version:
            return False
        if manual:
            return True
        postpone_until = int(self.get("gpupd_postpone_until", 0) or 0)
        if postpone_until > int(time.time()):
            return False
        if not mandatory and str(self.get("gpupd_skip_ver", "") or "").strip() == version:
            return False
        k = f"{version}:{int(mandatory)}"
        if k in self._prompted_updates:
            return False
        self._prompted_updates.add(k)
        return True

    def _restricted_reason(self) -> str:
        if self._tamper_mode:
            return "tamper-detected"
        mandatory_ver = str(self.get("gpupd_mandatory_ver", "") or "").strip()
        if self._upd_mandatory_active or (mandatory_ver and self._cmp_ver(mandatory_ver, self._module_version) > 0):
            return "mandatory-update"
        return ""

    def _is_restricted_mode(self) -> bool:
        return bool(self._restricted_reason())

    async def _log_restricted_once(self):
        now = time.time()
        if now < self._restricted_log_after:
            return
        self._restricted_log_after = now + 1800
        await self._log(self.strings("upd_lock").format(reason=self._restricted_reason()), cat="err")

    def _sync_verified_hash(self, sha256_hex: str):
        self.set("gpupd_verified_sha256", sha256_hex)
        self.set("gp_selfcheck_sha256", sha256_hex)
        self.set("gp_tamper_mode", False)

    async def _startup_self_check(self):
        try:
            fp = self._module_file_path()
            if not os.path.isfile(fp):
                self._tamper_mode = False
                return
            local_hash = await asyncio.get_event_loop().run_in_executor(None, self._sha256_file, fp)
            verified_hash = str(self.get("gpupd_verified_sha256", "") or "").strip().lower()
            baseline_hash = str(self.get("gp_selfcheck_sha256", "") or "").strip().lower()
            ref_hash = verified_hash or baseline_hash
            if not ref_hash:
                self.set("gp_selfcheck_sha256", local_hash)
                self._tamper_mode = False
                return
            if not hmac.compare_digest(local_hash, ref_hash):
                self._tamper_mode = True
                self.set("gp_tamper_mode", True)
                self.set("gp_tamper_ts", int(time.time()))
                await self._log(f"<b>[SELF CHECK]</b> mismatch: <code>{local_hash}</code> != <code>{ref_hash}</code>", cat="err")
                return
            self._tamper_mode = False
            self.set("gp_tamper_mode", False)
            self.set("gp_selfcheck_sha256", local_hash)
        except (FileNotFoundError, OSError):
            self._tamper_mode = False
        except Exception as e:
            self._tamper_mode = True
            self.set("gp_tamper_mode", True)
            await self._log(f"<b>[SELF CHECK ERR]</b> <code>{e}</code>", cat="err")

    async def _show_update_offer(self, manifest: dict, ctx: Any = None):
        ver = str(manifest.get("version", "") or "")
        mandatory = bool(manifest.get("mandatory", False))
        note = utils.escape_html(str(manifest.get("note", "") or "")[:300])
        note_line = f"📝 <b>Примечание:</b> <code>{note}</code>" if note else "📝 <b>Примечание:</b> <code>нет</code>"
        text = self.strings("upd_offer").format(
            cur=self._module_version,
            new=ver,
            mandatory="yes" if mandatory else "no",
            note=note_line,
        )
        buttons = [
            [{"text": "Обновить", "callback": self._cb_update_apply}],
            [{"text": "Отложить", "callback": self._cb_update_postpone, "args": (ver,)}],
        ]
        if not mandatory:
            buttons[1].append({"text": "Пропустить", "callback": self._cb_update_skip, "args": (ver,)})
        if getattr(self, "inline", None) and hasattr(self.inline, "form"):
            target = ctx
            if target is None:
                target = await self._c.send_message(self._my_id, "📡 Найдено обновление GoyPulse V9")
            try:
                await self.inline.form(text=text, message=target, reply_markup=buttons)
                return
            except Exception:
                pass
        fallback = text + "\n\n<code>.gpupdate apply</code> | <code>.gpupdate status</code>"
        if ctx is None:
            await self._c.send_message(self._my_id, fallback)
        else:
            await self._ans(ctx, fallback)

    async def _upd_loop(self):
        try:
            while True:
                await self._check_updates(manual=False, ctx=None, offer=True)
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    async def _check_updates(self, manual: bool = False, ctx: Any = None, offer: bool = True) -> dict:
        state = {
            "checked_at": int(time.time()),
            "ok": False,
            "available": False,
            "mandatory": False,
            "remote": "",
            "error": "",
        }
        try:
            manifest_url = self._upd_manifest_url_resolved()
            if not manifest_url:
                raise ValueError("manifest_url is empty")
            manifest = await self._fetch_remote_json(manifest_url, max_len=256 * 1024)
            valid, err, normalized = self._validate_update_manifest(manifest)
            if not valid:
                state["error"] = err
                self._upd_last_state = state
                return state
            remote_ver = normalized["version"]
            mandatory = bool(normalized.get("mandatory", False))
            min_version = str(normalized.get("min_version", "") or "")
            if min_version and self._cmp_ver(self._module_version, min_version) < 0:
                mandatory = True
            available = self._cmp_ver(remote_ver, self._module_version) > 0
            state.update({
                "ok": True,
                "available": available,
                "mandatory": mandatory,
                "remote": remote_ver,
                "error": "",
            })
            self._upd_last_state = state
            if available:
                normalized["mandatory"] = mandatory
                self._upd_pending_manifest = normalized
                self._upd_mandatory_active = mandatory
                if mandatory:
                    self.set("gpupd_mandatory_ver", remote_ver)
                if offer and self._update_offer_allowed(remote_ver, mandatory, manual=manual):
                    await self._show_update_offer(normalized, ctx=ctx)
            else:
                self._upd_pending_manifest = {}
                self._upd_mandatory_active = False
                self.set("gpupd_mandatory_ver", "")
            return state
        except Exception as e:
            state["error"] = str(e)
            self._upd_last_state = state
            return state

    async def _apply_update(self, manifest: Optional[dict] = None) -> Tuple[bool, str, str]:
        target = manifest or self._upd_pending_manifest
        valid, err, normalized = self._validate_update_manifest(target)
        if not valid:
            return False, self.strings("upd_fail").format(utils.escape_html(err)), ""
        try:
            # We skip manual download/write as requested by the user.
            # But we still return success if the manifest validation passed.
            self.set("gpupd_verified_ver", normalized["version"])
            self.set("gpupd_skip_ver", "")
            self.set("gpupd_postpone_until", 0)
            self.set("gpupd_mandatory_ver", "")
            self._upd_pending_manifest = {}
            self._upd_mandatory_active = False
            self._tamper_mode = False
            return True, "verified", normalized["module_url"]
        except Exception as e:
            return False, self.strings("upd_fail").format(utils.escape_html(str(e))), ""

    async def _format_update_status(self) -> str:
        st = self._upd_last_state or {}
        checked_ts = int(st.get("checked_at", 0) or 0)
        checked = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(checked_ts)) if checked_ts else "never"
        postpone_until = int(self.get("gpupd_postpone_until", 0) or 0)
        postpone = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(postpone_until)) if postpone_until > int(time.time()) else "-"
        skip_ver = str(self.get("gpupd_skip_ver", "") or "-")
        restricted = self._restricted_reason() or "off"
        return (
            "🛰️ <b>Update status</b> | <code>GoyPulse V9 by goy(@samsepi0l_ovf)</code>\n"
            f"├ Version: <code>{self._module_version}</code>\n"
            f"├ Last check: <code>{checked}</code>\n"
            f"├ Available: <code>{'yes' if st.get('available') else 'no'}</code>\n"
            f"├ Remote: <code>{st.get('remote') or '-'}</code>\n"
            f"├ Mandatory: <code>{'yes' if (self._upd_mandatory_active or st.get('mandatory')) else 'no'}</code>\n"
            f"├ Restricted: <code>{restricted}</code>\n"
            f"├ Postpone until: <code>{postpone}</code>\n"
            f"├ Skip version: <code>{utils.escape_html(skip_ver)}</code>\n"
            f"└ Last error: <code>{utils.escape_html(st.get('error') or '-')}</code>"
        )

    async def _cb_update_apply(self, call: Any):
        ok, msg, url = await self._apply_update()
        if ok:
            target_chat = getattr(call, "chat_id", self._my_id)
            await self._c.send_message(target_chat, f".dlm {url}")
        else:
            await self._respond(call, msg)

    async def _cb_update_postpone(self, call: Any, version: str):
        until = int(time.time()) + 6 * 3600
        self.set("gpupd_postpone_until", until)
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(until))
        await self._respond(call, self.strings("upd_postpone").format(ts))

    async def _cb_update_skip(self, call: Any, version: str):
        self.set("gpupd_skip_ver", str(version))
        await self._respond(call, self.strings("upd_skip").format(utils.escape_html(str(version))))

    async def _cb_subscribe_yes(self, call: Any):
        self.set("sub_prompt_done", True)
        self.set("sub_prompt_answer", "yes")
        try:
            await self._c(JoinChannelRequest(self._sub_channel))
            await self._respond(call, self.strings("sub_yes"))
        except Exception as e:
            await self._respond(call, self.strings("sub_fail").format(utils.escape_html(str(e))))

    async def _cb_subscribe_no(self, call: Any):
        self.set("sub_prompt_done", True)
        self.set("sub_prompt_answer", "no")
        await self._respond(call, self.strings("sub_no"))

    def _crypto_ready(self) -> bool:
        return bool(CRYPTO_READY and x25519 and HKDF and ChaCha20Poly1305)

    def _ensure_kp(self) -> bool:
        if not self._crypto_ready():
            return False
        prv_b64 = self.get("gpb2_priv", "")
        pub_b64 = self.get("gpb2_pub", "")
        try:
            if prv_b64 and pub_b64:
                prv_raw = self._b64d(prv_b64, 64)
                pub_raw = self._b64d(pub_b64, 64)
                if len(prv_raw) == 32 and len(pub_raw) == 32:
                    prv = x25519.X25519PrivateKey.from_private_bytes(prv_raw)
                    calc_pub = prv.public_key().public_bytes(
                        encoding=serialization.Encoding.Raw,
                        format=serialization.PublicFormat.Raw,
                    )
                    if calc_pub == pub_raw:
                        self._kp_priv = prv_b64
                        self._kp_pub = pub_b64
                        return True
        except Exception:
            pass
        prv = x25519.X25519PrivateKey.generate()
        pub = prv.public_key()
        prv_raw = prv.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption(),
        )
        pub_raw = pub.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        self._kp_priv = self._b64e(prv_raw)
        self._kp_pub = self._b64e(pub_raw)
        self.set("gpb2_priv", self._kp_priv)
        self.set("gpb2_pub", self._kp_pub)
        return True

    def _load_trust_keys(self) -> Dict[str, str]:
        raw = self.get("gpb2_trust", {}) or {}
        out: Dict[str, str] = {}
        if not isinstance(raw, dict):
            return out
        for uid_raw, pub_b64 in raw.items():
            try:
                uid = str(int(uid_raw))
                pb = self._b64d(pub_b64, 64)
                if len(pb) != 32:
                    continue
                x25519.X25519PublicKey.from_public_bytes(pb)
                out[uid] = self._b64e(pb)
            except Exception:
                continue
        return out

    def _save_trust_keys(self, d: Dict[str, str]):
        self.set("gpb2_trust", d)

    def _register_trust_key(self, uid: int, pub_b64: str) -> bool:
        try:
            if uid <= 0 or not self._crypto_ready():
                return False
            pb = self._b64d(pub_b64, 64)
            if len(pb) != 32:
                return False
            x25519.X25519PublicKey.from_public_bytes(pb)
            td = self._load_trust_keys()
            td[str(uid)] = self._b64e(pb)
            self._save_trust_keys(td)
            return True
        except Exception:
            return False

    def _key_fingerprint(self, pub_b64: str) -> str:
        try:
            raw = self._b64d(pub_b64, 64)
            return hashlib.sha256(raw).hexdigest()[:16]
        except Exception:
            return ""

    def _build_keycard_payload(self) -> str:
        if not self._ensure_kp():
            raise RuntimeError("crypto unavailable")
        payload = {
            "v": 2,
            "uid": int(self._my_id),
            "pub": self._kp_pub,
            "fp": self._key_fingerprint(self._kp_pub),
            "ts": int(time.time()),
        }
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        return "GPK2_" + self._b64e(raw)

    def _parse_keycard_payload(self, payload: str) -> Optional[int]:
        try:
            if not isinstance(payload, str) or not payload.startswith("GPK2_"):
                return None
            raw = self._b64d(payload[5:], 8192)
            data = json.loads(raw.decode("utf-8"))
            if not isinstance(data, dict):
                return None
            uid = int(data.get("uid", 0))
            pub = data.get("pub", "")
            if not self._register_trust_key(uid, pub):
                return None
            return uid
        except Exception:
            return None

    def _derive_wrap_key(self, shared: bytes) -> bytes:
        return HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=None,
            info=b"goypulse-gpb2-wrap-v2",
        ).derive(shared)

    def _mk_wrap(self, uid: int, recipient_pub_b64: str, data_key: bytes) -> dict:
        recipient_pub_raw = self._b64d(recipient_pub_b64, 64)
        recipient_pub = x25519.X25519PublicKey.from_public_bytes(recipient_pub_raw)
        eph = x25519.X25519PrivateKey.generate()
        shared = eph.exchange(recipient_pub)
        wrap_key = self._derive_wrap_key(shared)
        nonce = os.urandom(12)
        aad = f"uid:{uid}".encode("utf-8")
        cipher = ChaCha20Poly1305(wrap_key)
        ct = cipher.encrypt(nonce, data_key, aad)
        epk = eph.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        return {
            "uid": int(uid),
            "epk": self._b64e(epk),
            "n": self._b64e(nonce),
            "ct": self._b64e(ct),
        }

    def _unwrap_data_key(self, wraps: List[dict]) -> bytes:
        if not self._ensure_kp():
            raise RuntimeError("crypto unavailable")
        prv_raw = self._b64d(self._kp_priv, 64)
        prv = x25519.X25519PrivateKey.from_private_bytes(prv_raw)
        uid = int(self._my_id)
        for w in wraps:
            if not isinstance(w, dict):
                continue
            try:
                if int(w.get("uid", 0)) != uid:
                    continue
                epk = self._b64d(w.get("epk", ""), 64)
                nonce = self._b64d(w.get("n", ""), 64)
                ct = self._b64d(w.get("ct", ""), 512)
                if len(epk) != 32 or len(nonce) != 12:
                    continue
                pub = x25519.X25519PublicKey.from_public_bytes(epk)
                shared = prv.exchange(pub)
                wrap_key = self._derive_wrap_key(shared)
                aad = f"uid:{uid}".encode("utf-8")
                key = ChaCha20Poly1305(wrap_key).decrypt(nonce, ct, aad)
                if len(key) == 32:
                    return key
            except Exception:
                continue
        raise ValueError("no matching key envelope")

    def _obf(self, d: dict, recipient_ids: Optional[List[int]] = None, strict_recipients: bool = False) -> str:
        try:
            if not self._ensure_kp() or not self._vld_bkp(d):
                return ""
            recipients = {str(int(self._my_id)): self._kp_pub}
            trusted = self._load_trust_keys()
            missed = []
            for rid in recipient_ids or []:
                sid = str(int(rid))
                if sid == str(int(self._my_id)):
                    continue
                if sid in trusted:
                    recipients[sid] = trusted[sid]
                else:
                    missed.append(sid)
            if strict_recipients and missed:
                raise ValueError(f"missing trusted keys: {', '.join(missed)}")
            plain = json.dumps(d, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            if len(plain) > self._max_backup_plain:
                raise ValueError("backup payload too large")
            comp = zlib.compress(plain, 9)
            data_key = os.urandom(32)
            alg = "AESGCM" if AESGCM else "CHACHA20"
            nonce = os.urandom(12)
            meta = {
                "v": 2,
                "alg": alg,
                "sender": int(self._my_id),
                "ts": int(time.time()),
            }
            aad = json.dumps(meta, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            if alg == "AESGCM":
                ct = AESGCM(data_key).encrypt(nonce, comp, aad)
            else:
                ct = ChaCha20Poly1305(data_key).encrypt(nonce, comp, aad)
            wraps = [self._mk_wrap(int(uid), pub, data_key) for uid, pub in recipients.items()]
            body = {
                "v": 2,
                "z": 1,
                "meta": meta,
                "aad": self._b64e(aad),
                "n": self._b64e(nonce),
                "ct": self._b64e(ct),
                "wrp": wraps,
            }
            packed = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            if len(packed) > self._max_backup_input:
                raise ValueError("encrypted payload too large")
            return "GPB2_" + self._b64e(packed)
        except Exception as e:
            if self._c:
                self._c.loop.create_task(self._log(f"<b>[OBF ERR]</b> <code>{e}</code>"))
            return ""

    def _deobf_v2(self, s: str) -> dict:
        try:
            if not s.startswith("GPB2_"):
                return {}
            packed = self._b64d(s[5:], self._max_backup_input)
            body = json.loads(packed.decode("utf-8"))
            if not isinstance(body, dict):
                return {}
            if int(body.get("v", 0)) != 2:
                return {}
            wraps = body.get("wrp", [])
            if not isinstance(wraps, list) or len(wraps) > 128:
                return {}
            nonce = self._b64d(body.get("n", ""), 64)
            aad = self._b64d(body.get("aad", ""), 4096)
            ct = self._b64d(body.get("ct", ""), self._max_backup_input)
            if len(nonce) != 12:
                return {}
            key = self._unwrap_data_key(wraps)
            alg = str(body.get("meta", {}).get("alg", "AESGCM")) if isinstance(body.get("meta"), dict) else "AESGCM"
            if alg == "AESGCM":
                comp = AESGCM(key).decrypt(nonce, ct, aad)
            else:
                comp = ChaCha20Poly1305(key).decrypt(nonce, ct, aad)
            plain = self._safe_decompress(comp, self._max_backup_plain)
            data = json.loads(plain.decode("utf-8"))
            return data if self._vld_bkp(data) else {}
        except Exception:
            return {}

    def _deobf(self, s: str) -> dict:
        try:
            if not isinstance(s, str):
                return {}
            if s.startswith("GPB2_"):
                return self._deobf_v2(s)
            if not s.startswith("GPB_"):
                return {}
            b = self._b64d(s[4:], self._max_backup_input)
            j = self._safe_decompress(b, self._max_backup_plain).decode("utf-8")
            d = json.loads(j)
            return d if self._vld_bkp(d) else {}
        except Exception:
            return {}

    def _vld_bkp(self, d: dict) -> bool:
        try:
            if not isinstance(d, dict) or not d or len(d) > self._max_backup_chats:
                return False
            total_edges = 0
            token_re = re.compile(r"^[\wа-яё-]{1,64}$", re.I)

            def _validate_tfq(tfq: Any) -> bool:
                if not isinstance(tfq, dict) or len(tfq) > self._max_chat_tokens:
                    return False
                for tk, cnt in tfq.items():
                    if not isinstance(tk, str) or not token_re.fullmatch(tk):
                        return False
                    if not isinstance(cnt, int) or cnt < 0 or cnt > 10**9:
                        return False
                return True

            def _validate_mkv(mkv: Any, depth: int) -> int:
                nonlocal total_edges
                if not isinstance(mkv, dict):
                    return -1
                local_edges = 0
                for pref, nxts in mkv.items():
                    if not isinstance(pref, str):
                        return -1
                    parts = pref.split("|")
                    if len(parts) != depth or any((not token_re.fullmatch(p)) for p in parts):
                        return -1
                    if not isinstance(nxts, dict):
                        return -1
                    for nxt, cnt in nxts.items():
                        if not isinstance(nxt, str) or not token_re.fullmatch(nxt):
                            return -1
                        if not isinstance(cnt, int) or cnt < 0 or cnt > 10**9:
                            return -1
                        local_edges += 1
                        total_edges += 1
                        if total_edges > self._max_markov_edges:
                            return -1
                return local_edges

            for cid_s, dat in d.items():
                if not isinstance(cid_s, str) or not re.fullmatch(r"-?\d{1,20}", cid_s):
                    return False
                if not isinstance(dat, dict):
                    return False
                required = ("tfq", "mkv", "mkv3", "mkv4", "ign")
                if any(k not in dat for k in required):
                    return False
                if not _validate_tfq(dat.get("tfq")):
                    return False
                if _validate_mkv(dat.get("mkv"), 2) < 0:
                    return False
                if _validate_mkv(dat.get("mkv3"), 3) < 0:
                    return False
                if _validate_mkv(dat.get("mkv4"), 4) < 0:
                    return False
                ign = dat.get("ign")
                if not isinstance(ign, list) or len(ign) > 200000:
                    return False
                for uid in ign:
                    if not isinstance(uid, int):
                        return False
                for opt in ("last_mid", "parsed_cnt", "w_cnt"):
                    if opt in dat and (not isinstance(dat[opt], int) or dat[opt] < 0):
                        return False
            return True
        except Exception as e:
            if self._c:
                self._c.loop.create_task(self._log(f"<b>[BKP VAL ERR]</b> <code>{e}</code>"))
            return False

    def _migrate(self):
        if not os.path.exists(self._df): return
        try:
            with open(self._df, "r", encoding="utf-8") as f: d = json.load(f)
            for cid_s, dat in d.items():
                cid = int(cid_s)
                self._sql("INSERT OR REPLACE INTO chats (cid, parsed_cnt, last_mid) VALUES (?, ?, ?)", (cid, dat.get("parsed_cnt", 0), dat.get("last_mid", 0)))
                for tk, c in dat.get("tfq", {}).items():
                    self._sql("INSERT OR REPLACE INTO tokens (cid, tk, cnt) VALUES (?, ?, ?)", (cid, tk, c))
                for k, v in dat.get("mkv", {}).items():
                    for nxt, c in v.items(): self._sql("INSERT OR REPLACE INTO markov (cid, d, pref, nxt, cnt) VALUES (?, 2, ?, ?, ?)", (cid, k, nxt, c))
                for k, v in dat.get("mkv3", {}).items():
                    for nxt, c in v.items(): self._sql("INSERT OR REPLACE INTO markov (cid, d, pref, nxt, cnt) VALUES (?, 3, ?, ?, ?)", (cid, k, nxt, c))
                for k, v in dat.get("mkv4", {}).items():
                    for nxt, c in v.items(): self._sql("INSERT OR REPLACE INTO markov (cid, d, pref, nxt, cnt) VALUES (?, 4, ?, ?, ?)", (cid, k, nxt, c))
            os.rename(self._df, self._df + ".bak")
        except Exception as e:
            if self._c: self._c.loop.create_task(self._log(f"<b>[MIGRATE ERR]</b> <code>{e}</code>"))

    def _sv_br(self):
        try:
            self._sql("BEGIN")
            for cid, st in self._chs.items():
                if not st.on and not st.msgs and not st.parsed_cnt: continue
                self._sql("INSERT OR REPLACE INTO chats (cid, on_off, lim, min_m, r_ch, m_ch, my_ch, cd_m, cd_x, bp_on, bp_int, react_ch, last_mid, parsed_cnt, w_cnt, cd_u, mute_u, last_usr, last_tone, last_t) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 
                          (cid, int(st.on), st.lim, st.min_m, st.r_ch, st.m_ch, st.my_ch, st.cd_m, st.cd_x, int(self.config["bp_on"]), int(self.config["bp_int"]), int(self.config["react_ch"]), st.last_mid, st.parsed_cnt, st.w_cnt, st.cd_u, st.mute_u, st.last_usr, st.last_tone, st.last_t), commit=False)
                self._sql("DELETE FROM mem_msgs WHERE cid=?", (cid,), commit=False)
                for mtype, dq in [('msgs', st.msgs), ('rec', st.rec), ('mds', st.mds)]:
                    for m in dq: self._sql("INSERT INTO mem_msgs (cid, mid, txt, hm, mk, mtype, sender_id) VALUES (?,?,?,?,?,?,?)", (cid, m.mid, m.txt, int(m.hm), m.mk, mtype, m.sender_id), commit=False)

                self._sql("DELETE FROM my_msgs WHERE cid=?", (cid,), commit=False)
                for mid in st.my_msgs: self._sql("INSERT INTO my_msgs (cid, mid) VALUES (?,?)", (cid, mid), commit=False)
                self._sql("DELETE FROM my_outs WHERE cid=?", (cid,), commit=False)
                for txt in st.my_outs: self._sql("INSERT INTO my_outs (cid, txt) VALUES (?,?)", (cid, txt), commit=False)
                self._sql("DELETE FROM usr_cd WHERE cid=?", (cid,), commit=False)
                for uid, t in st.usr_cd.items(): self._sql("INSERT INTO usr_cd (cid, uid, t) VALUES (?,?,?)", (cid, uid, t), commit=False)
                self._sql("DELETE FROM md_cnt WHERE cid=?", (cid,), commit=False)
                for mk, cnt in st.md_cnt.items(): self._sql("INSERT INTO md_cnt (cid, mk, cnt) VALUES (?,?,?)", (cid, mk, cnt), commit=False)
                self._sql("DELETE FROM ign WHERE cid=?", (cid,), commit=False)
                for uid in st.ign: self._sql("INSERT INTO ign (cid, uid) VALUES (?,?)", (cid, uid), commit=False)
            self._sql("COMMIT")
        except Exception as e:
            try: self._sql("ROLLBACK")
            except Exception: pass
            if self._c: self._c.loop.create_task(self._log(f"<b>[SAVE ERR]</b> {e}", cat="err"))


    def _ld_br(self):
        try:
            res = self._sql("SELECT * FROM chats", fetch=True)
            if not res: return
            for r in res:
                cid = r[0]
                st = self._chs[cid]
                st.cid = cid
                                      
                st.on, st.lim, st.min_m, st.r_ch, st.m_ch, st.my_ch, st.cd_m, st.cd_x = bool(r[1]), r[2], r[3], r[4], r[5], r[6], r[7], r[8]
                                                                               
                st.last_mid = r[12] or 0
                st.parsed_cnt = r[13] or 0
                st.w_cnt = r[14] or 0
                st.cd_u = r[15] or 0.0
                st.mute_u = r[16] or 0.0
                st.last_usr = r[17] or 0
                st.last_tone = r[18] or "нейтрал"
                st.last_t = r[19] or 0.0

                                    
                for tk, cnt in self._sql("SELECT tk, cnt FROM tokens WHERE cid=?", (cid,), fetch=True):
                    st.tfq[tk] = cnt or 0

                
                                                                   
                if not st.w_cnt and st.tfq:
                    st.w_cnt = sum(st.tfq.values())

                                       
                for d, pref, nxt, cnt in self._sql("SELECT d, pref, nxt, cnt FROM markov WHERE cid=?", (cid,), fetch=True):
                    pref_t = tuple(pref.split("|"))
                    if d == 2: st.mkv[pref_t][nxt] = cnt or 0
                    elif d == 3: st.mkv3[pref_t][nxt] = cnt or 0
                    elif d == 4: st.mkv4[pref_t][nxt] = cnt or 0


                                       
                m_res = self._sql("SELECT mid, txt, hm, mk, mtype, sender_id FROM mem_msgs WHERE cid=?", (cid,), fetch=True)
                for mid, txt, hm, mk, mtype, sid in m_res:
                    mo = MObj(mid, txt, self._tks(txt), bool(hm or 0), mk or "", sid or 0, cid)
                    if mtype == 'msgs': st.msgs.append(mo)

                    elif mtype == 'rec': st.rec.append(mo)
                    elif mtype == 'mds': st.mds.append(mo)
                
                for mid, in self._sql("SELECT mid FROM my_msgs WHERE cid=?", (cid,), fetch=True): st.my_msgs.append(mid)
                for txt, in self._sql("SELECT txt FROM my_outs WHERE cid=?", (cid,), fetch=True): st.my_outs.append(txt)
                for uid, t in self._sql("SELECT uid, t FROM usr_cd WHERE cid=?", (cid,), fetch=True): st.usr_cd[uid] = t
                for mk, cnt in self._sql("SELECT mk, cnt FROM md_cnt WHERE cid=?", (cid,), fetch=True): st.md_cnt[mk] = cnt or 0

                for uid, in self._sql("SELECT uid FROM ign WHERE cid=?", (cid,), fetch=True): st.ign.add(uid)
        except Exception as e:
            if self._c: self._c.loop.create_task(self._log(f"<b>[LOAD ERR]</b> {e}", cat="err"))



    async def _wait_or_stop(self, timeout: float) -> bool:
        if not self._stop_event:
            await asyncio.sleep(timeout)
            return False
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def _start_bg_tasks(self):
        await self._stop_bg_tasks()
        self._stop_event = asyncio.Event()
        if not self._bp_lock:
            self._bp_lock = asyncio.Lock()
        if not self._pending_restore_lock:
            self._pending_restore_lock = asyncio.Lock()
        self._sv_task = self._c.loop.create_task(self._sv_loop())
        self._bp_task = self._c.loop.create_task(self._bp_loop())

    async def _stop_bg_tasks(self):
        if self._stop_event:
            self._stop_event.set()
        tasks = [t for t in (self._sv_task, self._bp_task) if t and not t.done()]
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except BaseException:
                pass
        self._sv_task = None
        self._bp_task = None
        self._stop_event = None

    def _known_backup_cids(self) -> List[int]:
        seen = {int(cid) for cid in self._chs.keys()}
        for q in (
            "SELECT DISTINCT cid FROM chats",
            "SELECT DISTINCT cid FROM tokens",
            "SELECT DISTINCT cid FROM markov",
            "SELECT DISTINCT cid FROM ign",
        ):
            rows = self._sql(q, fetch=True) or []
            for row in rows:
                try:
                    seen.add(int(row[0]))
                except Exception:
                    continue
        return sorted(seen)

    def _chat_backup_data(self, cid: int) -> dict:
        st = self._chs.get(cid)
        if st:
            return {
                "tfq": {str(k): int(v) for k, v in dict(st.tfq).items()},
                "mkv": {"|".join(k): {str(n): int(c) for n, c in dict(v).items()} for k, v in st.mkv.items()},
                "mkv3": {"|".join(k): {str(n): int(c) for n, c in dict(v).items()} for k, v in st.mkv3.items()},
                "mkv4": {"|".join(k): {str(n): int(c) for n, c in dict(v).items()} for k, v in st.mkv4.items()},
                "ign": [int(uid) for uid in st.ign],
                "last_mid": int(st.last_mid),
                "parsed_cnt": int(st.parsed_cnt),
                "w_cnt": int(st.w_cnt),
            }
        tfq_rows = self._sql("SELECT tk, cnt FROM tokens WHERE cid=?", (cid,), fetch=True) or []
        ign_rows = self._sql("SELECT uid FROM ign WHERE cid=?", (cid,), fetch=True) or []
        return {
            "tfq": {str(t): int(c or 0) for t, c in tfq_rows},
            "mkv": {str(k): {str(n): int(c) for n, c in v.items()} for k, v in self._get_full_mkv(cid, 2).items()},
            "mkv3": {str(k): {str(n): int(c) for n, c in v.items()} for k, v in self._get_full_mkv(cid, 3).items()},
            "mkv4": {str(k): {str(n): int(c) for n, c in v.items()} for k, v in self._get_full_mkv(cid, 4).items()},
            "ign": [int(uid) for uid, in ign_rows],
            "last_mid": 0,
            "parsed_cnt": 0,
            "w_cnt": 0,
        }

    def _collect_backup_dataset(self, target_cids: Optional[List[int]] = None) -> Tuple[dict, int, int, int]:
        all_cids = self._known_backup_cids()
        known = set(all_cids)
        if target_cids is None:
            selected = all_cids
        else:
            selected = []
            seen = set()
            for cid in target_cids:
                if cid in seen:
                    continue
                seen.add(cid)
                if cid in known:
                    selected.append(cid)
        data = {}
        total_words = 0
        total_links = 0
        for cid in selected:
            dat = self._chat_backup_data(cid)
            data[str(cid)] = dat
            total_words += sum(v for v in dat.get("tfq", {}).values() if isinstance(v, int))
            total_links += sum(len(x) for x in dat.get("mkv", {}).values())
            total_links += sum(len(x) for x in dat.get("mkv3", {}).values())
            total_links += sum(len(x) for x in dat.get("mkv4", {}).values())
        return data, len(selected), total_words, total_links

    async def _send_payload_file(self, dest: int, fname: str, caption: str):
        try:
            await self._c.send_file(dest, fname, caption=caption)
        except ValueError:
            ent = await self._c.get_entity(dest)
            await self._c.send_file(ent, fname, caption=caption)

    async def _sv_loop(self):
        while True:
            if await self._wait_or_stop(120):
                return
            try:
                await asyncio.get_event_loop().run_in_executor(None, self._sv_br)
            except asyncio.CancelledError:
                return
            except Exception as e:
                if self._c:
                    self._c.loop.create_task(self._log(f"<b>[SV LOOP ERR]</b> <code>{e}</code>", cat="err"))

    async def _bp_up(
        self,
        m: Message = None,
        manual: bool = False,
        target_cids: Optional[List[int]] = None,
        recipient_ids: Optional[List[int]] = None,
        out_chat: Optional[int] = None,
    ):
        fname = ""
        try:
            if not self._crypto_ready():
                if manual and m:
                    await self._ans(m, self.strings("bp_no_crypto"))
                return
            if not self._bp_lock:
                self._bp_lock = asyncio.Lock()
            async with self._bp_lock:
                data, total_chats, total_words, total_links = self._collect_backup_dataset(target_cids)
                if not data:
                    if manual and m:
                        await self._ans(m, "❌ Нет данных для выбранных чатов.")
                    return
                payload = self._obf(data, recipient_ids=recipient_ids, strict_recipients=bool(recipient_ids))
                if not payload:
                    if manual and m:
                        await self._ans(m, "❌ Не удалось сформировать зашифрованный бэкап.")
                    return
                fname = f"gp_backup_{time.strftime('%Y%m%d_%H%M%S')}.gpb2"
                with open(fname, "w", encoding="utf-8") as f:
                    f.write(payload)
                reason = "Manual" if manual else "Auto"
                caption = (
                    f"💾 <b>GoyPulse Backup ({reason})</b>\n"
                    f"├ 📁 Чатов: <code>{total_chats}</code>\n"
                    f"├ 🧠 Слов: <code>{total_words}</code>\n"
                    f"└ 🔗 Связей: <code>{total_links}</code>\n\n"
                    f"Формат: <code>GPB2 (AEAD)</code>"
                )
                dst = out_chat if out_chat is not None else await self._get_log()
                await self._send_payload_file(dst, fname, caption)
                if manual and m:
                    await self._ans(m, self.strings("bp_up").format(fname))
                if self._c:
                    self._c.loop.create_task(self._log(f"Бэкап отправлен. Чатов: <code>{total_chats}</code>", cat="bkp"))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if manual and m:
                await self._ans(m, self.strings("bp_err").format(e))
            if self._c:
                self._c.loop.create_task(self._log(f"<b>[BKP ERR]</b> {e}", cat="err"))
        finally:
            if fname and os.path.exists(fname):
                try:
                    os.remove(fname)
                except Exception:
                    pass

    async def _bp_loop(self):
        while True:
            try:
                interval = self._sanitize_bp_interval(self.config.get("bp_int", 30))
                if await self._wait_or_stop(interval * 60):
                    return
                if int(self.config.get("bp_on", 1)):
                    await self._bp_up()
            except asyncio.CancelledError:
                return
            except Exception as e:
                if self._c:
                    self._c.loop.create_task(self._log(f"<b>[BKP LOOP ERR]</b> <code>{e}</code>", cat="err"))
                if await self._wait_or_stop(60):
                    return
    def _sv(self):
        try:
            c = {k: v for k, v in self._chs.items() if v.on}
            self.set("on", list(c.keys()))
            for p in ["lim", "min_m", "r_ch", "m_ch", "my_ch", "cd_m", "cd_x"]:
                self.set(p, {str(k): getattr(v, p) for k, v in c.items()})
        except Exception: pass
    def _ld(self):
        try:
            on_list = self.get("on", [])
            for cid_str in on_list:
                cid = int(cid_str)
                st = self._chs[cid]
                st.on = True
                for p, d in [("lim", "d_lim"), ("min_m", "d_min"), ("r_ch", "d_ch"), ("m_ch", "d_mch"), ("my_ch", "d_mych"), ("cd_m", "d_cdm"), ("cd_x", "d_cdx")]:
                    try:
                        cfg_dict = self.get(p, {})
                        val = cfg_dict.get(str(cid), self.config[d])
                        setattr(st, p, int(val))
                    except:
                        setattr(st, p, self.config[d])
        except Exception: pass
    async def _get_log(self):
        if self._log_ch: return self._log_ch
        self._log_ch = self.get("log_ch", 0)
        if self._log_ch: return self._log_ch
        
                                     
        try:
            async for dialog in self._c.iter_dialogs():
                if dialog.is_channel and dialog.title == "GoyPulse Logs":
                    self._log_ch = dialog.id
                    self.set("log_ch", self._log_ch)
                    return self._log_ch
        except Exception: pass

                                 
        try:
            r = await self._c(CreateChannelRequest(title="GoyPulse Logs", about="GoyPulse V9 by goy(@samsepi0l_ovf) | Stealth & Activity Logs", megagroup=False))
            self._log_ch = r.chats[0].id
            self.set("log_ch", self._log_ch)
            return self._log_ch
        except Exception:
            self._log_ch = self._my_id
            return self._my_id

    async def _log(self, text: str, cat: str = "err"):
        cmap = {"err": "log_err", "stl": "log_stl", "bkp": "log_bkp", "lrn": "log_lrn", "ans": "log_ans"}
        if not self.config[cmap.get(cat, "log_err")]: return
        
                             
        icons = {"err": "❌", "stl": "🕵️", "bkp": "💾", "lrn": "🧠", "ans": "💬"}
        labels = {"err": "ERROR", "stl": "STEALTH", "bkp": "BACKUP", "lrn": "SYSTEM", "ans": "ANSWER"}
        icon = icons.get(cat, "📝")
        label = labels.get(cat, "LOG")
        ts = f"<code>[{time.strftime('%H:%M:%S')}]</code>"
        
        formatted = f"{ts} {icon} <b>[{label}]</b>\n{text}"
        
        try:
            l_ch = await self._get_log()
            try:
                await self._c.send_message(l_ch, formatted)
            except ValueError:
                ent = await self._c.get_entity(l_ch)
                await self._c.send_message(ent, formatted)
        except Exception as e:
            if cat == "err": print(f"FAILED TO LOG: {e}\nORIGINAL TEXT: {text}")




    def _add(self, st: CSt, m: Message, commit: bool = True):
        try:
            t = (m.raw_text or "").strip()
            if len(t) > 1000 or t.startswith(("/", ".", "!")) or "GoyPulse" in t: return
            hm = bool(getattr(m, "media", None))
            tk = self._tks(t)
            mk = next((k for k in ["sticker", "photo", "gif", "video", "voice"] if getattr(m, k, None)), "media") if hm else ""
            cid = m.chat_id
            mo = MObj(m.id, t, tk, hm, mk, getattr(m, 'sender_id', 0), cid)

            st.msgs.append(mo); st.rec.append(mo)

            if tk:
                st.w_cnt += len(tk)
                st.tfq.update(tk)
                                                           
                for word in tk:
                    self._sql("INSERT INTO tokens (cid, tk, cnt) VALUES (?, ?, 1) ON CONFLICT(cid, tk) DO UPDATE SET cnt=cnt+1", (cid, word), commit=commit)
                
                if len(tk) >= 3:
                    for a, b, c in zip(tk, tk[1:], tk[2:]):
                        st.mkv[(a, b)][c] += 1
                        self._sql("INSERT INTO markov (cid, d, pref, nxt, cnt) VALUES (?, ?, ?, ?, 1) ON CONFLICT(cid, d, pref, nxt) DO UPDATE SET cnt=cnt+1", (cid, 2, f"{a}|{b}", c), commit=commit)
                
                if len(tk) >= 4:
                    for a, b, c, d in zip(tk, tk[1:], tk[2:], tk[3:]):
                        st.mkv3[(a, b, c)][d] += 1
                        self._sql("INSERT INTO markov (cid, d, pref, nxt, cnt) VALUES (?, ?, ?, ?, 1) ON CONFLICT(cid, d, pref, nxt) DO UPDATE SET cnt=cnt+1", (cid, 3, f"{a}|{b}|{c}", d), commit=commit)

                if len(tk) >= 5:
                    for a, b, c, d, e in zip(tk, tk[1:], tk[2:], tk[3:], tk[4:]):
                        st.mkv4[(a, b, c, d)][e] += 1
                        self._sql("INSERT INTO markov (cid, d, pref, nxt, cnt) VALUES (?, ?, ?, ?, 1) ON CONFLICT(cid, d, pref, nxt) DO UPDATE SET cnt=cnt+1", (cid, 4, f"{a}|{b}|{c}|{d}", e), commit=commit)



            if hm:
                st.mds.append(mo)
                st.md_cnt[mk] += 1
        except Exception as e:
            if self._c: self._c.loop.create_task(self._log(f"<b>[ADD ERR]</b> <code>{e}</code>", cat="err"))


    async def _lrn(self, cid: int, rmsg: Message = None):
        st = self._chs[cid]
        if st.lrn: return
        st.lrn = True
        start_t = time.perf_counter()
        last_upd = start_t
        try:
            cnt = 0
            offs = st.last_mid if st.last_mid else 0
            lm = None if st.lim == 0 else st.lim
            total_msgs = 0
            if rmsg:
                try:
                    full = await self._c.get_messages(cid, limit=0)
                    total_msgs = full.total - (st.parsed_cnt if st.parsed_cnt else 0)
                    if lm and lm < total_msgs: total_msgs = lm
                except: pass
            update_interval = 2000
            self._sql("BEGIN")
            async for m in self._c.iter_messages(cid, limit=lm, offset_id=offs):
                if getattr(m.sender, 'bot', False) or getattr(m, 'fwd_from', None) or (getattr(m, 'sender_id', None) in st.ign): continue
                st.last_mid = m.id
                if (m.raw_text or "").strip() or getattr(m, "media", None): self._add(st, m, commit=False)
                cnt += 1
                if cnt % 500 == 0:
                    self._sql("COMMIT")
                    self._sql("BEGIN")
                if cnt % update_interval == 0:

                    now = time.perf_counter()
                    await asyncio.sleep(0.01)
                    if rmsg and now - last_upd > 3.0:
                        last_upd = now
                        spd = int(cnt / (now - start_t + 0.001))
                        vocab = len(st.mkv) + len(st.mkv3) + len(st.mkv4)
                        eta = "???"
                        if total_msgs and spd > 0:
                            rem = total_msgs - cnt
                            if rem > 0:
                                s = int(rem / spd)
                                eta = f"{s // 60:d}m {s % 60:d}s" if s >= 60 else f"{s}s"
                        try:
                            await self._ans(rmsg, self.strings("ref_upd").format(
                                st.parsed_cnt + cnt, 
                                f"/{st.lim}" if st.lim else "", 
                                vocab, 
                                sum(st.md_cnt.values()), 
                                st.w_cnt, 
                    spd, 
                                eta
                            ))
                            log_msg = f"📊 <b>Training Progress</b> [Chat: <code>{cid}</code>]\n├ Parsed: <code>{cnt}</code>\n├ Vocabulary: <code>{vocab}</code>\n└ ETA: <code>{eta}</code>"
                            self._c.loop.create_task(self._log(log_msg, cat="lrn"))
                        except Exception as e:
                            rmsg = None
                            if self._c: self._c.loop.create_task(self._log(f"<b>[REF UPD ERR]</b> <code>{e}</code>", cat="lrn"))




            self._sql("COMMIT")
            st.parsed_cnt += cnt

            if rmsg:
                md_info = ", ".join([f"{k}: {v}" for k, v in st.md_cnt.items()])
                try: 
                    await self._ans(rmsg, self.strings("ref_dn").format(
                        st.parsed_cnt, 
                        len(st.mkv) + len(st.mkv3) + len(st.mkv4),
                        st.w_cnt,
                        sum(st.md_cnt.values()),
                        md_details=f"\n└─ 🖼️ Детали: <code>{md_info}</code>" if md_info else ""
                    ))
                except Exception as e:
                    if self._c: self._c.loop.create_task(self._log(f"<b>[REF DN ERR]</b> <code>{e}</code>"))


        except Exception as e:
            try: self._sql("ROLLBACK")
            except: pass
            if rmsg: 
                try: await self._ans(rmsg, f"⚠️ Остановка на {cnt}: {e}\nНапишите .gpref чтобы продолжить.")
                except Exception as e:
                    if self._c: self._c.loop.create_task(self._log(f"<b>[REF ERR ANS]</b> <code>{e}</code>"))



        finally: 
            st.lrn = False
            try: await asyncio.get_event_loop().run_in_executor(None, self._sv_br)
            except Exception as e:
                if self._c: self._c.loop.create_task(self._log(f"<b>[SV LOOP ERR]</b> <code>{e}</code>"))

    def _sim(self, st: CSt, a: Tuple[str, ...], b: Tuple[str, ...]) -> float:
        try:
            sa, sb = set(a), set(b)
            if not (c := sa & sb): return 0.0
            idf = lambda x: 2.0 if st.tfq.get(x, 0) <= 0 else 1.0 + math.log(1.0 + (len(st.msgs) / (1.0 + st.tfq[x])))
            return ((sum(idf(t) for t in c) / (sum(idf(t) for t in sa | sb) or 1.0)) * 0.7 + (len(c) / len(sa | sb)) * 0.3)
        except Exception as e:
            if self._c: self._c.loop.create_task(self._log(f"<b>[SIM ERR]</b> <code>{e}</code>"))
            return 0.0

    def _cnd(self, st: CSt, ctx_tks: Tuple[str, ...], tme: bool) -> List[str]:
        try:
            if not ctx_tks: return []
            scrs = []
            w_ctx = ctx_tks[-10:]
            for m in st.msgs:
                if not m.tks: continue
                sc = self._sim(st, w_ctx, m.tks)
                if sc > 0.9: sc *= 0.1
                if set(self._ngs(w_ctx, 2)) & set(self._ngs(m.tks, 2)): sc += 0.4
                if tme and sc > 0.1: sc += 0.6
                if sc > 0.2 and m.txt.lower() not in st.my_outs:
                    scrs.append((sc, m.txt))
            res = sorted(scrs, reverse=True, key=lambda x: x[0])[:25]
            return [txt for _, txt in res]
        except Exception as e:
            if self._c: self._c.loop.create_task(self._log(f"<b>[CND ERR]</b> <code>{e}</code>"))
            return []

    def _gen(self, st: CSt, ctx_tks: Tuple[str, ...], tme: bool) -> str:
        try:
            sds = []
            rel_tks = set(ctx_tks[-8:])
            cid = st.cid if hasattr(st, 'cid') else 0
            if not cid:
                for k, v in self._chs.items():
                    if v == st: cid = k; break

            if tme:
                if len(ctx_tks) >= 4:
                    for w in rel_tks:
                        res = self._sql("SELECT pref FROM markov WHERE cid=? AND d=3 AND (pref LIKE ? OR pref LIKE ? OR pref LIKE ?) LIMIT 30", (cid, f"{w}|%", f"%|{w}|%", f"%|{w}"), fetch=True)
                        sds.extend([tuple(r[0].split("|")) for r in res])
                if not sds and len(ctx_tks) >= 3:
                    for w in rel_tks:
                        res = self._sql("SELECT pref FROM markov WHERE cid=? AND d=2 AND (pref LIKE ? OR pref LIKE ?) LIMIT 30", (cid, f"{w}|%", f"%|{w}"), fetch=True)
                        sds.extend([tuple(r[0].split("|")) for r in res])
            if not sds:
                res = self._sql("SELECT pref FROM markov WHERE cid=? AND d=2 LIMIT 100", (cid,), fetch=True)
                sds = [tuple(r[0].split("|")) for r in res]
            if not sds: return ""
            out = list(random.choice(sds))
            target_len = random.randint(3, 25)
            for _ in range(target_len):
                choices = Counter()
                if len(out) >= 4: choices.update(self._get_mkv(st.cid if hasattr(st, 'cid') else 0, 4, "|".join(out[-4:])))
                if not choices and len(out) >= 3: choices.update(self._get_mkv(st.cid if hasattr(st, 'cid') else 0, 3, "|".join(out[-3:])))
                if not choices and len(out) >= 2: choices.update(self._get_mkv(st.cid if hasattr(st, 'cid') else 0, 2, "|".join(out[-2:])))
                if not choices: break
                most_common = choices.most_common(10)
                words, counts = zip(*most_common)
                nxt = random.choices(words, weights=counts, k=1)[0]
                if nxt == out[-1] and random.random() < 0.3: break
                out.append(nxt)
            r = " ".join(out).strip()
            if not r or r.lower() in st.my_outs: return "" 
            rc_chk = [m.tks for m in list(st.rec)[-15:] if m.tks]
            if any(self._sim(st, self._tks(r), x) > 0.8 for x in rc_chk): return ""
            st.my_outs.append(r.lower())
            return r
        except Exception as e:
            if self._c: self._c.loop.create_task(self._log(f"<b>[GEN ERR]</b> <code>{e}</code>"))
            return ""


    def _get_mkv(self, cid: int, d: int, pref: str) -> Counter:
        res = self._sql("SELECT nxt, cnt FROM markov WHERE cid=? AND d=? AND pref=?", (cid, d, pref), fetch=True)
        c = Counter()
        if res:
            for nxt, cnt in res: c[nxt] = cnt
        return c

    def _get_full_mkv(self, cid: int, d: int) -> Dict[str, Dict[str, int]]:
        res = self._sql("SELECT pref, nxt, cnt FROM markov WHERE cid=? AND d=?", (cid, d), fetch=True)
        r = {}
        if res:
            for p, n, c in res:
                if p not in r: r[p] = {}
                r[p][n] = c
        return r

    def _fb(self, st: CSt, t: str) -> str:
        try:
            if self._ig(t): return random.choice(["хай", "дарова", "прив", "ку", "qq", "салам", "салют"])
            if self._ir(t): return random.choice(["ахах", "лол", "жиза", "пон", "база", "рил", "мда", "пздц", "чел...", "имбово", "разрывная", "треш"])
            if self._iq(t): return random.choice(["скорее всего", "хз", "надо погуглить", "надо чекнуть", "без понятия", "посмотрим", "мб", "а хз", "в душе не ебу"])
            com = [w for w, _ in st.tfq.most_common(50) if len(w) > 3]
            if com and random.random() < 0.5:
                w = random.choice(com[:20])
                r = f"{w} — факт" if random.random() < 0.5 else f"ну {w} это база"
                st.my_outs.append(r.lower())
                return r
            return random.choice(["пон", "ок", "ага", "согл", "ясно", "бывает", "ну да", "мда уж"])
        except Exception: return "пон"
    def _stl(self, t: str, src: str) -> str:
        try:
            if not (t := (t or "").strip()): return t
            if src.isupper(): t = t.upper()
            elif random.random() < 0.85: t = t.lower()
            else: t = t.capitalize()
            if "???" in src: t += "???"
            elif "!!!" in src: t += "!!!"
            elif "..." in src: t += "..."
            w_src = set(self._tks(src))
            cat = "нейтрал"
            if w_src & {"шок", "охуеть", "пиздец", "ужас", "жесть", "wtf", "omg"}: cat = "шок"
            elif w_src & {"ахах", "лол", "хаха", "ору", "пздц", "ржу", "лмоа", "ор", "хи"}: cat = "смех"
            elif w_src & {"блять", "ебать", "чел", "клоун", "хуйня", "дичь", "кринж", "мда"}: cat = "агр"
            if random.random() < 0.4: t += f" {random.choice(EMO_M[cat])}"
            return t
        except Exception as e:
            if self._c: self._c.loop.create_task(self._log(f"<b>[STL ERR]</b> <code>{e}</code>"))
            return t

    def _md(self, st: CSt, t: str) -> Optional[int]:
        try:
            if not st.mds or (st.m_ch < 100 and random.randint(1, 100) > int(st.m_ch)): return None
            it, ws = set(self._tks(t)), []
            for m in list(st.mds)[-600:]:
                sc = (len(it & set(m.tks)) * 0.7) if m.tks else 0.0
                if m.mk == "sticker" and self._ir(t): sc += 0.8
                if m.mk in ("photo", "video") and not it: sc += 0.3
                ws.append((sc, m.mid))
            b = [mid for s, mid in sorted(ws, reverse=True, key=lambda x: x[0]) if s > 0.2]
            return random.choice(b[:8]) if b else random.choice(list(st.mds)[-100:]).mid
        except Exception as e:
            if self._c: self._c.loop.create_task(self._log(f"<b>[MD ERR]</b> <code>{e}</code>"))
            return None

    def _extract_prefixed_blob(self, text: str, prefixes: Tuple[str, ...]) -> str:
        if not isinstance(text, str):
            return ""
        for pref in prefixes:
            m = re.search(rf"{re.escape(pref)}[A-Za-z0-9+/=]+", text)
            if m:
                return m.group(0)
        return ""

    async def _extract_payload_from_message(self, msg: Message, prefixes: Tuple[str, ...]) -> str:
        if not msg:
            return ""
        parts = []
        raw_text = getattr(msg, "raw_text", None)
        if raw_text:
            parts.append(raw_text)
        if getattr(msg, "media", None):
            raw = await self._c.download_media(msg, bytes)
            if raw:
                if len(raw) > self._max_backup_input:
                    raise ValueError("input is too large")
                parts.append(raw.decode("utf-8", errors="ignore"))
        for item in parts:
            hit = self._extract_prefixed_blob(item, prefixes)
            if hit:
                return hit
        return ""

    async def _maybe_import_keycard(self, msg: Message) -> bool:
        try:
            payload = self._extract_prefixed_blob(getattr(msg, "raw_text", "") or "", ("GPK2_",))
            if not payload and getattr(msg, "media", None):
                fobj = getattr(msg, "file", None)
                fname = (getattr(fobj, "name", "") or "").lower()
                mime = (getattr(fobj, "mime_type", "") or "").lower()
                if not (fname.endswith(".gpk2") or "text" in mime or "json" in mime):
                    return False
                payload = await self._extract_payload_from_message(msg, ("GPK2_",))
            if not payload:
                return False
            uid = self._parse_keycard_payload(payload)
            if not uid:
                return False
            if self._c:
                self._c.loop.create_task(self._log(f"Ключ пользователя <code>{uid}</code> импортирован", cat="bkp"))
            return True
        except Exception:
            return False

    def _is_group_entity(self, ent: Any) -> bool:
        if not ent:
            return False
        if getattr(ent, "megagroup", False) or getattr(ent, "gigagroup", False):
            return True
        cname = ent.__class__.__name__.lower()
        if cname == "chat":
            return True
        if cname == "channel" and not getattr(ent, "broadcast", False):
            return True
        return False

    async def _resolve_chat_target(self, token: str, current: Optional[int] = None, require_group: bool = False) -> int:
        t = (token or "").strip()
        if not t:
            raise ValueError("target is empty")
        if t.lower() == "here":
            if current is None:
                raise ValueError("target 'here' is unavailable")
            return int(current)
        try:
            cid = int(t)
            if require_group and cid >= 0:
                raise ValueError("target must be a group")
            return cid
        except ValueError:
            ent = await self._c.get_entity(t)
            cid = int(tl_utils.get_peer_id(ent))
            if require_group and not self._is_group_entity(ent):
                raise ValueError("target must be a group")
            return cid

    async def _resolve_user_target(self, token: str) -> int:
        t = (token or "").strip()
        if not t:
            raise ValueError("user target is empty")
        try:
            uid = int(t)
            if uid <= 0:
                raise ValueError("invalid user id")
            return uid
        except ValueError:
            ent = await self._c.get_entity(t)
            if self._is_group_entity(ent):
                raise ValueError("target user expected")
            uid = int(getattr(ent, "id", 0) or 0)
            if uid <= 0:
                raise ValueError("invalid user id")
            return uid

    async def _send_keycard(self, uid: int):
        if not self._ensure_kp():
            raise RuntimeError("crypto unavailable")
        fname = f"gp_keycard_{self._my_id}.gpk2"
        payload = self._build_keycard_payload()
        with open(fname, "w", encoding="utf-8") as f:
            f.write(payload)
        try:
            await self._send_payload_file(uid, fname, "🔐 GoyPulse keycard")
        finally:
            if os.path.exists(fname):
                os.remove(fname)

    async def _respond(self, ctx: Any, text: str):
        if hasattr(ctx, "edit") and not isinstance(ctx, Message):
            try:
                await ctx.edit(text)
                return
            except Exception:
                pass
        await self._ans(ctx, text)

    async def _restore_dataset(self, d: dict) -> Tuple[int, int, int, int, List[int]]:
        cnt_words = 0
        cnt_links = 0
        cnt_ign = 0
        cids = []
        self._sql("BEGIN")
        try:
            for cid_s, dat in d.items():
                cid = int(cid_s)
                cids.append(cid)
                self._sql("DELETE FROM tokens WHERE cid=?", (cid,), commit=False)
                self._sql("DELETE FROM markov WHERE cid=?", (cid,), commit=False)
                self._sql("DELETE FROM ign WHERE cid=?", (cid,), commit=False)
                for tk, c in dat.get("tfq", {}).items():
                    self._sql("INSERT INTO tokens (cid, tk, cnt) VALUES (?, ?, ?)", (cid, tk, int(c)), commit=False)
                    cnt_words += int(c)
                for d_lvl, key in ((2, "mkv"), (3, "mkv3"), (4, "mkv4")):
                    for pref, nxts in dat.get(key, {}).items():
                        for nxt, c in nxts.items():
                            self._sql("INSERT INTO markov (cid, d, pref, nxt, cnt) VALUES (?, ?, ?, ?, ?)", (cid, d_lvl, pref, nxt, int(c)), commit=False)
                            cnt_links += 1
                for uid in dat.get("ign", []):
                    self._sql("INSERT INTO ign (cid, uid) VALUES (?, ?)", (cid, int(uid)), commit=False)
                    cnt_ign += 1
            self._sql("COMMIT")
        except Exception:
            self._sql("ROLLBACK")
            raise
        for cid_s, dat in d.items():
            cid = int(cid_s)
            st = self._chs[cid]
            st.cid = cid
            st.msgs.clear()
            st.rec.clear()
            st.mds.clear()
            st.tfq.clear()
            st.mkv.clear()
            st.mkv3.clear()
            st.mkv4.clear()
            st.md_cnt.clear()
            st.my_msgs.clear()
            st.my_outs.clear()
            st.usr_cd.clear()
            st.ign.clear()
            st.tfq.update({str(k): int(v) for k, v in dat.get("tfq", {}).items()})
            for pref, nxts in dat.get("mkv", {}).items():
                p = tuple(pref.split("|"))
                st.mkv[p].update({str(k): int(v) for k, v in nxts.items()})
            for pref, nxts in dat.get("mkv3", {}).items():
                p = tuple(pref.split("|"))
                st.mkv3[p].update({str(k): int(v) for k, v in nxts.items()})
            for pref, nxts in dat.get("mkv4", {}).items():
                p = tuple(pref.split("|"))
                st.mkv4[p].update({str(k): int(v) for k, v in nxts.items()})
            st.ign.update({int(x) for x in dat.get("ign", [])})
            st.last_mid = int(dat.get("last_mid", 0))
            st.parsed_cnt = int(dat.get("parsed_cnt", 0))
            st.w_cnt = int(dat.get("w_cnt", sum(st.tfq.values())))
        return len(d), cnt_words, cnt_links, cnt_ign, cids

    async def _restore_from_payload(self, ctx: Any, payload: str):
        await self._respond(ctx, self.strings("bp_chk"))
        d = self._deobf(payload)
        if not d:
            await self._respond(ctx, "❌ Неверный формат бэкапа или ключ недоступен.")
            return
        if not self._vld_bkp(d):
            await self._respond(ctx, "❌ Бэкап отклонен валидацией структуры.")
            return
        await self._respond(ctx, self.strings("bp_vld").format(chats=len(d)))
        await self._respond(ctx, self.strings("bp_run"))
        cnt_chats, cnt_words, cnt_links, cnt_ign, cids = await self._restore_dataset(d)
        lines = []
        for cid in cids[:25]:
            try:
                ent = await self._c.get_entity(cid)
                title = utils.escape_html(getattr(ent, "title", getattr(ent, "username", str(cid))))
            except Exception:
                title = str(cid)
            lines.append(f"├ <b>{title}</b> (<code>{cid}</code>)")
        chat_list = "\n".join(lines) if lines else "├ <code>no chat metadata</code>"
        await self._respond(ctx, self.strings("bp_dn").format(chats=cnt_chats, words=cnt_words, links=cnt_links, ign=cnt_ign, chat_list=chat_list))

    async def _format_backup_status(self) -> str:
        enabled = bool(int(self.config.get("bp_on", 1)))
        interval = self._sanitize_bp_interval(self.config.get("bp_int", 30))
        mode = "включена" if enabled else "выключена"
        crypto = "доступно" if self._crypto_ready() else "недоступно"
        return (
            f"💾 <b>Статус бэкапов</b> | <code>GoyPulse V9 by goy(@samsepi0l_ovf)</code>\n"
            f"├ Состояние: <code>{mode}</code>\n"
            f"├ Интервал: <code>{interval} мин</code>\n"
            f"├ Формат: <code>GPB2 (AEAD)</code>\n"
            f"└ Криптография: <code>{crypto}</code>"
        )

    async def _show_backup_help(self, m: Message):
        text = self.strings("bp_help")
        if getattr(self, "inline", None) and hasattr(self.inline, "form"):
            buttons = [[{"text": "Backup all", "callback": self._cb_backup_all}], [{"text": "Status", "callback": self._cb_backup_status}], [{"text": "Keycard me", "callback": self._cb_backup_keycard_me}]]
            if getattr(m, "is_group", False):
                buttons.insert(1, [{"text": "Backup here", "callback": self._cb_backup_here}])
            try:
                await self.inline.form(text=text, message=m, reply_markup=buttons)
                return
            except Exception:
                pass
        await self._ans(m, text)

    async def _cb_backup_all(self, call: Any):
        await self._respond(call, "⚙️ Запуск бэкапа всех чатов...")
        await self._bp_up(manual=False)
        await self._respond(call, "✅ Бэкап запущен. Файл отправлен в лог-чат.")

    async def _cb_backup_here(self, call: Any):
        chat_id = getattr(getattr(call, "message", None), "chat_id", None)
        if chat_id is None:
            chat_id = getattr(call, "chat_id", None)
        if chat_id is None:
            await self._respond(call, "❌ Не удалось определить чат.")
            return
        await self._respond(call, "⚙️ Запуск бэкапа текущего чата...")
        await self._bp_up(manual=False, target_cids=[int(chat_id)])
        await self._respond(call, "✅ Бэкап запущен. Файл отправлен в лог-чат.")

    async def _cb_backup_status(self, call: Any):
        await self._respond(call, await self._format_backup_status())

    async def _cb_backup_keycard_me(self, call: Any):
        await self._respond(call, "⚙️ Подготовка keycard...")
        me_chat = await self._get_log()
        await self._send_keycard(me_chat)
        await self._respond(call, "✅ Keycard отправлен в лог-чат.")

    async def _cb_restore_confirm(self, call: Any, token: str):
        job = self._pending_restore.pop(token, None)
        if not job:
            await self._respond(call, "❌ Сессия подтверждения истекла.")
            return
        rep = await self._c.get_messages(job["chat_id"], ids=job["reply_id"])
        payload = await self._extract_payload_from_message(rep, ("GPB2_", "GPB_"))
        if not payload:
            await self._respond(call, "❌ Не найдено содержимое бэкапа.")
            return
        await self._restore_from_payload(call, payload)

    async def _cb_restore_cancel(self, call: Any, token: str):
        self._pending_restore.pop(token, None)
        await self._respond(call, self.strings("bp_restore_cancel"))

    @loader.unrestricted
    async def watcher(self, e: Message):
        try:
            if not getattr(e, 'message', None) or self._glob_stop or getattr(e, 'out', False): return
            if self._is_restricted_mode():
                await self._log_restricted_once()
                return
            await self._maybe_import_keycard(e)
            if getattr(e, 'is_private', False): return
            if getattr(e.sender, 'bot', False) or getattr(e, 'fwd_from', None): return
            st = self._chs[e.chat_id]
            sid = getattr(e, 'sender_id', None)
            if not st.on or st.lrn or time.time() < st.mute_u or (sid in st.ign): return
            t = (e.raw_text or "").strip()
            tk = self._tks(t)
            hm = bool(getattr(e, "media", None))
            if len(t) > 1000 or t.startswith(("/", ".", "!")): return
            if not t and not hm: return
            if self._jnk(t, tk) and not hm: return
            self._add(st, e, commit=True)

            tme = False

            if getattr(e, 'reply_to_msg_id', None):
                if e.reply_to_msg_id in st.my_msgs: tme = True
                elif getattr(e, 'is_reply', False):
                    try:
                        rep = await e.get_reply_message()
                        if rep and getattr(rep, 'sender_id', None) == self._my_id:
                            tme = True
                            st.my_msgs.append(e.reply_to_msg_id)
                    except Exception as ex:
                        if self._c: self._c.loop.create_task(self._log(f"<b>[WATCHER REP ERR]</b> <code>{ex}</code>"))

            tone = self._emo_cat(tk[0]) if tk else "нейтрал"
            if sid == st.last_usr and time.time() - st.last_t < 180: tone = st.last_tone
            st.last_usr, st.last_tone, st.last_t = sid, tone, time.time()

            ch = st.my_ch if tme else st.r_ch
            if ch < 100:
                if (st.cd_m > 0 and time.time() < st.cd_u) or len(st.msgs) < int(st.min_m): return
                if sid and time.time() < st.usr_cd.get(sid, 0): return 
                if random.randint(1, 100) > int(ch): return

            if random.randint(1, 100) <= self.config["react_ch"]:
                try:
                    emo = random.choice(["👍", "😂", "❤️", "🔥", "🤔", "👀", "🌚", "🤡"])
                    await e.react(emo)
                    if random.random() < 0.7: return 
                except Exception as ex:
                    if self._c: self._c.loop.create_task(self._log(f"<b>[REACT ERR]</b> <code>{ex}</code>"))

            ctx_msgs = [m.tks for m in list(st.rec)[-4:] if m.tks]
            ctx_tks = tuple(w for msg in ctx_msgs for w in msg) + tk
            ans = self._gen(st, ctx_tks, tme) or self._fb(st, t)
            ans = self._stl(ans, t)
            mid = self._md(st, t)
            r_del = len(t) * 0.03 if t else 0.5
            await asyncio.sleep(min(max(r_del, 0.5), 3.0))
            try: await e.client.send_read_acknowledge(e.chat_id, e)
            except Exception as ex:
                if self._c: self._c.loop.create_task(self._log(f"<b>[ACK ERR]</b> <code>{ex}</code>"))

            if mid and (random.random() < 0.45 or not ans):
                try:
                    if (mm := await e.client.get_messages(e.chat_id, ids=mid)) and mm.media:
                        act = 'document' if getattr(mm, 'sticker', None) or getattr(mm, 'gif', None) else 'photo'
                        dur = 2.0
                        if getattr(mm, 'voice', None) or getattr(mm, 'audio', None):
                            act = 'record_audio'
                            try:
                                if hasattr(mm, 'voice') and mm.voice: dur = mm.voice.duration
                                elif hasattr(mm, 'audio') and mm.audio: dur = mm.audio.duration
                            except: pass
                        try:
                            async with e.client.action(e.chat_id, act): await asyncio.sleep(min(max(dur, 1.5), 10.0))
                        except Exception as ex:
                            if self._c: self._c.loop.create_task(self._log(f"<b>[ACTION ERR]</b> <code>{ex}</code>"))
                        msg = await e.client.send_file(e.chat_id, mm, reply_to=e.id)
                        st.my_msgs.append(msg.id)
                        st.cd_u = time.time() + random.uniform(st.cd_m, st.cd_x)
                        if sid: st.usr_cd[sid] = time.time() + random.uniform(st.cd_m, st.cd_x) * 2.0
                        if random.random() < 0.8: return 
                except Exception as ex:
                    if self._c: self._c.loop.create_task(self._log(f"<b>[MEDIA ANS ERR]</b> <code>{ex}</code>", cat="err"))


            if ans and len(ans) > 0:
                tdl = min(max(len(ans) * random.uniform(0.12, 0.22), 1.5), 15.0)
                try:
                    async with e.client.action(e.chat_id, 'typing'): await asyncio.sleep(tdl)
                except Exception as ex:
                    if self._c: self._c.loop.create_task(self._log(f"<b>[TYPING ERR]</b> <code>{ex}</code>"))
                try:
                    if random.random() < 0.03 and len(ans) > 10:
                        w_ans = ans[:-1] + random.choice(["ь", "ж", "ф", "а"])
                        msg = await e.reply(w_ans); await asyncio.sleep(random.uniform(1.0, 2.0)); await e.reply(f"*{ans.split()[-1]}")
                    else: msg = await e.reply(ans)
                    st.my_msgs.append(msg.id)
                except Exception as ex:
                    if self._c: self._c.loop.create_task(self._log(f"<b>[TEXT ANS ERR]</b> <code>{ex}</code>", cat="err"))


            st.cd_u = time.time() + random.uniform(st.cd_m, st.cd_x)
            if sid: st.usr_cd[sid] = time.time() + random.uniform(st.cd_m, st.cd_x) * 2.0
        except Exception as ex:
            if self._c: self._c.loop.create_task(self._log(f"<b>[WATCHER GLOBAL ERR]</b> <code>{ex}</code>"))
    @loader.command(ru_doc="<on/off> | Включить/выключить автоответчик")
    async def gpulsecmd(self, m: Message):
        try:
            if not m.is_group: return await self._ans(m, self.strings("og"))
            if self._is_restricted_mode():
                return await self._ans(m, self.strings("upd_lock").format(reason=self._restricted_reason()))
            a = utils.get_args_raw(m).strip().lower()
            if a not in ["on", "off"]: return await self._ans(m, self.strings("h_pulse"))
            st = self._chs[m.chat_id]
            st.on = (a == "on")
            self._glob_stop = False
            self._sv()
            t = self.strings("on") if st.on else self.strings("off")
            if st.on and not st.parsed_cnt: t += "\n\n⚠️ <b>База пуста!</b> Напиши <code>.gpref</code>"
            await self._ans(m, t, log=True)
            try: await asyncio.get_event_loop().run_in_executor(None, self._sv_br)
            except Exception as e:
                if self._c: self._c.loop.create_task(self._log(f"<b>[GPULSE EXEC ERR]</b> <code>{e}</code>"))
        except Exception as e: await self._ans(m, f"❌ Ошибка: {e}")

    @loader.command(ru_doc="| Вывести статистику работы")
    async def gpstatcmd(self, m: Message):
        try:
            if not m.is_group: return await self._ans(m, self.strings("og"))
            st = self._chs[m.chat_id]
            tw = ", ".join([w for w, _ in st.tfq.most_common(20) if len(w) >= 4][:7]) if st.tfq else "Пусто"
            warn = "\n\n⚠️ <b>Бот не обучался! Запусти</b> <code>.gpref</code>" if not st.parsed_cnt else ""
            if self._is_restricted_mode():
                warn += f"\n\n🔒 <b>Ограниченный режим:</b> <code>{self._restricted_reason()}</code>"
            lm_str = "Безлимит" if st.lim == 0 else f"{st.lim} msg"
            cd_str = "Без задержки" if st.cd_m == 0 and st.cd_x == 0 else f"{st.cd_m}-{st.cd_x} сек"
            await self._ans(m, self.strings("st").format(
                on="Вкл ✅" if st.on else "Выкл ❌", 
                pc=st.parsed_cnt, 
                wc=st.w_cnt,
                m=len(st.msgs), 
                l=lm_str, 
                vk=len(st.mkv)+len(st.mkv3)+len(st.mkv4), 
                md=sum(st.md_cnt.values()), 
                c=st.r_ch, 
                my=st.my_ch, 
                mc=st.m_ch, 
                cd=cd_str, 
                tw=tw, 
                ig=len(st.ign), 
                warn=warn
            ))
        except Exception as e: await self._ans(m, f"❌ Ошибка: {e}")

    @loader.command(ru_doc="[check|apply|status] | Проверка и применение GitHub-обновлений")
    async def gpupdatecmd(self, m: Message):
        try:
            args = utils.get_args_raw(m).strip().lower()
            if not args or args in {"check", "c"}:
                st = await self._check_updates(manual=True, ctx=m, offer=True)
                if not st.get("ok"):
                    return await self._ans(m, self.strings("upd_fail").format(utils.escape_html(st.get("error") or "unknown")))
                if not st.get("available"):
                    return await self._ans(m, self.strings("upd_none"))
                return await self._ans(m, f"🛰️ Обновление <code>{utils.escape_html(st.get('remote') or '-')}</code> найдено. Предложение отправлено inline.")
            if args in {"status", "st", "s"}:
                return await self._ans(m, await self._format_update_status())
            if args in {"apply", "a", "now", "update"}:
                if not self._upd_pending_manifest:
                    st = await self._check_updates(manual=True, ctx=None, offer=False)
                    if not st.get("ok"):
                        return await self._ans(m, self.strings("upd_fail").format(utils.escape_html(st.get("error") or "unknown")))
                    if not st.get("available"):
                        return await self._ans(m, self.strings("upd_none"))
                
                ok, msg, url = await self._apply_update()
                if ok:
                    await self._c.send_message(m.chat_id, f".dlm {url}")
                else:
                    await self._ans(m, msg)
                return
            return await self._ans(m, "❌ Использование: <code>.gpupdate [check|apply|status]</code>")
        except Exception as e:
            await self._ans(m, f"❌ Ошибка: {e}")

    @loader.command(ru_doc="| Диагностика пути модуля")
    async def gppathcmd(self, m: Message):
        await self._ans(m, f"📂 Путь модуля: <code>{__file__}</code>\nВерсия: <code>{self._module_version}</code>")

    @loader.command(ru_doc="| Собрать сообщения и обновить память бота")
    async def gprefcmd(self, m: Message):
        try:
            if not m.is_group: return await self._ans(m, self.strings("og"))
            if self._is_restricted_mode():
                return await self._ans(m, self.strings("upd_lock").format(reason=self._restricted_reason()))
            cid = m.chat_id
            st = self._chs[cid]
            mod = " (продолжение)" if st.last_mid else ""
            msg_res = await self._ans(m, self.strings("ref_st").format(mod))
            act_msg = msg_res if isinstance(msg_res, Message) else m
            await self._log(f"🧬 <b>Started training</b> in chat <code>{cid}</code>{mod}", cat="lrn")

            asyncio.create_task(self._lrn(cid, act_msg))
        except Exception as e: await self._ans(m, f"❌ Ошибка: {e}")
    @loader.command(ru_doc="<минуты> | Мут бота на время")
    async def gpmutecmd(self, m: Message):
        try:
            if not m.is_group: return await self._ans(m, self.strings("og"))
            a = utils.get_args_raw(m).strip()
            if not a: return await self._ans(m, self.strings("h_mute"))
            v = int(a) if a.isdigit() else 15
            self._chs[m.chat_id].mute_u = time.time() + (v * 60)
            await self._ans(m, self.strings("mute").format(v))
        except Exception as e: await self._ans(m, f"❌ Ошибка: {e}")
    @loader.command(ru_doc="| Узнать вайб чата")
    async def gpinfocmd(self, m: Message):
        try:
            if not m.is_group: return await self._ans(m, self.strings("og"))
            st = self._chs[m.chat_id]
            tw = ", ".join([w for w, _ in st.tfq.most_common(25) if len(w) >= 4][:6]) if st.tfq else "Тишина"
            act_rate = len(st.rec)
            act_lvl = "Высокая 🔥" if act_rate > 80 else "Средняя 💬" if act_rate > 30 else "Низкая 💤"
            c_agr = c_sm = c_sh = c_nt = 0
            u_act = Counter()
            for msg in list(st.rec):
                if not msg.tks: continue
                u_act[msg.sender_id] += 1
                for tk in msg.tks:
                    cat = self._emo_cat(tk)
                    if cat == "агр": c_agr += 1
                    elif cat == "смех": c_sm += 1
                    elif cat == "шок": c_sh += 1
                    else: c_nt += 1
            tot = c_agr + c_sm + c_sh + c_nt or 1
            ton = f"├─ 🤬 Траур/Агр: <code>{int(c_agr/tot*100)}%</code>\n├─ 😂 Позитив: <code>{int(c_sm/tot*100)}%</code>\n└─ 😱 Шок: <code>{int(c_sh/tot*100)}%</code>"
            
            async def get_user_info(uid):
                if not uid: return "???"
                try:
                    user = await self._c.get_entity(uid)
                    name = utils.escape_html(getattr(user, 'first_name', '') or '')
                    if getattr(user, 'last_name', None): name += f" {utils.escape_html(user.last_name)}"
                    uname = f" (@{user.username})" if getattr(user, 'username', None) else ""
                    return f"<b>{name}</b>{uname} [<code>{uid}</code>]"
                except:
                    return f"<code>ID_{uid}</code>"

            top_u_id = 0
            dushnila_id = 0
            if u_act:
                top_u_id = u_act.most_common(1)[0][0]
                                                                                                      
                active_users = list(u_act.keys())
                if active_users: dushnila_id = random.choice(active_users)

            top_u_str = await get_user_info(top_u_id)
            dushnila_str = await get_user_info(dushnila_id)

            warn = "\n\n⚠️ <b>Пусто. Сделай</b> <code>.gpref</code>" if not st.parsed_cnt else ""
            out = self.strings("info").format(tonality=ton, act=act_lvl, tw=tw, warn=warn) + f"\n\n👑 <b>Топ чата:</b> {top_u_str}\n👓 <b>Главный душнила:</b> {dushnila_str}"
            
            if hasattr(m, 'out') and m.out:
                await m.edit(out)
            else:
                await self._ans(m, out)
        except Exception as e: await self._ans(m, f"❌ Ошибка: {e}")
    @loader.command(ru_doc="<реплай> | Игнор юзера")
    async def gpignorecmd(self, m: Message):
        try:
            if not getattr(m, 'is_reply', False): return await self._ans(m, self.strings("h_ign"))
            rep = await m.get_reply_message()
            uid = getattr(rep, 'sender_id', None)
            if not uid: return await self._ans(m, "❌ Нет ID.")
            st = self._chs[m.chat_id]
            if uid in st.ign:
                st.ign.remove(uid)
                await self._ans(m, self.strings("ign_del"), log=True)
            else:
                st.ign.add(uid)
                await self._ans(m, self.strings("ign_add"), log=True)
            try: await asyncio.get_event_loop().run_in_executor(None, self._sv_br)
            except Exception as e:
                if self._c: self._c.loop.create_task(self._log(f"<b>[IGNORE EXEC ERR]</b> <code>{e}</code>"))

        except Exception as e: await self._ans(m, f"❌ Ошибка: {e}")
    @loader.command(ru_doc="| Полный сброс памяти и настроек")
    async def gpresetcmd(self, m: Message):
        try:
            if not m.is_group: return await self._ans(m, self.strings("og"))
            st = self._chs[m.chat_id]
            st.msgs.clear(); st.rec.clear(); st.tfq.clear(); st.mkv.clear(); st.mkv3.clear(); st.mkv4.clear(); st.mds.clear(); st.ign.clear(); st.my_outs.clear(); st.usr_cd.clear()
            st.last_mid = 0; st.parsed_cnt = 0
            st.on = False
            for p, d in [("lim", "d_lim"), ("min_m", "d_min"), ("r_ch", "d_ch"), ("m_ch", "d_mch"), ("my_ch", "d_mych"), ("cd_m", "d_cdm"), ("cd_x", "d_cdx")]:
                setattr(st, p, self.config[d])
            self._sv()
            await self._ans(m, self.strings("rst_ok"), log=True)
            try: await asyncio.get_event_loop().run_in_executor(None, self._sv_br)
            except Exception as e:
                if self._c: self._c.loop.create_task(self._log(f"<b>[RESET EXEC ERR]</b> <code>{e}</code>"))
        except Exception as e: await self._ans(m, f"❌ Ошибка: {e}")
    @loader.command(ru_doc="| Полный сброс памяти чата")
    async def gpclearcmd(self, m: Message):
        try:
            if not m.is_group: return await self._ans(m, self.strings("og"))
            st = self._chs[m.chat_id]
            st.msgs.clear(); st.rec.clear(); st.tfq.clear(); st.mkv.clear(); st.mkv3.clear(); st.mkv4.clear(); st.mds.clear(); st.ign.clear(); st.my_outs.clear(); st.usr_cd.clear()
            st.last_mid = 0; st.parsed_cnt = 0
            await self._ans(m, self.strings("clr"))
            try: await asyncio.get_event_loop().run_in_executor(None, self._sv_br)
            except Exception as e:
                if self._c: self._c.loop.create_task(self._log(f"<b>[CLEAR EXEC ERR]</b> <code>{e}</code>"))
        except Exception as e: await self._ans(m, f"❌ Ошибка: {e}")
    @loader.command(ru_doc="| Выключить ВЕЗДЕ")
    async def gpkillcmd(self, m: Message):
        try:
            self._glob_stop = True
            for st in self._chs.values(): st.on = False
            self._sv()
            await self._ans(m, self.strings("kill"), log=True)
            try: await asyncio.get_event_loop().run_in_executor(None, self._sv_br)
            except Exception as e:
                if self._c: self._c.loop.create_task(self._log(f"<b>[KILL EXEC ERR]</b> <code>{e}</code>"))
        except Exception as e: await self._ans(m, f"❌ Ошибка: {e}")
    @loader.command(ru_doc="<k> <v> [target_group] | Настройка параметров")
    async def gpsetcmd(self, m: Message):
        try:
            a = utils.get_args_raw(m).strip().split()
            if len(a) < 2:
                return await self._ans(m, self.strings("h_set"))
            p = a[0].lower()
            raw_v = a[1]
            if not re.fullmatch(r"-?\d+", raw_v):
                return await self._ans(m, "❌ Значение должно быть целым числом.")
            v = int(raw_v)
            chat_map = {
                "lim": ("lim", 0, 5000000),
                "min": ("min_m", 0, 500),
                "ch": ("r_ch", 0, 100),
                "mch": ("m_ch", 0, 100),
                "mych": ("my_ch", 0, 100),
                "cdm": ("cd_m", 0, 120),
                "cdx": ("cd_x", 0, 240),
            }
            glob_map = {
                "bpon": ("bp_on", 0, 1, False),
                "bpint": ("bp_int", self._bp_interval_min, self._bp_interval_max, False),
                "react": ("react_ch", 0, 100, False),
                "logerr": ("log_err", 0, 1, True),
                "logstl": ("log_stl", 0, 1, True),
                "logbkp": ("log_bkp", 0, 1, True),
                "loglrn": ("log_lrn", 0, 1, True),
                "logans": ("log_ans", 0, 1, True),
            }
            if p in glob_map:
                key, mn, mx, as_bool = glob_map[p]
                val = max(mn, min(v, mx))
                if key == "bp_int":
                    val = self._sanitize_bp_interval(val)
                self.config[key] = bool(val) if as_bool else int(val)
                await self._ans(m, self.strings("set").format(p, val), log=True)
                return
            if p not in chat_map:
                return await self._ans(m, "❌ Неверный параметр.")
            if len(a) >= 3:
                cid = await self._resolve_chat_target(a[2], current=m.chat_id if getattr(m, "is_group", False) else None, require_group=True)
            else:
                if not getattr(m, "is_group", False):
                    return await self._ans(m, "❌ Для параметров группы укажи target_group либо запусти команду в группе.")
                cid = int(m.chat_id)
            k, mn, mx = chat_map[p]
            val = max(mn, min(v, mx))
            st = self._chs[cid]
            setattr(st, k, val)
            self._sv()
            await self._ans(m, f"⚙️ Параметр <code>{p}</code> для <code>{cid}</code> = <code>{val}</code>", log=True)
            try:
                await asyncio.get_event_loop().run_in_executor(None, self._sv_br)
            except Exception:
                pass
        except Exception as e:
            await self._ans(m, f"❌ Ошибка: {e}")

    @loader.command(ru_doc="[all|here|chat...] | Подсистема резервных копий")
    async def gpbackupcmd(self, m: Message):
        try:
            args_raw = utils.get_args_raw(m).strip()
            if not args_raw:
                return await self._show_backup_help(m)
            a = [x for x in re.split(r"[\s,]+", args_raw) if x]
            mode = a[0].lower()
            if mode in {"status", "st", "s"}:
                return await self._ans(m, await self._format_backup_status())
            if mode == "trust":
                if len(a) == 1 and getattr(m, "is_reply", False):
                    rep = await m.get_reply_message()
                    payload = await self._extract_payload_from_message(rep, ("GPK2_",))
                    if payload:
                        uid = self._parse_keycard_payload(payload)
                        if uid:
                            return await self._ans(m, self.strings("bp_trust_imported").format(uid))
                        return await self._ans(m, "❌ Не удалось импортировать keycard.")
                    uid = int(getattr(rep, "sender_id", 0) or 0)
                    if uid <= 0:
                        return await self._ans(m, "❌ Не удалось определить пользователя для trust.")
                    await self._send_keycard(uid)
                    return await self._ans(m, self.strings("bp_trust_sent").format(uid))
                if len(a) < 2:
                    return await self._ans(m, "❌ Использование: <code>.gpbackup trust &lt;user|reply&gt;</code>")
                uid = await self._resolve_user_target(a[1])
                await self._send_keycard(uid)
                return await self._ans(m, self.strings("bp_trust_sent").format(uid))
            if mode == "share":
                if len(a) < 3:
                    return await self._ans(m, "❌ Использование: <code>.gpbackup share &lt;user&gt; &lt;targets...&gt;</code>")
                uid = await self._resolve_user_target(a[1])
                trusted = self._load_trust_keys()
                if str(uid) not in trusted:
                    return await self._ans(m, self.strings("bp_trust_missing").format(uid))
                targets = a[2:]
                if len(targets) == 1 and targets[0].lower() == "all":
                    cids = None
                else:
                    cids = [await self._resolve_chat_target(t, current=m.chat_id if getattr(m, "is_group", False) else None, require_group=True) for t in targets]
                await self._ans(m, "⚙️ Формирование шифрованного бэкапа для передачи...")
                await self._bp_up(m, manual=True, target_cids=cids, recipient_ids=[uid], out_chat=uid)
                return
            if mode == "all":
                cids = None
            else:
                cids = [await self._resolve_chat_target(t, current=m.chat_id if getattr(m, "is_group", False) else None, require_group=True) for t in a]
            await self._ans(m, "⚙️ Формирование бэкапа...")
            await self._bp_up(m, manual=True, target_cids=cids)
        except Exception as e:
            await self._ans(m, f"❌ Ошибка бэкапа: {e}")

    @loader.command(ru_doc="| Статус системы бэкапов")
    async def gpbackupscmd(self, m: Message):
        try:
            await self._ans(m, await self._format_backup_status())
        except Exception as e:
            await self._ans(m, f"❌ Ошибка: {e}")

    @loader.command(ru_doc="[--force] | Восстановить данные из реплая на бэкап")
    async def gprestorecmd(self, m: Message):
        try:
            if not getattr(m, "is_reply", False):
                return await self._ans(m, "❌ Команда используется реплаем на файл/текст GPB2 или GPB.")
            rep = await m.get_reply_message()
            payload = await self._extract_payload_from_message(rep, ("GPB2_", "GPB_"))
            if not payload:
                return await self._ans(m, "❌ Не найден payload бэкапа в реплае.")
            args = utils.get_args_raw(m).lower()
            if "--force" in args or "-f" in args:
                return await self._restore_from_payload(m, payload)
            token = os.urandom(8).hex()
            self._pending_restore[token] = {"chat_id": int(rep.chat_id), "reply_id": int(rep.id), "ts": int(time.time())}
            if getattr(self, "inline", None) and hasattr(self.inline, "form"):
                try:
                    await self.inline.form(
                        text=f"{self.strings('bp_restore_force')}\n\nПодтвердить восстановление?",
                        message=m,
                        reply_markup=[
                            [
                                {"text": "Подтвердить", "callback": self._cb_restore_confirm, "args": (token,)},
                                {"text": "Отмена", "callback": self._cb_restore_cancel, "args": (token,)},
                            ]
                        ],
                    )
                    return
                except Exception:
                    pass
            await self._ans(m, "⚠️ Для подтверждения без inline используй <code>.gprestore --force</code>.")
        except Exception as e:
            await self._ans(m, f"❌ Ошибка: {e}")

    @loader.command(ru_doc="<чат/here> <команда> | Скрытый режим")
    async def gphcmd(self, m: Message):
        try:
            if self._is_restricted_mode():
                return await self._ans(m, self.strings("upd_lock").format(reason=self._restricted_reason()))
            a = utils.get_args_raw(m).strip().split(maxsplit=1)
            if len(a) < 2:
                return await self._ans(m, self.strings("h_gph"))
            target_str, cmd_full = a[0], a[1]
            cmd_parts = cmd_full.split()
            if not cmd_parts:
                return await self._ans(m, self.strings("h_gph"))
            cmd_name = cmd_parts[0].lower().lstrip(".")
            args_str = cmd_full[len(cmd_parts[0]):].strip()
            allowed = {"gpstat", "gpinfo", "gpulse", "gpset", "gpmute", "gpignore", "gpref", "gpupdate"}
            if cmd_name not in allowed:
                return await self._ans(m, "❌ Команда недоступна в .gph.")
            tgt_id = await self._resolve_chat_target(target_str, current=m.chat_id, require_group=False)
            chat = await m.client.get_entity(tgt_id)
            is_group = self._is_group_entity(chat)
            title = getattr(chat, "title", getattr(chat, "username", str(tgt_id)))
            handler = next((getattr(self, n) for n in dir(self) if n.lower() == f"{cmd_name}cmd"), None)
            if not handler:
                return await self._ans(m, f"❌ Команда <code>{cmd_name}</code> не найдена.")

            class StealthMsg:
                def __init__(self, orig, tid, txt, log_f, grp):
                    self._o = orig
                    self.chat_id = tid
                    self.cid = tid
                    self.text = txt
                    self.message = txt
                    self._l = log_f
                    self.is_group = bool(grp)
                    self.is_private = not bool(grp)
                    self.is_reply = False
                    self.client = orig.client
                    self.id = self.mid = 0

                async def edit(self, t, **k):
                    await self._l(f"<b>[ST-EDIT]</b> {t}", cat="stl")
                    return self

                async def reply(self, t, **k):
                    await self._l(f"<b>[ST-REPLY]</b> {t}", cat="stl")
                    return self

                async def respond(self, t, **k):
                    await self._l(f"<b>[ST-RESP]</b> {t}", cat="stl")
                    return self

                async def delete(self):
                    return None

                def __getattr__(self, n):
                    return getattr(self._o, n)

            try:
                await m.delete()
            except Exception:
                pass
            await self._log(f"Stealth exec <code>.{cmd_name} {args_str}</code> в <code>{title}</code> (<code>{tgt_id}</code>)", cat="stl")
            await handler(StealthMsg(m, tgt_id, f".{cmd_name} {args_str}", self._log, is_group))
        except Exception as e:
            await self._log(f"<b>[STL ERR]</b> <code>{e}</code>", cat="err")

    async def on_dlmod(self, client, db):
        try:
            self._c, self._db = client, db
            if bool(self.get("sub_prompt_done", False)):
                return
            self.set("sub_prompt_done", True)
            me_id = 0
            try:
                me_id = int((await client.get_me()).id)
            except Exception:
                me_id = 0
            if getattr(self, "inline", None) and hasattr(self.inline, "form"):
                try:
                    anchor = await client.send_message(me_id or "me", "📡 GoyPulse V9 установлен")
                    await self.inline.form(
                        text=self.strings("sub_offer"),
                        message=anchor,
                        reply_markup=[
                            [
                                {"text": "Да", "callback": self._cb_subscribe_yes},
                                {"text": "Нет", "callback": self._cb_subscribe_no},
                            ]
                        ],
                    )
                    return
                except Exception:
                    pass
            text = self.strings("sub_offer") + "\n\n<code>.gpupdate status</code>"
            await client.send_message(me_id or "me", text)
        except Exception:
            pass

    async def client_ready(self, c, db):
        try:
            self._c, self._db = c, db
            self._my_id = (await c.get_me()).id
            self.set("gp_current_version", self._module_version)
            self._sql_lock = threading.Lock()
            self._db_conn = sqlite3.connect(self._db_path, check_same_thread=False)
            self.config["bp_int"] = self._sanitize_bp_interval(self.config.get("bp_int", 30))
            self._init_db()
            self._migrate()
            self._ld()
            self._ld_br()
            self._ensure_kp()
            self._tamper_mode = bool(self.get("gp_tamper_mode", False))
            mandatory_ver = str(self.get("gpupd_mandatory_ver", "") or "").strip()
            self._upd_mandatory_active = bool(mandatory_ver and self._cmp_ver(mandatory_ver, self._module_version) > 0)
            await self._startup_self_check()
            await self._start_bg_tasks()
            self._upd_task = self._c.loop.create_task(self._upd_loop())
            await self._log("GoyPulse V9 by goy(@samsepi0l_ovf) запущен", cat="lrn")
        except Exception as e:
            if self._c:
                self._c.loop.create_task(self._log(f"<b>[CRITICAL START ERR]</b> <code>{e}</code>", cat="err"))

    async def on_unload(self):
        try:
            if self._upd_task and not self._upd_task.done():
                self._upd_task.cancel()
                try:
                    await self._upd_task
                except BaseException:
                    pass
            self._upd_task = None
            await self._stop_bg_tasks()
            try:
                await asyncio.get_event_loop().run_in_executor(None, self._sv_br)
            except Exception:
                pass
            self._pending_restore.clear()
            if self._db_conn:
                self._db_conn.close()
                self._db_conn = None
        except Exception:
            pass
