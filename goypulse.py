# ====================================================================================================================
#   ██████╗  ██████╗ ██╗   ██╗███╗   ███╗ ██████╗ ██████╗ ██╗   ██╗██╗     ███████╗███████╗
#  ██╔════╝ ██╔═══██╗╚██╗ ██╔╝████╗ ████║██╔═══██╗██╔══██╗██║   ██║██║     ██╔════╝██╔════╝
#  ██║  ███╗██║   ██║ ╚████╔╝ ██╔████╔██║██║   ██║██║  ██║██║   ██║██║     █████╗  ███████╗
#  ██║   ██║██║   ██║  ╚██╔╝  ██║╚██╔╝██║██║   ██║██║  ██║██║   ██║██║     ██╔══╝  ╚════██║
#  ╚██████╔╝╚██████╔╝   ██║   ██║ ╚═╝ ██║╚██████╔╝██████╔╝╚██████╔╝███████╗███████╗███████║
#   ╚═════╝  ╚═════╝    ╚═╝   ╚═╝     ╚═╝ ╚═════╝ ╚═════╝  ╚═════╝ ╚══════╝╚══════╝╚══════╝
#
#   OFFICIAL USERNAMES: @goymodules | @samsepi0l_ovf
#   MODULE: goypulse
#
#   THIS MODULE IS LICENSED UNDER GNU AGPLv3, PROTECTED AGAINST UNAUTHORIZED COPYING/RESALE,
#   AND ITS ORIGINAL AUTHORSHIP BELONGS TO @samsepi0l_ovf.
#   ALL OFFICIAL UPDATES, RELEASE NOTES, AND PATCHES ARE PUBLISHED IN THE TELEGRAM CHANNEL @goymodules.
# ====================================================================================================================

# requires: cryptography
# meta developer: @goymodules
# authors: @goymodules
# Description: Нейро-автоответчик на цепях маркова с полезными функциями.
# Этот модуль разработан исключительно для личного использования и автоматизации чатов.
# Функции автообновления, логирования и скрытого режима являются легитимными инструментами
# пользователя для управления ботом и не предназначены для несанкционированного доступа.
# Любая модификация данного кода без разрешения автора крайне не рекомендуется.
# meta banner: https://raw.githubusercontent.com/sepiol026-wq/goypulse/main/assets/goypulse.png

import asyncio, base64, hashlib, hmac, json, math, os, random, re, sqlite3, time, zlib, threading, urllib.error, urllib.parse, urllib.request
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional, Set, Tuple
__version__ = (10, 0, 0)

from telethon import events, utils as tl_utils
from telethon.tl.types import Message
from .. import loader, utils

TOK_RE = re.compile(r"[a-zа-яё0-9_]+", re.I)
URL_RE = re.compile(r"https?://\S+")
CODE_FENCE_RE = re.compile(r"```|`[^`\n]{2,}`")
LONG_BLOB_RE = re.compile(r"\b(?:[01]{8,}|[a-f0-9]{16,}|[A-Za-z0-9+/]{24,}={0,2})\b", re.I)
STOP_W = {"и", "в", "во", "на", "не", "что", "это", "я", "ты", "он", "она", "оно", "мы", "вы", "они", "а", "но", "или", "да", "нет", "ну", "как", "так", "к", "ко", "из", "за", "по", "у", "от", "до", "же", "ли", "бы", "то", "для", "если", "уже", "тут", "там", "ведь", "вот", "даже", "лишь", "о", "об", "очень", "с", "со", "тоже", "только", "чем", "чтобы", "этом", "эти", "этого", "какой", "просто", "может", "раз", "два", "типа", "короче", "кст", "кстати", "вообще", "наверное", "вроде", "кажется", "однако", "хотя", "хоть", "между", "через", "около", "будто", "словно", "ровно", "почти", "вдруг", "разве", "неужели", "снова", "опять", "все", "всё", "вся", "весь", "всех", "всем", "всеми", "всею", "всея", "меня", "мне", "тебя", "тебе", "его", "ее", "её", "их", "наш", "ваш", "свой", "кто", "чей", "этот", "тот", "мой", "твой", "сам", "самый", "весь", "вся", "всё", "все", "зачем", "почему", "когда", "где", "куда", "откуда", "есть", "быть", "был", "была", "было", "были", "хочу", "хочет", "будет", "будут", "твоя", "мое", "моё", "the", "a", "an", "and", "or", "but", "is", "are", "am", "was", "were", "be", "been", "being", "in", "on", "at", "to", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "from", "up", "down", "of", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "can", "will", "just", "should", "now", "could", "would", "which", "who", "whom", "whose", "this", "that", "these", "those", "my", "your", "his", "her", "its", "our", "their", "mine", "yours", "hers", "ours", "theirs", "me", "him", "us", "them", "myself", "yourself", "himself", "herself", "itself", "ourselves", "themselves", "whose", "which", "what", "where", "when", "why", "how", "all", "another", "any", "anybody", "anyone", "anything", "both", "each", "either", "everybody", "everyone", "everything", "few", "many", "neither", "nobody", "none", "no one", "nothing", "one", "other", "others", "some", "somebody", "someone", "something", "such", "that", "these", "this", "those", "each", "which", "who", "whom", "whose", "as", "at", "by", "for", "from", "in", "into", "of", "off", "on", "onto", "out", "over", "to", "up", "with", "yet", "above", "below", "beside", "between", "beyond", "during", "except", "near", "past", "since", "through", "toward", "under", "until", "upon", "within", "without", "че", "чё", "чо", "кароч", "кароче", "типо", "ваще", "ващето", "вобще", "походу", "плз", "плиз", "спс", "пасиб", "пасиба", "пж", "дя", "нее", "неа", "нука", "мол", "типатого", "прям", "тока", "пока", "прив", "ку", "hi", "hey", "hello", "yo", "sup", "howdy", "morning", "afternoon", "evening", "night", "goodbye", "bye", "see ya", "later", "please", "thanks", "thank you", "thx", "welcome", "yw", "sorry", "excuse me", "yes", "yeah", "yep", "no", "nope", "nah", "maybe", "perhaps", "actually", "basically", "literally", "totally", "definitely", "absolutely", "probably", "sure", "ok", "okay", "alright", "fine", "cool", "great", "awesome", "good", "bad", "well", "very", "much", "many", "little", "few", "enough", "too", "so", "very", "really", "quite", "rather", "pretty", "somewhat", "almost", "nearly", "mostly", "partly", "half", "least", "most", "more", "less", "bit", "piece", "way", "far", "near", "long", "short", "high", "low", "big", "small", "new", "old", "young", "early", "late", "fast", "slow", "hard", "easy", "clear", "dark", "light", "heavy", "soft", "loud", "quiet", "strong", "weak", "rich", "poor", "hot", "cold", "warm", "cool", "dry", "wet", "clean", "dirty", "full", "empty", "open", "closed", "first", "last", "right", "wrong", "true", "false", "real", "fake"}
GRTS = {"привет", "хай", "здарова", "ку", "дарова", "салам", "куку", "добрый", "вечер", "утро", "шалом", "qq", "прив", "здрасьте", "вечерочек", "утречко", "салют", "здравствуй", "приветствую", "алоха", "дратути", "кусь", "сап", "йо", "кукушки", "здоров", "превед", " хаюшки", "добрейший", "конишуа", "бонжур", "гутен", "таг", "приветс", "дароу", "салам алейкум", "ассаламу", "hello", "hi", "hey", "greetings", "morning", "yo", "sup", "howdy", "hiya", "evening", "afternoon", "welcome", "aloha", "shalom", "hola", "bonjour", "ciao", "namaste", "heyyo", "hibro", "hi there", "wassup", "whats up", "good morning", "good evening", "good afternoon", "nice to see you", "hey there", "it's been a while", "long time no see", "lovely to meet you", "how's it going", "how are you", "what's new", "how's life", "how's things", "morning all", "hi everyone", "hello folks", "доброе", "утречко", "вечер", "в хату", "здравия", "желаю", "почтение", "приветствую всех", "хайль", "ave", "здорово", "привет", "доброе утро", "добрый день", "добрый вечер", "доброй ночи", "хайль", "здравия желаю", "низкий поклон", "моё почтение", "честь имею", "барев", "салам алейкум", "уалейкум ассалам", "нихао", "ола", "аннён", "дзякуй", "вечер в радость", "здорово бандиты", "здорово жиганы", "всем ку", "кукусики", "приветики", "даровчики", "приветствую", "добрейшего денёчка", "утреца", "доброй ночи", "салам пополам", "здаристи", "здрасьте мордасьте", "куку", "qq", "q-q", "ку", "hi", "hey", "hello", "greetings", "salutations", "morning", "evening", "afternoon", "good day", "howdy", "sup", "yo", "wassup", "welcome", "aloha", "bonjour", "ciao", "hola", "namaste", "shalom", "hiya", "hey there", "hi there", "long time no see", "lovely to meet you", "nice to meet you", "how's it going", "how are you", "how have you been", "what's up", "what's going on", "what's new", "good to see you", "pleasure to meet you", "it's an honor", "greetings everyone"}
RCTS = {"ахах", "лол", "жиза", "жестко", "имба", "топ", "пон", "кринж", "база", "хах", "треш", "ору", "рил", "мда", "пздц", "пиздец", "ебать", "бля", "блять", "чел", "капец", "шок", "ужас", "жесть", "согл", "базару", "факты", "хуйня", "дичь", "ор", "ржомба", "рофл", "кринге", "понял", "ясно", "хуй", "пизда", "охренеть", "охуеть", "писец", "бб", "ок", "окей", "окда", "спс", "спасибо", "сигма", "вумен", "скуф", "нормис", "альтушка", "масик", "тюбик", "штрих", "чечик", "имбово", "разрывная", "свэг", "чиназес", "легенда", "гигачад", "базированно", "кринжатина", "дэмн", "люто", "чертовски", "пиздато", "офигенно", "кайф", "кайфово", "найс", "ладно", "бебра", "абуз", "тильт", "флекс", "шейм", "горишь", "слит", "попуск", "агро", "душно", "душнила", "ахаха", "лолз", "мем", "рофлишь", "кринжую", "нереально", "круто", "четко", "красава", "красавчик", "харош", "хорош", "гений", "легендарно", "сильно", "жёстко", "безумно", "lmao", "rofl", "wtf", "omg", "bruh", "based", "cringe", "fr", "frfr", "ong", "tbh", "ngl", "idk", "idc", "stfu", "gtfo", "lmfao", "damn", "sheesh", "bet", "cap", "nocap", "dope", "lit", "trash", "awesome", "cool", "sick", "wild", "insane", "legit", "standard", "classic", "vibes", "mood", "shook", "dead", "skull", "че за", "шо за", "пиздец", "ппц", "ну и ну", "ох", "ах", "ого", "ух ты", "нифига", "ничоси", "воу", "эщкере", "вуху", "ура", "еее", "йоу", "хоспади", "господи", "боже мой", "матерь божья", "бляха муха", "сука", "епт", "епта", "ебаный рот", "пидорас", "гандон", "хуило", "еблан", "дебил", "даун", "лошара", "чушпан", "пацаны", "ребята", "братва", "брат", "братик", "бро", "кентуфурик", "кореш", "дружбан", "парень", "девушка", "тян", "кун", "лоля", "вайфу", "краш", "шип", "шипперить", "кринжануть", "рофляночка", "шуточка", "прикол", "кек", "кеке", "кекаю", "пушка", "бомба", "ракета", "космос", "вышка", "огонь", "горячо", "жара", "мощно", "крутяк", "заебись", "охуенно", "чётко", "збс", "гг", "wp", "gl", "hf", "ggwp", "ez", "ezez", "сидим", "кайфуем", "отдыхаем", "работаем", "учимся", "спать", "жрать", "бухать", "курить", "парить", "жидкость", "жижа", "под", "вейп", "электронка", "ашка", "одноразка", "кальян", "пиво", "водка", "виски", "вино", "коньяк", "вискарь", "текила", "ром", "джин", "шампусик", "шампанское", "лимонад", "кола", "пепси", "энергетик", "монстр", "редбулл", "адреналин", "флэш", "торч", "солевой", "наркоман", "алкаш", "бомж", "бич", "нищий", "богатый", "мажор", "нищеброд", "нищий", "лох", "терпила", "куколд", "омежка", "сигмач", "гигачад", "папич", "величайший", "видос", "стрим", "видосик", "видяха", "карта", "проц", "комп", "пк", "ноут", "клава", "мышка", "моник", "наушники", "уши", "микро", "вебка", "телефон", "смартфон", "айфон", "самсунг", "андроид", "дискорд", "тг", "телеграм", "вк", "инста", "тикток", "ютуб", "твич", "кик", "сайт", "интернет", "сеть", "вайфай", "скорость", "пинг" , "лаги", "фризы", "баги", "ошибки", "еррор", "хелп", "помогите", "спасите", "админ", "модер", "владелец", "овнер", "создатель", "хуйло", "красавец", "молодец", "умничка", "солнышко", "зайка", "котик", "киса", "лапочка", "милота", "кавай", "ня", "няшка", "ня кавай", "ураа", "еее", "крутотенюшка", "лучший", "лучшая", "the best", "classic", "standard", "norm", "normal", "okey", "fine", "good", "nice", "very good", "excellent", "perfect", "amazing", "wonderful", "terrible", "awful", "bad", "badly", "horrible", "shit", "holy shit", "omg", "wtf", "wth", "lmao", "lmfao", "lol", "rofl", "bruh", "damn", "sheesh", "fr", "frfr", "real", "really", "literally", "literally me", "me", "mood", "vibes", "vibe", "aesthetic", "standard", "basic", "premium", "lux", "luxury", "rich", "poor", "money", "cash", "dollars", "bucks", "rubles", "crypto", "bitcoin", "eth", "nft", "scam", "scammer", "legit", "safe", "scary", "fear", "horror", "spooky", "creepy", "weird", "strange", "bizarre", "odd", "funny", "hilarious", "joke", "prank", "troll", "trolling", "hater", "fan", "fandom", "stan", "simp", "incel", "femcel", "chad", "gigachad", "sigma", "alpha", "beta", "omega", "cuck", "cuckold", "soyboy", "npc", "main character", "hero", "villain", "boss", "noob", "pro", "hacker", "cheater", "admin", "mod", "staff", "user", "player", "game", "gaming", "stream", "steamer", "video", "content", "creator", "famous", "popular", "viral", "tranding", "tags", "reposts", "likes", "views", "subscribers", "subs", "goat", "legend", "icon", "masterpiece", "peak", "mid", "flop", "w", "l", "massive w", "huge l", "ratio", "canceled", "cancelled", "toxic", "wholesome", "cursed", "blessed", "blursed", "sus", "imposter", "amongus", "amogus", "vent", "sussy", "baka", "pog", "poggers", "pogchamp", "kekw", "omegalul", "pepeh", "kappa", "sadge", "monkas", "feelsbadman", "feelsgoodman", "clap", "ez", "ggwp", "get rekt", "rekt", "destroyed", "owned", "skill issue", "noob", "get gud", "cope", "seethe", "mald", "cry about it", "stay mad", "touch grass", "ratio", "owned", "powned", "pwned", "clapped", "dumped", "washed", "washed up", "fraud", "overrated", "underrated", "sleeper", "banger", "slaps", "fire", "heat", "cold", "frozen", "icy", "drip", "drippy", "swag", "yolo", "swag", "gucci", "prada", "hype", "hypebeast", "og", "real one", "homie", "bestie", "brother", "sister", "fam", "squad", "crew", "gang", "tribe", "folk", "folks", "peeps", "people"}
QW = ("что", "как", "почему", "зачем", "когда", "где", "кто", "кого", "кому", "кем", "чем", "откуда", "куда", "чей", "че", "чё", "чо", "хули", "всмысле", "какого", "хто", "шо", "какой", "какая", "какие", "херли", "какого", "хрена", "поч", "почему бы", "почем", "зачем это", "хто то", "куда это", "откуда это", "какие новости", "че за", "чё за", "шо за", "че там", "чё там", "шо там", "what", "how", "why", "when", "where", "who", "whom", "whose", "which", "how come", "what for", "whats up", "whats going on", "what about", "че почем", "чё почём", "шо почём", "сколько", "скока", "скоко", "почём", "за сколько", "на фига", "нафига", "на хуя", "нахуя", "какого хуя", "че за фигня", "че за дичь", "чё за треш", "как это", "что это", "кто это", "где это", "когда это", "почему так", "зачем ты", "что делаешь", "как дела", "чё каво", "чё кого", "шо там", "что нового", "какие планы", "ты где", "вы где", "мы где", "куда идем", "что купить", "сколько стоит", "поможешь", "сможешь", "хочешь", "знаешь", "помнишь", "слышал", "видел", "what", "how", "why", "when", "where", "who", "whom", "whose", "which", "how come", "what for", "whats up", "whats going on", "what about", "how many", "how much", "how long", "how far", "how often", "who is", "what is", "where is", "when is", "why is", "can you", "could you", "would you", "do you", "did you", "have you", "are you", "is it", "will you", "shall we", "may I", "what's that", "who's there", "whose turn", "any news", "any idea", "anyone know", "how to", "why not", "what if", "is there", "are there", "shall I", "should I", "could I")
EMO_M = {"смех": ["😂", "🤣", "💀", "😭", "хах", "ахах", "😹", "🤭", "пхпх", "хахаха", "ору", "🤣", "😅", "😆", "😸", "😂", "🤣", "💀", "😭", "хах", "ахах", "😹", "🤭", "пхпх", "хахаха", "ору", "🤣", "😅", "😆", "😸", "😁", "😃", "😄", "😅", "😆", "😅", "😂", "🤣", "😹", "😸", "😻", "😽", "🫠", "🙃", "🤪", "😝", "😜", "😛", "🤤", "😤", "🤯", "🥳", "😎", "🤡", "👺", "👻", "👽", "💩", "🔥", "💯", "💥", "<tg-emoji emoji-id=5253877736207821121>🔥</tg-emoji>️", "✨", "🌟"], "агр": ["🤡", "🤬", "🗿", "👺", "мда", " трэш", "😤", "🤦‍♂️", "🤦‍♀️", "☠️", "<tg-emoji emoji-id=5255831443816327915>🗑</tg-emoji>", "😡", "👿", "🖕", "😠", "👿", "👹", "🖕", "🤬", "💢", "🤡", "🤬", "🗿", "👺", "мда", "трэш", "😤", "🤦‍♂️", "🤦‍♀️", "☠️", "<tg-emoji emoji-id=5255831443816327915>🗑</tg-emoji>", "😡", "👿", "🖕", "😠", "👿", "👹", "🖕", "🤬", "💢", "👎", "🤮", "💩", "🧨", "🔫", "🗡️", "🔪", "⛓️", "💣", "🚬", "🥀", "🔨", "⚒️", "🛠️", "⛏️", "🪚", "🪓", "🧱", "🪨", "🪵", "⛓️", "💣", "🧨", "💥", "🗡️", "⚔️", "🏹", "<tg-emoji emoji-id=5253780051471642059>🛡</tg-emoji>", "⚰️", "🪦", "⚱️", "🏺"], "нейтрал": ["👀", "🤔", "пон", "🚬", "ну ок", "ладно", "🤷‍♂️", "🤷‍♀️", "🙃", "🧐", "🥱", "🥴", "😶", "🌝", "🌚", "🫠", "😑", "😐", "👀", "🤔", "пон", "🚬", "ну ок", "ладно", "🤷‍♂️", "🤷‍♀️", "🙃", "🧐", "🥱", "🥴", "😶", "🌝", "🌚", "🫠", "😑", "😐", "🚶‍♂️", "🚶‍♀️", "🪴", "☁️", "🌊", "☕️", "🛋️", "💻", "<tg-emoji emoji-id=5256230583717079814>📝</tg-emoji>", "🖊️", "📅", "📎", "<tg-emoji emoji-id=5253521692008917018>🌙</tg-emoji>", "<tg-emoji emoji-id=5253590213917158323>💬</tg-emoji>", "💭", "🗯️", "♠️", "♣️", "♥️", "♦️", "🃏", "🎴", "🎭", "🎨", "🧵", "🧶", "🎹", "🎺", "🎸", "🎻", "🥁", "🪗", "🎧", "🎤", "🎬"], "шок": ["😱", "🤯", "😳", "😨", "🙀", "охуеть", "😲", "😯", "😧", "😮", "😵", "😵‍💫", "😱", "🤯", "😳", "😨", "🙀", "охуеть", "😲", "😯", "😧", "😮", "😵", "😵‍💫", "‼️", "❓", "🆘", "💥", "🔥", "💨", "🌊", "🌩️", "⛈️", "🌪️", "🌊", "🌋", "☄️", "<tg-emoji emoji-id=5253877736207821121>🔥</tg-emoji>️", "💥", "🔥", "🧨", "💣", "🔫", "⛏️", "⚔️", "<tg-emoji emoji-id=5253780051471642059>🛡</tg-emoji>", "⚰️", "🪦", "👻", "👹", "👺", "💀", "👽", "💩", "🤡", "🧞‍♂️", "🧞‍♀️", "🧟‍♂️", "🧟‍♀️"]}


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
    auto_off_u: float = 0.0
    lrn: bool = False
    last_usr: int = 0
    last_tone: str = "нейтрал"
    last_t: float = 0.0
    cid: int = 0


@loader.tds
class GoyPulseMod(loader.Module):
    """Нейро-автоответчик GoyPulse by goy(@samsepi0l_ovf)"""
    strings = {
        "name": "GoyPulse",
        "brand": "GoyPulse by goy(@samsepi0l_ovf)",
        "og": "<tg-emoji emoji-id=5253780051471642059>🛡</tg-emoji> <b>[GoyPulse]</b> Только для групп.",
        "on": "<tg-emoji emoji-id=5253877736207821121>🔥</tg-emoji> <b>[GoyPulse]</b> Система активирована.\n<i>Теперь я обучаюсь и буду отвечать в этом чате.</i>{}",
        "off": "<tg-emoji emoji-id=5253521692008917018>🌙</tg-emoji> <b>[GoyPulse]</b> Система деактивирована.\n<i>Я больше не буду отвечать здесь.</i>",
        "ref_st": "🧬 <b>[Обучение]</b> Анализ истории сообщений...{}",
        "ref_upd": "<tg-emoji emoji-id=5256250435055920155>1️⃣</tg-emoji> <b>[Обучение]</b> В процессе... <code>[{}{}]</code>\n\n<tg-emoji emoji-id=5253931337399674296>2️⃣</tg-emoji> <b>Статистика:</b>\n├─ 💠 Словарь: <code>{}</code>\n├─ <tg-emoji emoji-id=5255917867148257511>🖼</tg-emoji> Медиа: <code>{}</code>\n├─ <tg-emoji emoji-id=5256230583717079814>📝</tg-emoji> Слова: <code>{}</code>\n└─ <tg-emoji emoji-id=5253877736207821121>🔥</tg-emoji> Скорость: <code>{}</code> msg/s\n\n<tg-emoji emoji-id=5255971360965930740>🕔</tg-emoji> <b>Осталось:</b> <code>{}</code>",
        "ref_dn": "<tg-emoji emoji-id=5255813619702049821>✅</tg-emoji> <b>[Обучение]</b> Успешно завершено!\n\n📈 <b>Итоги:</b>\n├─ <tg-emoji emoji-id=5253590213917158323>💬</tg-emoji> Сообщений: <code>{}</code>\n├─ <tg-emoji emoji-id=5256250435055920155>1️⃣</tg-emoji> Словарь: <code>{}</code>\n├─ <tg-emoji emoji-id=5256230583717079814>📝</tg-emoji> Всего слов: <code>{}</code>\n└─ <tg-emoji emoji-id=5255917867148257511>🖼</tg-emoji> Медиа: <code>{}</code> {md_details}\n\n<i>GoyPulse готов к работе!</i>",
        "st": "<tg-emoji emoji-id=5253931337399674296>2️⃣</tg-emoji> <b>Статус GoyPulse</b> | <code>by goy(@samsepi0l_ovf)</code>\n\n<tg-emoji emoji-id=5253713110111365241>📍</tg-emoji> <b>Состояние:</b> {on}\n📈 <b>База:</b> <code>{pc}</code> msg | <tg-emoji emoji-id=5256230583717079814>📝</tg-emoji> <code>{wc}</code> слов\n<tg-emoji emoji-id=5256250435055920155>1️⃣</tg-emoji> <b>Словарь:</b> <code>{vk}</code> связок\n<tg-emoji emoji-id=5255917867148257511>🖼</tg-emoji> <b>Медиа:</b> <code>{md}</code> | <tg-emoji emoji-id=5253527438675158560>🔕</tg-emoji> <b>Игнор:</b> <code>{ig}</code>\n\n<tg-emoji emoji-id=5253952855185829086>⚙️</tg-emoji> <b>Конфигурация:</b>\n├─ 🎲 Шанс (обыч): <code>{c}%</code>\n├─ <tg-emoji emoji-id=5253464392850221514>🔃</tg-emoji> Шанс (реплай): <code>{my}%</code>\n├─ 🎨 Шанс (медиа): <code>{mc}%</code>\n└─ <tg-emoji emoji-id=5255971360965930740>🕔</tg-emoji> Задержка: <code>{cd}</code>\n\n🗣️ <b>Актуальные темы:</b>\n<code>{tw}</code>{warn}",
        "set": "<tg-emoji emoji-id=5253952855185829086>⚙️</tg-emoji> <b>[Настройки]</b> Параметр <code>{}</code> обновлен: <code>{}</code>",
        "mute": "🤫 <b>[Тсс!]</b> Бот отправлен отдыхать на <code>{}</code> мин.",
        "kill": "🛑 <b>[HALT]</b> Глобальная остановка всех модулей GoyPulse.",
        "info": "<tg-emoji emoji-id=5256250435055920155>1️⃣</tg-emoji> <b>[Аналитика]</b> Вибрации чата\n\n📡 <b>Активность:</b> <code>{act}</code>\n🎭 <b>Тональность:</b>\n{tonality}\n\n🔥 <b>Топ обсуждений:</b>\n<code>{tw}</code>{warn}",
        "ign_add": "🚷 <b>[Игнор]</b> Пользователь добавлен в черный список.",
        "ign_del": "<tg-emoji emoji-id=5255813619702049821>✅</tg-emoji> <b>[Игнор]</b> Пользователь удален из черного списка.",
        "clr": "<tg-emoji emoji-id=5255831443816327915>🗑</tg-emoji> <b>[Очистка]</b> Память текущего чата полностью стерта.",
        "rst_ok": "<tg-emoji emoji-id=5253464392850221514>🔃</tg-emoji> <b>[Сброс]</b> Настройки и память сброшены успешно.",
        "log_err": "<tg-emoji emoji-id=5253864872780769235>❗️</tg-emoji> <b>[ERROR]</b> Ошибка: <code>{}</code>",
        "log_ok": "<tg-emoji emoji-id=5255813619702049821>✅</tg-emoji> <b>[Stealth]</b> Команда выполнена в <code>{}</code>\n\n<tg-emoji emoji-id=5256230583717079814>📝</tg-emoji> <b>Ответ:</b>\n<code>{}</code>",
        "react_ok": "✨ <b>[Реакция]</b> Бот отреагировал на сообщение.",
        "h_pulse": "🔌 <b>[Usage] .gpulse [on|off] [time]</b>\n\nВключает или отключает обработку сообщений ботом в текущем чате.\n\n<b>Инструкция:</b>\n├ <code>.gpulse on</code> — включить без таймера.\n├ <code>.gpulse on 30</code> — включить на 30 минут.\n├ <code>.gpulse on 2h</code> — включить на 2 часа.\n└ <code>.gpulse off</code> — выключить сразу.\n\n<i>Поддерживаются суффиксы: s, m, h, d и русские м, ч, д.</i>\n\n<code>GoyPulse by goy(@samsepi0l_ovf)</code>",
        "h_set": "<tg-emoji emoji-id=5253952855185829086>⚙️</tg-emoji> <b>Настройки GoyPulse</b> | <code>by goy(@samsepi0l_ovf)</code>\n\n"
                 "Использование: <code>.gpset &lt;ключ&gt; &lt;значение&gt; [target_group]</code>\n\n"
                 "🌐 <b>Глобальные параметры:</b>\n"
                 "├ <code>bpon [1/0]</code> — Автоматический бэкап базы.\n"
                 "├ <code>bpint [5-1440]</code> — Интервал бэкапа в минутах.\n"
                 "├ <code>react [0-100]</code> — Шанс реакции (эмодзи) на сообщение.\n"
                 "├ <code>updint [0-720]</code> — Интервал проверки обновлений (ч, 0=выкл).\n"
                 "└ <b>Логирование (1=вкл, 0=выкл):</b>\n"
                 "  ├ <code>logerr</code> — Ошибки модуля.\n"
                 "  ├ <code>logstl</code> — Скрытые команды (.gph).\n"
                 "  ├ <code>logbkp</code> — События бэкапа.\n"
                 "  ├ <code>loglrn</code> — Процесс обучения.\n"
                 "  └ <code>logans</code> — Ответы бота (в лог-канал).\n\n"
                 "🏘️ <b>Параметры группы:</b>\n"
                 "├ <code>lim [0-5M]</code> — Лимит сообщений для обучения.\n"
                 "├ <code>min [0-500]</code> — Минимум сообщений для активации.\n"
                 "├ <code>ch [0-100]</code> — Шанс ответа на обычное сообщение.\n"
                 "├ <code>mch [0-100]</code> — Шанс ответа медиа-контентом.\n"
                 "├ <code>mych [0-100]</code> — Шанс ответа на реплы/меншены.\n"
                 "├ <code>cdm [0-120]</code> — Минимальная пауза между ответами (сек).\n"
                 "└ <code>cdx [0-240]</code> — Максимальная пауза (сек).\n\n"
                 "📝 <b>Примеры:</b>\n"
                 "├ <code>.gpset ch 50</code> — 50% шанс в текущем чате.\n"
                 "└ <code>.gpset react 20</code> — Глобальный шанс реакции 20%.\n\n"
                 "<i>GoyPulse by goy(@samsepi0l_ovf)</i>",
        "h_mute": "<tg-emoji emoji-id=5253690110561494560>🔇</tg-emoji> <b>[Usage] .gpmute <минуты></b>\n\nВременно отключает ответы бота, сохраняя процесс обучения и сбора статистики.\n\n<b>Примеры:</b>\n├ <code>.gpmute 30</code> — Замолчать на полчаса.\n├ <code>.gpmute 1440</code> — Замолчать на сутки.\n└ <code>.gpmute 0</code> — Снять ограничение немедленно.\n\n<code>GoyPulse by goy(@samsepi0l_ovf)</code>",
        "h_ign": "🚷 <b>[Usage] .gpignore</b>\n\nДобавляет или удаляет пользователя из черного списка бота.\n\n<b>Как использовать:</b>\n1. Найдите сообщение пользователя.\n2. Ответьте на него (Reply) командой <code>.gpignore</code>.\n\n<i>Результат: Бот не будет обучаться на нем и не будет ему отвечать.</i>\n\n<code>GoyPulse by goy(@samsepi0l_ovf)</code>",
        "h_gph": "🕵️ <b>[Usage] .gph <цель> <команда></b>\n\nВыполнение команд GoyPulse в любом чате анонимно.\n\n<b>Параметры:</b>\n├ <code>цель</code> — ID чата, юзернейм или слово <code>here</code>\n└ <code>команда</code> — Любая команда без точки (напр. <code>gpstat</code>)\n\n<b>Примеры:</b>\n├ <code>.gph -100... gpstat</code> — Статус чужого чата.\n├ <code>.gph @username gpinfo</code> — Вайб в личке.\n└ <code>.gph here gpclear</code> — Скрытая очистка.\n\n<code>GoyPulse by goy(@samsepi0l_ovf)</code>",

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
                                    cv("react_ch", 15, lambda: "Шанс реакции (%)", validator=vi),
            cv("log_err", True, lambda: "Лог: Ошибки", validator=vb),
            cv("log_stl", True, lambda: "Лог: Скрытый режим", validator=vb),
                        cv("log_lrn", True, lambda: "Лог: Обучение", validator=vb),
            cv("log_ans", False, lambda: "Лог: Ответы бота", validator=vb),
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
        self._stop_event = None
        self._max_chat_tokens = 400000
        self._max_markov_edges = 1200000
        self._module_version = "9.2.0"
        self._module_file_name = "goypulse.py"
        self._sub_channel = "@goy_ai"
        self._tamper_mode = False


    def _sql(self, q: str, p: tuple = (), fetch: bool = False, commit: bool = True):
        try:
            qn = q.strip().upper()
            is_begin = qn.startswith("BEGIN")
            is_commit = qn.startswith("COMMIT")
            is_rollback = qn.startswith("ROLLBACK")
            is_trans = is_begin or is_commit or is_rollback
            if self._db_conn:
                with self._sql_lock:
                    cur = self._db_conn.cursor()
                    try:
                        cur.execute(q, p)
                    except sqlite3.Error as e:
                        msg = str(e).lower()
                        if is_begin and ("within a transaction" in msg or "already in a transaction" in msg):
                            return None
                        if (is_commit or is_rollback) and ("no transaction is active" in msg or "not in a transaction" in msg):
                            return None
                        raise
                    res = cur.fetchall() if fetch else None
                    if commit and not fetch and "SELECT" not in q.upper() and not is_trans:
                        if self._db_conn.in_transaction:
                            self._db_conn.commit()
                    return res
            else:
                if is_trans:
                    return None
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

                                       
        self._sql("CREATE TABLE IF NOT EXISTS chats (cid INTEGER PRIMARY KEY, on_off INTEGER, lim INTEGER, min_m INTEGER, r_ch INTEGER, m_ch INTEGER, my_ch INTEGER, cd_m INTEGER, cd_x INTEGER, bp_on INTEGER, bp_int INTEGER, react_ch INTEGER, last_mid INTEGER, parsed_cnt INTEGER, w_cnt INTEGER, cd_u REAL, mute_u REAL, auto_off_u REAL, last_usr INTEGER, last_tone TEXT, last_t REAL)")

        
                                                 
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
                    ("mute_u", "REAL"), ("auto_off_u", "REAL"), ("last_usr", "INTEGER"), ("last_tone", "TEXT"),
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
    def _parse_duration_seconds(self, raw: str) -> int:
        s = (raw or "").strip().lower()
        if not s:
            return 0
        m = re.fullmatch(r"(\d{1,7})([smhdмчд]?)", s)
        if not m:
            raise ValueError("bad duration")
        val = int(m.group(1))
        unit = m.group(2) or "m"
        mult = {
            "s": 1,
            "m": 60,
            "h": 3600,
            "d": 86400,
            "м": 60,
            "ч": 3600,
            "д": 86400,
        }.get(unit, 60)
        return max(0, min(val * mult, 30 * 86400))
    def _is_code_like(self, t: str, tk: Tuple[str, ...] = ()) -> bool:
        t = (t or "").strip()
        if not t:
            return False
        low = t.lower()
        lines = [ln.strip() for ln in t.splitlines() if ln.strip()]
        if CODE_FENCE_RE.search(t):
            return True
        if len(lines) >= 3:
            code_lines = 0
            for ln in lines[:12]:
                if re.search(r"[{}[\];<>]|=>|==|!=|:=|::|#include\b|\b(import|from|class|def|return|await|async|func|package|var|let|const|public|private)\b", ln, re.I):
                    code_lines += 1
            if code_lines >= max(2, len(lines) // 2):
                return True
        if re.search(r"\b(import|from|class|def|return|async|await|lambda|console\.|print\(|SELECT\b|INSERT\b|UPDATE\b|DELETE\b|function\s*\(|var\s+\w+\s*=|let\s+\w+\s*=|const\s+\w+\s*=)\b", t, re.I):
            return True
        if sum(ch in "{}[]();<>/\\=*_#" for ch in t) >= max(6, len(t) // 8):
            return True
        if tk and len(tk) >= 4 and sum(1 for x in tk if any(ch.isdigit() for ch in x) or "_" in x) >= max(3, len(tk) // 2):
            return True
        if low.startswith(("traceback", "error:", "exception:", "warning:", "info:", "debug:")) and len(lines) > 1:
            return True
        return False
    def _ig(self, t: str) -> bool:
        tks = set(self._tks(t))
        return bool(tks & GRTS or any(any(x.startswith(g) for g in GRTS if len(g) > 3) for x in tks))
    def _ir(self, t: str) -> bool:
        tks = set(self._tks(t))
        return bool(tks & RCTS or any(any(x.startswith(r) for r in RCTS if len(r) > 3) for x in tks))
    def _is_bad_text(self, t: str, tk: Tuple[str, ...] = (), allow_short: bool = True) -> bool:
        t = (t or "").strip()
        if not t:
            return True
        if len(t) > 1000 or "GoyPulse" in t:
            return True
        if t.startswith(("/", ".", "!")):
            return True
        if self._is_code_like(t, tk):
            return True
        if LONG_BLOB_RE.search(t):
            return True
        digits = sum(ch.isdigit() for ch in t)
        alpha = sum(ch.isalpha() for ch in t)
        non_space = sum(not ch.isspace() for ch in t) or 1
        punct = sum(not ch.isalnum() and not ch.isspace() for ch in t)
        if digits / non_space > 0.45:
            return True
        if punct / non_space > 0.35 and alpha < max(3, len(tk)):
            return True
        if re.search(r'(.)\1{4,}', t):
            return True
        if tk and any(len(x) >= 20 for x in tk):
            return True
        if tk and len(set(tk)) <= len(tk) * 0.35 and len(tk) > 3:
            return True
        if not tk:
            return True
        if not allow_short and len(tk) < 2 and not (self._ig(t) or self._ir(t) or self._iq(t)):
            return True
        return False
    def _should_log_client_err(self, ex: Exception) -> bool:
        msg = str(ex or "").lower()
        noisy = (
            "local variable 'e' referenced before assignment",
            "local variable e referenced before assignment",
            "invalid reaction provided",
            "only emoji are allowed",
        )
        return not any(x in msg for x in noisy)
    def _jnk(self, t: str, tk: Tuple[str, ...]) -> bool:
        if self._is_bad_text(t, tk, allow_short=True): return True
        if len(tk) < 2 and not (self._ig(t) or self._ir(t) or self._iq(t) or len(t) < 5): return True
        if t.isupper() and len(t) > 12: return True
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

    def _safe_int(self, value: Any, default: int) -> int:
        try:
            return int(value)
        except Exception:
            try:
                return int(float(value))
            except Exception:
                return default

    def _safe_float(self, value: Any, default: float) -> float:
        try:
            return float(value)
        except Exception:
            return default

    def _normalize_chat_state(self, st: CSt) -> None:
        st.lim = max(0, min(self._safe_int(st.lim, self.config["d_lim"]), 5000000))
        st.min_m = max(0, min(self._safe_int(st.min_m, self.config["d_min"]), 500))
        st.r_ch = max(0, min(self._safe_int(st.r_ch, self.config["d_ch"]), 100))
        st.m_ch = max(0, min(self._safe_int(st.m_ch, self.config["d_mch"]), 100))
        st.my_ch = max(0, min(self._safe_int(st.my_ch, self.config["d_mych"]), 100))
        st.cd_m = max(0, min(self._safe_int(st.cd_m, self.config["d_cdm"]), 120))
        st.cd_x = max(0, min(self._safe_int(st.cd_x, self.config["d_cdx"]), 240))
        if st.cd_x < st.cd_m:
            st.cd_x = st.cd_m
        st.last_mid = max(0, self._safe_int(st.last_mid, 0))
        st.parsed_cnt = max(0, self._safe_int(st.parsed_cnt, 0))
        st.w_cnt = max(0, self._safe_int(st.w_cnt, 0))
        st.cd_u = self._safe_float(st.cd_u, 0.0)
        st.mute_u = self._safe_float(st.mute_u, 0.0)
        st.auto_off_u = self._safe_float(st.auto_off_u, 0.0)
        st.last_usr = self._safe_int(st.last_usr, 0)
        st.last_t = self._safe_float(st.last_t, 0.0)
        st.usr_cd = {self._safe_int(uid, 0): self._safe_float(ts, 0.0) for uid, ts in dict(st.usr_cd).items() if self._safe_int(uid, 0)}
        st.ign = {self._safe_int(uid, 0) for uid in set(st.ign) if self._safe_int(uid, 0)}

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

    def _build_private_keycard_payload(self) -> str:
        if not self._ensure_kp() or not self._kp_priv:
            raise RuntimeError("crypto unavailable")
        payload = {
            "v": 2,
            "uid": int(self._my_id),
            "pub": self._kp_pub,
            "priv": self._kp_priv,
            "fp": self._key_fingerprint(self._kp_pub),
            "ts": int(time.time()),
        }
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        return "GPK2_PRIV_" + self._b64e(raw)

    def _parse_private_keycard_payload(self, payload: str) -> bool:
        try:
            if not isinstance(payload, str) or not payload.startswith("GPK2_PRIV_"):
                return False
            raw = self._b64d(payload[10:], 8192)
            data = json.loads(raw.decode("utf-8"))
            if not isinstance(data, dict):
                return False
            pub = data.get("pub", "")
            priv = data.get("priv", "")
            if pub and priv:
                self.set("gpb2_priv", priv)
                self.set("gpb2_pub", pub)
                self._kp_priv = priv
                self._kp_pub = pub
                return True
            return False
        except Exception:
            return False

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
            token_re = re.compile(r"^.{1,65536}$", re.I | re.DOTALL)

            def _validate_tfq(tfq: Any) -> bool:
                if not isinstance(tfq, dict) or len(tfq) > self._max_chat_tokens:
                    return False
                for tk, cnt in tfq.items():
                    if not isinstance(tk, str) or not token_re.fullmatch(tk):
                        print(f"[VLD_BKP] INVALID TFQ TOKEN: {tk}")
                        if getattr(self, "_c", None): self._c.loop.create_task(self._log(f"[VLD_BKP] INVALID TFQ TOKEN: {tk}", cat="err"))
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
                        print(f"[VLD_BKP] INVALID MKV PREF: {pref} FOR DEPTH {depth}")
                        if getattr(self, "_c", None): self._c.loop.create_task(self._log(f"[VLD_BKP] INVALID MKV PREF: {pref} FOR DEPTH {depth}", cat="err"))
                        return -1
                    if not isinstance(nxts, dict):
                        return -1
                    for nxt, cnt in nxts.items():
                        if not isinstance(nxt, str) or not token_re.fullmatch(nxt):
                            print(f"[VLD_BKP] INVALID MKV NXT TOKEN: {nxt}")
                            if getattr(self, "_c", None): self._c.loop.create_task(self._log(f"[VLD_BKP] INVALID MKV NXT TOKEN: {nxt}", cat="err"))
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
            self._sql("BEGIN")
            for cid_s, dat in d.items():
                cid = int(cid_s)
                self._sql("INSERT OR REPLACE INTO chats (cid, parsed_cnt, last_mid) VALUES (?, ?, ?)", (cid, dat.get("parsed_cnt", 0), dat.get("last_mid", 0)), commit=False)
                for tk, c in dat.get("tfq", {}).items():
                    self._sql("INSERT OR REPLACE INTO tokens (cid, tk, cnt) VALUES (?, ?, ?)", (cid, tk, c), commit=False)
                for k, v in dat.get("mkv", {}).items():
                    for nxt, c in v.items(): self._sql("INSERT OR REPLACE INTO markov (cid, d, pref, nxt, cnt) VALUES (?, 2, ?, ?, ?)", (cid, k, nxt, c), commit=False)
                for k, v in dat.get("mkv3", {}).items():
                    for nxt, c in v.items(): self._sql("INSERT OR REPLACE INTO markov (cid, d, pref, nxt, cnt) VALUES (?, 3, ?, ?, ?)", (cid, k, nxt, c), commit=False)
                for k, v in dat.get("mkv4", {}).items():
                    for nxt, c in v.items(): self._sql("INSERT OR REPLACE INTO markov (cid, d, pref, nxt, cnt) VALUES (?, 4, ?, ?, ?)", (cid, k, nxt, c), commit=False)
            self._sql("COMMIT")
            os.rename(self._df, self._df + ".bak")
        except Exception as e:
            try: self._sql("ROLLBACK")
            except Exception: pass
            if self._c: self._c.loop.create_task(self._log(f"<b>[MIGRATE ERR]</b> <code>{e}</code>"))

    def _sv_br(self):
        try:
            self._sql("BEGIN")
            for cid, st in self._chs.items():
                if not st.on and not st.msgs and not st.parsed_cnt: continue
                self._sql("INSERT OR REPLACE INTO chats (cid, on_off, lim, min_m, r_ch, m_ch, my_ch, cd_m, cd_x, bp_on, bp_int, react_ch, last_mid, parsed_cnt, w_cnt, cd_u, mute_u, auto_off_u, last_usr, last_tone, last_t) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 
                          (cid, int(st.on), st.lim, st.min_m, st.r_ch, st.m_ch, st.my_ch, st.cd_m, st.cd_x, int(self.config["bp_on"]), int(self.config["bp_int"]), int(self.config["react_ch"]), st.last_mid, st.parsed_cnt, st.w_cnt, st.cd_u, st.mute_u, st.auto_off_u, st.last_usr, st.last_tone, st.last_t), commit=False)
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
                st.cd_u = float(r[15] or 0.0)
                st.mute_u = float(r[16] or 0.0)
                st.auto_off_u = float(r[17] or 0.0)
                st.last_usr = r[18] or 0
                st.last_tone = r[19] or "нейтрал"
                st.last_t = r[20] or 0.0

                                    
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
        self._sv_task = self._c.loop.create_task(self._sv_loop())

    async def _stop_bg_tasks(self):
        if self._stop_event:
            self._stop_event.set()
        tasks = [t for t in (self._sv_task,) if t and not t.done()]
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except BaseException:
                pass
        self._sv_task = None
        self._stop_event = None

    async def _send_payload_file(self, dest: int, fname: str, caption: str) -> int:
        last_err = None
        targets = []
        if dest is not None:
            targets.append(dest)
        if self._my_id and self._my_id not in targets:
            targets.append(self._my_id)
        if "me" not in targets:
            targets.append("me")
        for target in targets:
            try:
                msg = await self._c.send_file(target, fname, caption=caption)
                if target != dest:
                    self._log_ch = self._my_id if target in {self._my_id, "me"} else self._log_ch
                    self.set("log_ch", self._my_id if target in {self._my_id, "me"} else self._log_ch)
                return getattr(msg, "id", 0) or 0
            except Exception as e:
                last_err = e
                try:
                    ent = await self._c.get_entity(target)
                    msg = await self._c.send_file(ent, fname, caption=caption)
                    if target != dest:
                        self._log_ch = self._my_id if target in {self._my_id, "me"} else self._log_ch
                        self.set("log_ch", self._my_id if target in {self._my_id, "me"} else self._log_ch)
                    return getattr(msg, "id", 0) or 0
                except Exception as inner:
                    last_err = inner
                    continue
        raise last_err or RuntimeError("send_file failed")

    async def _sv_loop(self):
        while True:
            if await self._wait_or_stop(120):
                return
            try:
                await self._apply_chat_timeouts()
                await asyncio.get_event_loop().run_in_executor(None, self._sv_br)
            except asyncio.CancelledError:
                return
            except Exception as e:
                if self._c:
                    self._c.loop.create_task(self._log(f"<b>[SV LOOP ERR]</b> <code>{e}</code>", cat="err"))

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
        if self._log_ch:
            return self._log_ch
        self._log_ch = self.get("log_ch", 0)
        if self._log_ch:
            return self._log_ch

        try:
            async for dialog in self._c.iter_dialogs():
                if dialog.is_channel and (dialog.title or "").lower() == "heroku-logs":
                    self._log_ch = dialog.id
                    self.set("log_ch", self._log_ch)
                    break
        except Exception:
            pass

        if not self._log_ch:
            self._log_ch = self._my_id
            return self._my_id

        try:
            if not self.get("log_topic_id", 0) and hasattr(utils, "asset_forum_topic"):
                topic = await utils.asset_forum_topic(
                    self._c,
                    self._log_ch,
                    "GoyPulse",
                    "Логи GoyPulse"
                )
                topic_id = int(getattr(topic, "id", 0) or 0)
                if topic_id:
                    self.set("log_topic_id", topic_id)
        except Exception:
            pass

        return self._log_ch

    async def _log(self, text: str, cat: str = "err"):

        cmap = {"err": "log_err", "stl": "log_stl", "lrn": "log_lrn", "ans": "log_ans"}
        if not self.config[cmap.get(cat, "log_err")]: return
        
        icons = {"err": "<tg-emoji emoji-id=5253864872780769235>❗️</tg-emoji>", "stl": "<tg-emoji emoji-id=5255813619702049821>✅</tg-emoji>", "lrn": "<tg-emoji emoji-id=5256250435055920155>1️⃣</tg-emoji>", "ans": "<tg-emoji emoji-id=5253590213917158323>💬</tg-emoji>"}
        labels = {"err": "ERROR", "stl": "STEALTH", "lrn": "SYSTEM", "ans": "ANSWER"}
        icon = icons.get(cat, "<tg-emoji emoji-id=5256230583717079814>📝</tg-emoji>")
        label = labels.get(cat, "LOG")
        ts = f"<code>[{time.strftime('%H:%M:%S')}]</code>"
        
        formatted = f"{ts} {icon} <b>[{label}]</b>\n{text}"
        
        try:
            l_ch = await self._get_log()
            if cat == "lrn" and getattr(self, "_last_lrn_log_mid", 0) and getattr(self, "_last_lrn_log_cid", 0) == l_ch:
                try:
                    await self._c.edit_message(l_ch, self._last_lrn_log_mid, formatted)
                    return
                except Exception:
                    self._last_lrn_log_mid = 0
            
            try:
                msg = await self._c.send_message(l_ch, formatted, reply_to=int(self.get("log_topic_id", 0) or None))
            except ValueError:
                ent = await self._c.get_entity(l_ch)
                msg = await self._c.send_message(ent, formatted, reply_to=int(self.get("log_topic_id", 0) or None))
            
            if cat == "lrn":
                self._last_lrn_log_mid = msg.id
                self._last_lrn_log_cid = l_ch
        except Exception as e:
            if cat == "err": print(f"FAILED TO LOG: {e}\nORIGINAL TEXT: {text}")




    def _add(self, st: CSt, m: Message, commit: bool = True):
        try:
            t = (m.raw_text or "").strip()
            hm = bool(getattr(m, "media", None))
            tk = self._tks(t)
            if self._is_bad_text(t, tk, allow_short=False) and not hm: return
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
                raw = (m.raw_text or "").strip()
                tk = self._tks(raw)
                if ((raw and not self._is_bad_text(raw, tk, allow_short=False)) or getattr(m, "media", None)):
                    self._add(st, m, commit=False)
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
                if self._is_bad_text(m.txt, m.tks, allow_short=False): continue
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

    def _pick_smart_reply(self, st: CSt, src_text: str, ctx_tks: Tuple[str, ...], tme: bool) -> str:
        try:
            cands = self._cnd(st, ctx_tks, tme)
            if not cands:
                return ""
            src_tks = tuple(ctx_tks[-10:]) if ctx_tks else self._tks(src_text)
            src_is_q = self._iq(src_text)
            src_is_g = self._ig(src_text)
            src_is_r = self._ir(src_text)
            best_txt, best_sc = "", 0.0
            for txt in cands[:12]:
                tk = self._tks(txt)
                if self._is_bad_text(txt, tk, allow_short=False):
                    continue
                sc = self._sim(st, src_tks, tk)
                if src_is_g and self._ig(txt):
                    sc += 0.45
                if src_is_r and self._ir(txt):
                    sc += 0.25
                if src_is_q and not self._iq(txt):
                    sc += 0.2
                if 2 <= len(tk) <= 16:
                    sc += 0.15
                if len(txt) > 220:
                    sc -= 0.35
                if txt.lower() in st.my_outs:
                    sc -= 0.5
                if sc > best_sc:
                    best_txt, best_sc = txt, sc
            return best_txt if best_sc >= 0.42 else ""
        except Exception as e:
            if self._c: self._c.loop.create_task(self._log(f"<b>[SMART REPLY ERR]</b> <code>{e}</code>"))
            return ""

    def _pick_dialogue_reply(self, st: CSt, src_text: str, src_tks: Tuple[str, ...], sender_id: int = 0) -> str:
        try:
            if not src_tks or len(st.msgs) < 8:
                return ""
            best_txt, best_sc = "", 0.0
            msgs = list(st.msgs)[-2500:]
            src_is_q = self._iq(src_text)
            src_is_g = self._ig(src_text)
            src_is_r = self._ir(src_text)
            for i, cur in enumerate(msgs[:-1]):
                if not cur.tks or self._is_bad_text(cur.txt, cur.tks, allow_short=False):
                    continue
                sc = self._sim(st, src_tks[-10:], cur.tks[-10:])
                if sc < 0.18:
                    continue
                if src_is_q and self._iq(cur.txt):
                    sc += 0.12
                if src_is_g and self._ig(cur.txt):
                    sc += 0.18
                if src_is_r and self._ir(cur.txt):
                    sc += 0.1
                reply = None
                for nxt in msgs[i + 1:i + 5]:
                    if not nxt.tks or self._is_bad_text(nxt.txt, nxt.tks, allow_short=False):
                        continue
                    if nxt.sender_id == cur.sender_id:
                        continue
                    reply = nxt
                    break
                if not reply:
                    continue
                if sender_id and reply.sender_id == sender_id:
                    sc += 0.08
                if self._iq(reply.txt):
                    sc -= 0.12
                if len(reply.tks) < 1 or len(reply.tks) > 18:
                    sc -= 0.15
                if len(reply.txt) > 240:
                    sc -= 0.25
                if reply.txt.lower() in st.my_outs:
                    sc -= 0.5
                sc += max(0.0, (i / max(1, len(msgs))) * 0.12)
                if sc > best_sc:
                    best_txt, best_sc = reply.txt, sc
            return best_txt if best_sc >= 0.48 else ""
        except Exception as e:
            if self._c: self._c.loop.create_task(self._log(f"<b>[DIALOGUE REPLY ERR]</b> <code>{e}</code>"))
            return ""

    def _pick_author_style_reply(self, st: CSt, sender_id: int, src_text: str, src_tks: Tuple[str, ...]) -> str:
        try:
            if not sender_id or not src_tks:
                return ""
            best_txt, best_sc = "", 0.0
            samples = [m for m in list(st.msgs)[-1200:] if m.sender_id == sender_id and m.tks]
            if not samples:
                return ""
            src_is_q = self._iq(src_text)
            src_is_g = self._ig(src_text)
            src_is_r = self._ir(src_text)
            for m in samples[-120:]:
                if self._is_bad_text(m.txt, m.tks, allow_short=False):
                    continue
                sc = self._sim(st, src_tks[-10:], m.tks[-10:])
                if src_is_g and self._ig(m.txt):
                    sc += 0.2
                if src_is_r and self._ir(m.txt):
                    sc += 0.12
                if src_is_q and self._iq(m.txt):
                    sc -= 0.08
                if 1 <= len(m.tks) <= 14:
                    sc += 0.12
                if len(m.txt) > 180:
                    sc -= 0.2
                if m.txt.lower() in st.my_outs:
                    sc -= 0.5
                if sc > best_sc:
                    best_txt, best_sc = m.txt, sc
            return best_txt if best_sc >= 0.38 else ""
        except Exception as e:
            if self._c: self._c.loop.create_task(self._log(f"<b>[AUTHOR STYLE ERR]</b> <code>{e}</code>"))
            return ""

    def _dialogue_mode(self, text: str, tks: Tuple[str, ...]) -> str:
        if self._ig(text):
            return "greet"
        if self._iq(text):
            return "question"
        if self._ir(text):
            return "react"
        if len(tks) <= 2:
            return "short"
        return "chat"

    def _pick_mode_reply(self, st: CSt, src_text: str, src_tks: Tuple[str, ...], sender_id: int = 0) -> str:
        try:
            mode = self._dialogue_mode(src_text, src_tks)
            best_txt, best_sc = "", 0.0
            msgs = list(st.msgs)[-2200:]
            for i, cur in enumerate(msgs[:-1]):
                if not cur.tks or self._is_bad_text(cur.txt, cur.tks, allow_short=False):
                    continue
                if self._dialogue_mode(cur.txt, cur.tks) != mode:
                    continue
                sc = self._sim(st, src_tks[-10:], cur.tks[-10:]) if src_tks else 0.0
                if mode == "greet" and self._ig(cur.txt):
                    sc += 0.35
                elif mode == "question" and self._iq(cur.txt):
                    sc += 0.22
                elif mode == "react" and self._ir(cur.txt):
                    sc += 0.22
                elif mode == "short" and len(cur.tks) <= 2:
                    sc += 0.16
                reply = None
                for nxt in msgs[i + 1:i + 4]:
                    if not nxt.tks or self._is_bad_text(nxt.txt, nxt.tks, allow_short=False):
                        continue
                    if nxt.sender_id == cur.sender_id:
                        continue
                    if self._dialogue_mode(nxt.txt, nxt.tks) == mode and mode != "question":
                        sc += 0.08
                    reply = nxt
                    break
                if not reply:
                    continue
                if sender_id and reply.sender_id == sender_id:
                    sc += 0.06
                if len(reply.txt) > 180:
                    sc -= 0.2
                if reply.txt.lower() in st.my_outs:
                    sc -= 0.5
                if sc > best_sc:
                    best_txt, best_sc = reply.txt, sc
            threshold = {"greet": 0.32, "question": 0.38, "react": 0.34, "short": 0.28, "chat": 0.42}.get(mode, 0.4)
            return best_txt if best_sc >= threshold else ""
        except Exception as e:
            if self._c: self._c.loop.create_task(self._log(f"<b>[MODE REPLY ERR]</b> <code>{e}</code>"))
            return ""

    def _compose_hybrid_reply(self, st: CSt, src_text: str, base: str, style: str, generated: str) -> str:
        try:
            parts = []
            for txt in (base, style, generated):
                if not txt:
                    continue
                txt = txt.strip()
                if not txt or self._is_bad_text(txt, self._tks(txt), allow_short=False):
                    continue
                if txt.lower() in {x.lower() for x in parts}:
                    continue
                parts.append(txt)
            if not parts:
                return ""
            src_is_q = self._iq(src_text)
            if src_is_q:
                for txt in parts:
                    if not self._iq(txt):
                        return txt
            if len(parts) >= 2:
                a = parts[0]
                b = parts[1]
                a_tk = self._tks(a)
                b_tk = self._tks(b)
                if a_tk and b_tk and len(a_tk) <= 8 and len(b_tk) <= 8:
                    joined = f"{a.rstrip('.!?, ')} {b}".strip()
                    if not self._is_bad_text(joined, self._tks(joined), allow_short=False) and len(joined) <= 220:
                        return joined
            return parts[0]
        except Exception as e:
            if self._c: self._c.loop.create_task(self._log(f"<b>[HYBRID REPLY ERR]</b> <code>{e}</code>"))
            return base or style or generated or ""

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
            if self._is_bad_text(r, self._tks(r), allow_short=False): return ""
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
                if not self._is_bad_text(r, self._tks(r), allow_short=False):
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

    async def _apply_chat_timeouts(self):
        now = time.time()
        changed = False
        for cid, st in self._chs.items():
            if st.on and st.auto_off_u and float(now) >= float(st.auto_off_u):
                st.on = False
                st.auto_off_u = 0.0
                changed = True
                if self._c:
                    self._c.loop.create_task(self._log(
                        f"Автовыключение чата <code>{cid}</code> по таймеру.",
                        cat="lrn",
                    ))
        if changed:
            self._sv()
            try:
                await asyncio.get_event_loop().run_in_executor(None, self._sv_br)
            except Exception:
                pass

    @loader.unrestricted
    async def watcher(self, e: Message):
        try:
            if not getattr(e, 'message', None) or self._glob_stop or getattr(e, 'out', False): return
            await self._apply_chat_timeouts()
            if self._is_restricted_mode():
                await self._log_restricted_once()
                return
            await self._maybe_import_keycard(e)
            if getattr(e, 'is_private', False): return
            sender = getattr(e, 'sender', None)
            if getattr(sender, 'bot', False) or getattr(e, 'fwd_from', None): return
            st = self._chs[e.chat_id]
            sid = getattr(e, 'sender_id', None)
            if not st.on or st.lrn or time.time() < float(st.mute_u) or (sid in st.ign): return
            t = (e.raw_text or "").strip()
            tk = self._tks(t)
            hm = bool(getattr(e, "media", None))
            if self._is_bad_text(t, tk, allow_short=False) and not hm: return
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
                if (float(st.cd_m) > 0 and time.time() < float(st.cd_u)) or len(st.msgs) < int(st.min_m): return
                if sid and time.time() < st.usr_cd.get(sid, 0): return 
                if random.randint(1, 100) > int(ch): return

            if random.randint(1, 100) <= self.config["react_ch"]:
                try:
                    emo = random.choice(["👍", "😂", "🔥", "❤", "👌", "👏"])
                    await e.react(emo)
                    if random.random() < 0.7: return 
                except Exception as ex:
                    if self._c and self._should_log_client_err(ex):
                        self._c.loop.create_task(self._log(f"<b>[REACT ERR]</b> <code>{ex}</code>"))

            ctx_msgs = [m.tks for m in list(st.rec)[-4:] if m.tks]
            ctx_tks = tuple(w for msg in ctx_msgs for w in msg) + tk
            mode_ans = self._pick_mode_reply(st, t, ctx_tks, sid or 0)
            dlg_ans = self._pick_dialogue_reply(st, t, ctx_tks, sid or 0)
            style_ans = self._pick_author_style_reply(st, sid or 0, t, ctx_tks)
            gen_ans = self._gen(st, ctx_tks, tme)
            smart_ans = self._pick_smart_reply(st, t, ctx_tks, tme)
            ans = self._compose_hybrid_reply(st, t, mode_ans or dlg_ans or smart_ans, style_ans, gen_ans) or mode_ans or smart_ans or gen_ans or self._fb(st, t)
            ans = self._stl(ans, t)
            if self._is_bad_text(ans, self._tks(ans), allow_short=False):
                ans = self._fb(st, t)
            mid = self._md(st, t)
            r_del = len(t) * 0.03 if t else 0.5
            await asyncio.sleep(min(max(r_del, 0.5), 3.0))
            try: await e.client.send_read_acknowledge(e.chat_id, e)
            except Exception as ex:
                if self._c and self._should_log_client_err(ex):
                    self._c.loop.create_task(self._log(f"<b>[ACK ERR]</b> <code>{ex}</code>"))

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
                            if self._c and self._should_log_client_err(ex):
                                self._c.loop.create_task(self._log(f"<b>[ACTION ERR]</b> <code>{ex}</code>"))
                        if mm and (mm.media or getattr(mm, 'sticker', None)):
                            msg = await e.client.send_file(e.chat_id, mm, reply_to=e.id)
                            st.my_msgs.append(msg.id)
                            st.cd_u = time.time() + random.uniform(float(st.cd_m), float(st.cd_x))
                            if sid: st.usr_cd[sid] = time.time() + random.uniform(float(st.cd_m), float(st.cd_x)) * 2.0
                            if random.random() < 0.8: return 
                except Exception as ex:
                    if self._c: self._c.loop.create_task(self._log(f"<b>[MEDIA ANS ERR]</b> <code>{ex}</code>", cat="err"))


            if ans and len(ans) > 0:
                tdl = min(max(len(ans) * random.uniform(0.12, 0.22), 1.5), 15.0)
                try:
                    async with e.client.action(e.chat_id, 'typing'): await asyncio.sleep(tdl)
                except Exception as ex:
                    if self._c and self._should_log_client_err(ex):
                        self._c.loop.create_task(self._log(f"<b>[TYPING ERR]</b> <code>{ex}</code>"))
                try:
                    if random.random() < 0.03 and len(ans) > 10:
                        w_ans = ans[:-1] + random.choice(["ь", "ж", "ф", "а"])
                        typo_tail = ans.split()
                        msg = await e.reply(w_ans)
                        await asyncio.sleep(random.uniform(1.0, 2.0))
                        if typo_tail:
                            await e.reply(f"*{typo_tail[-1]}")
                    else: msg = await e.reply(ans)
                    st.my_msgs.append(msg.id)
                except Exception as ex:
                    if self._c: self._c.loop.create_task(self._log(f"<b>[TEXT ANS ERR]</b> <code>{ex}</code>", cat="err"))


            st.cd_u = time.time() + random.uniform(float(st.cd_m), float(st.cd_x))
            if sid: st.usr_cd[sid] = time.time() + random.uniform(float(st.cd_m), float(st.cd_x)) * 2.0
        except Exception as ex:
            if self._c: self._c.loop.create_task(self._log(f"<b>[WATCHER GLOBAL ERR]</b> <code>{ex}</code>"))
    @loader.command(ru_doc="<on/off> | Включить/выключить автоответчик")
    async def gpulsecmd(self, m: Message):
        try:
            if not m.is_group: return await self._ans(m, self.strings("og"))
            if self._is_restricted_mode():
                return await self._ans(m, self.strings("upd_lock").format(reason=self._restricted_reason()))
            raw = utils.get_args_raw(m).strip().lower()
            if not raw:
                return await self._ans(m, self.strings("h_pulse"))
            parts = raw.split()
            if parts[0] not in ["on", "off"]:
                return await self._ans(m, self.strings("h_pulse"))
            st = self._chs[m.chat_id]
            ttl = 0
            if parts[0] == "on" and len(parts) > 1:
                try:
                    ttl = self._parse_duration_seconds(parts[1])
                except Exception:
                    return await self._ans(m, "❌ Неверный формат времени. Примеры: <code>30</code>, <code>45m</code>, <code>2h</code>, <code>1d</code>.")
            st.on = (parts[0] == "on")
            st.auto_off_u = (time.time() + ttl) if (st.on and ttl > 0) else 0.0
            self._glob_stop = False
            self._sv()
            timer_note = ""
            if st.on and ttl > 0:
                mins = max(1, int(math.ceil(ttl / 60)))
                timer_note = f"\n⏱️ Автовыключение через <code>{mins}</code> мин."
            t = self.strings("on").format(timer_note) if st.on else self.strings("off")
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
                try:
                    top_u_id = u_act.most_common(1)[0][0]
                    active_users = [u for u in u_act.keys() if u]
                    if active_users: dushnila_id = random.choice(active_users)
                except Exception: pass

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
            try: v = int(raw_v)
            except: v = raw_v
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
                                "react": ("react_ch", 0, 100, False),
                "logerr": ("log_err", 0, 1, True),
                "logstl": ("log_stl", 0, 1, True),
                "logbkp": ("log_bkp", 0, 1, True),
                "loglrn": ("log_lrn", 0, 1, True),
                "logans": ("log_ans", 0, 1, True),
                "updint": ("upd_int", 0, 720, False),
                "pub": ("upd_pubkey", None, None, False),
            }
            if p in glob_map:
                key, mn, mx, as_bool = glob_map[p]
                if mn is not None and mx is not None:
                    if not isinstance(v, int):
                        return await self._ans(m, "❌ Для этого параметра значение должно быть целым числом.")
                    val = max(mn, min(v, mx))
                else: val = v
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
            allowed = {"gpstat", "gpinfo", "gpulse", "gpset", "gpmute", "gpignore", "gpref"}
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
                    anchor = await client.send_message(me_id or "me", "📡 GoyPulse установлен")
                    await self.inline.form(
                        text="<b>GoyPulse</b>",
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
            text = "<b>GoyPulse</b>"
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
            self._init_db()
            self._migrate()
            self._ld()
            self._tamper_mode = bool(self.get("gp_tamper_mode", False))
            await self._start_bg_tasks()
            await self._log("GoyPulse by goy(@samsepi0l_ovf) запущен", cat="lrn")
        except Exception as e:
            if self._c:
                self._c.loop.create_task(self._log(f"<b>[CRITICAL START ERR]</b> <code>{e}</code>", cat="err"))

    async def on_unload(self):
        try:
            if None and not None.done():
                None.cancel()
                try:
                    await None
                except BaseException:
                    pass
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
