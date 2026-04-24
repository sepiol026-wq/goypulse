# ====================================================================================================================
#   ██████╗  ██████╗ ██╗   ██╗███╗   ███╗ ██████╗ ██████╗ ██╗   ██╗██╗     ███████╗███████╗
#  ██╔════╝ ██╔═══██╗╚██╗ ██╔╝████╗ ████║██╔═══██╗██╔══██╗██║   ██║██║     ██╔════╝██╔════╝
#  ██║  ███╗██║   ██║ ╚████╔╝ ██╔████╔██║██║   ██║██║  ██║██║   ██║██║     █████╗  ███████╗
#  ██║   ██║██║   ██║  ╚██╔╝  ██║╚██╔╝██║██║   ██║██║  ██║██║   ██║██║     ██╔══╝  ╚════██║
#  ╚██████╔╝╚██████╔╝   ██║   ██║ ╚═╝ ██║╚██████╔╝██████╔╝╚██████╔╝███████╗███████╗███████║
#   ╚═════╝  ╚═════╝    ╚═╝   ╚═╝     ╚═╝ ╚═════╝ ╚═════╝  ╚═════╝ ╚══════╝╚══════╝╚══════╝
#
#   OFFICIAL USERNAMES: @goymodules | @samsepi0l_ovf
#   MODULE: doom
#
#   THIS MODULE IS LICENSED UNDER GNU AGPLv3, PROTECTED AGAINST UNAUTHORIZED COPYING/RESALE,
#   AND ITS ORIGINAL AUTHORSHIP BELONGS TO @samsepi0l_ovf.
#   ALL OFFICIAL UPDATES, RELEASE NOTES, AND PATCHES ARE PUBLISHED IN THE TELEGRAM CHANNEL @goymodules.
# ====================================================================================================================

# requires: herokutl
# meta developer: @GoyModules
# authors: @goymodules
# Description: Inline DOOM mini-game module.
# meta banner: https://raw.githubusercontent.com/sepiol026-wq/goypulse/main/assets/doom.png

"""Huge inline DOOM module with settings, profiles, stats, waves and upgrades."""

__version__ = (1, 3, 1)

import asyncio
import math
import random
import re
import time
import traceback
from herokutl.types import Message
from .. import loader, utils


@loader.tds
class Doom(loader.Module):
    """Inline DOOM mini-game."""

    strings = {
        "name": "Doom",
        "_cls_doc": "Inline DOOM mini-game.",
        "e_title": "<tg-emoji emoji-id=5256054975389247793>📛</tg-emoji>",
        "e_hp": "<tg-emoji emoji-id=5253549669425882943>🔋</tg-emoji>",
        "e_ammo": "<tg-emoji emoji-id=5256094480498436162>📦</tg-emoji>",
        "e_kills": "<tg-emoji emoji-id=5256054975389247793>📛</tg-emoji>",
        "e_map": "<tg-emoji emoji-id=5253713110111365241>📍</tg-emoji>",
        "e_action": "<tg-emoji emoji-id=5256079005731271025>📟</tg-emoji>",
        "e_log": "<tg-emoji emoji-id=5253590213917158323>💬</tg-emoji>",
        "e_fire": "<tg-emoji emoji-id=5253877736207821121>🔥</tg-emoji>",
        "e_dead": "<tg-emoji emoji-id=5237682408163110073>💀</tg-emoji>",
    }
    strings_ru = {"_cls_doc": "Огромный инлайн DOOM-модуль с меню, режимами и статой."}
    strings_uk = {"_cls_doc": "Великий inline DOOM-комбайн з меню, режимами та статистикою."}
    strings_de = {"_cls_doc": "Großer Inline-DOOM-Kombinator mit Menü, Modi und Statistik."}
    strings_jp = {"_cls_doc": "メニュー・モード・統計付きの大型インラインDOOMモジュール。"}
    strings_neofit = {"_cls_doc": "Большой дум-модуль: меню, стата, режимы, всё серьёзно."}
    strings_tiktok = {"_cls_doc": "DOOM huge edition: меню, режимы, стата, полный вайб."}
    strings_leet = {"_cls_doc": "Hug3 DOOM c0mb1n3: m3nu, m0d3z, st4tz."}
    strings_uwu = {"_cls_doc": "Big DOOM combo wif menu, modes, and stats uwu."}
    strings_ru = {**strings, **locals().get("strings_ru", {})}
    strings_uk = {**strings, **locals().get("strings_uk", {})}
    strings_de = {**strings, **locals().get("strings_de", {})}
    strings_jp = {**strings, **locals().get("strings_jp", {})}
    strings_neofit = {**strings, **locals().get("strings_neofit", {})}
    strings_tiktok = {**strings, **locals().get("strings_tiktok", {})}
    strings_leet = {**strings, **locals().get("strings_leet", {})}
    strings_uwu = {**strings, **locals().get("strings_uwu", {})}

    def __init__(self):
        self.sessions = {}
        self.game_config = {
            "scr_w": 28,
            "scr_h": 13,
            "fov": math.pi / 3,
            "depth": 11.0,
            "shades": [" ", "░", "▒", "▓", "█"],
        }
        self.base_map = [
            "####################",
            "#....A.............#",
            "#.####.E..####..R..#",
            "#.#..#....#..#.....#",
            "#.#..######..#..A..#",
            "#........E...H.....#",
            "######.........###.#",
            "#...A.....D........#",
            "#.######...#...S...#",
            "#......#...#...H...#",
            "#.####.#.E.#...A...#",
            "#..H.........R.....#",
            "####################",
        ]
        self.map_h = len(self.base_map)
        self.map_w = len(self.base_map[0])
        self.difficulty = {
            "easy": {"hp": 120, "ammo": 20, "enemy_hp_mul": 0.85, "enemy_dmg_mul": 0.65, "enemy_ai": 0.95},
            "normal": {"hp": 100, "ammo": 14, "enemy_hp_mul": 1.0, "enemy_dmg_mul": 1.0, "enemy_ai": 0.75},
            "hard": {"hp": 90, "ammo": 12, "enemy_hp_mul": 1.35, "enemy_dmg_mul": 1.25, "enemy_ai": 0.60},
            "nightmare": {"hp": 85, "ammo": 10, "enemy_hp_mul": 1.6, "enemy_dmg_mul": 1.55, "enemy_ai": 0.48},
        }
        self.locale = self._build_locale()

    def _build_locale(self):
        return {
            "ru": {
                "menu_title": "{t} <b>DOOM</b>\n\nВыбери действие:",
                "new_game": "🟢 Новая игра",
                "continue": "🟡 Продолжить",
                "records": "🏆 Профиль/Рекорды",
                "settings": "⚙️ Настройки",
                "help": "📖 Помощь",
                "back": "↩️ Назад",
                "exit": "🚪 Выход",
                "difficulty": "Сложность",
                "view": "Вид",
                "lang": "Язык",
                "autosave": "Автосейв",
                "started": "Добро пожаловать в Ад.",
                "loaded": "Сохранение загружено.",
                "no_save": "Нет сохранений!",
                "saved": "Сохранено.",
                "autosaved": "Автосохранение выполнено.",
                "wall": "Упёрлись в стену.",
                "no_ammo": "Клик! Пусто.",
                "shot_wall": "Выстрел в стену.",
                "hit": "{f} Цель разорвана!",
                "med_used": "Использовал аптечку.",
                "med_none": "Аптечек нет.",
                "reload_done": "Перезарядка: +{n}.",
                "reload_none": "Нет патронов в запасе.",
                "pickup_ammo": "Патроны +6.",
                "pickup_hp": "Лечение +20 HP.",
                "pickup_armor": "Броня +15.",
                "pickup_secret": "Секретка! Лут поднят.",
                "enemy_hit": "Мelee-удар прошёл.",
                "enemy_miss": "Вблизи никого.",
                "enemy_attack": "Монстр атакует! -{n} HP",
                "dead": "{d} <b>ВЫ ПОГИБЛИ</b>\n\nСчёт: {s}\n\nНажми Новая игра.",
                "hud_deadline": "Убито: <b>{k}</b> | Точность: <b>{a}%</b> | Броня: <b>{ar}</b>",
                "help_text": "<b>DOOM</b>\n\nP — ты\nM/👾 — монстры\nA — патроны\nH — хилка\nR — броня\nS — секретка\nD — жирный демон\nB — босс\n\nКоманда запуска: <code>{pref}doom</code>",
                "profile": "<b>Профиль</b>\nИгр: <b>{gp}</b>\nКиллов: <b>{tk}</b>\nСмертей: <b>{td}</b>\nЛучший счёт: <b>{bs}</b>\nВыстрелов: <b>{sh}</b>\nПопаданий: <b>{hh}</b>\nТочность: <b>{acc}%</b>\nDaily best: <b>{db}</b>\nАчивок: <b>{ac}</b>",
                "in_game": "🕹️ В игру",
                "difficulty_pick": "Выбери сложность:",
                "set_ok": "Настройки обновлены.",
                "game_closed": "Игра завершена.",
                "shop": "🛒 Магазин",
                "daily_run": "☣️ Daily Run",
                "credits": "Кредиты",
                "wave": "Волна",
                "shop_title": "<b>Магазин апгрейдов</b>\nКредиты: <b>{c}</b>",
                "upgrade_damage": "Урон +{v}%",
                "upgrade_armor": "Броня +{v}",
                "upgrade_med": "+1 медкит в старт",
                "upgrade_mag": "Магазин +{v}",
                "buy_ok": "Куплено: {n}",
                "buy_no": "Не хватает кредитов.",
                "buy_max": "Апгрейд уже в максимуме.",
                "wave_start": "Старт волны {w}.",
                "wave_done": "Волна {w} очищена.",
                "boss_wave": "Босс-волна {w}!",
                "daily_started": "Daily Run стартовал.",
                "daily_done": "Daily Run завершен: {s}",
                "daily_cooldown": "Daily Run уже сыгран сегодня.",
                "ach_title": "<b>Ачивки</b>",
                "ach_unlock": "Открыта ачивка: {n}",
                "profile_daily": "Daily best: <b>{db}</b>",
                "profile_ach": "Ачивок: <b>{ac}</b>",
            },
            "en": {
                "menu_title": "{t} <b>DOOM</b>\n\nChoose action:",
                "new_game": "🟢 New game",
                "continue": "🟡 Continue",
                "records": "🏆 Profile/Records",
                "settings": "⚙️ Settings",
                "help": "📖 Help",
                "back": "↩️ Back",
                "exit": "🚪 Exit",
                "difficulty": "Difficulty",
                "view": "View",
                "lang": "Language",
                "autosave": "Autosave",
                "started": "Welcome to Hell.",
                "loaded": "Save loaded.",
                "no_save": "No save found!",
                "saved": "Saved.",
                "autosaved": "Autosave complete.",
                "wall": "Bumped into a wall.",
                "no_ammo": "Click! Empty.",
                "shot_wall": "Shot into the wall.",
                "hit": "{f} Target ripped apart!",
                "med_used": "Used medkit.",
                "med_none": "No medkits.",
                "reload_done": "Reloaded +{n}.",
                "reload_none": "No reserve ammo.",
                "pickup_ammo": "Ammo +6.",
                "pickup_hp": "Healed +20 HP.",
                "pickup_armor": "Armor +15.",
                "pickup_secret": "Secret found. Loot grabbed.",
                "enemy_hit": "Melee landed.",
                "enemy_miss": "No enemy in range.",
                "enemy_attack": "Monster attacks! -{n} HP",
                "dead": "{d} <b>YOU DIED</b>\n\nScore: {s}\n\nTap New game.",
                "hud_deadline": "Kills: <b>{k}</b> | Accuracy: <b>{a}%</b> | Armor: <b>{ar}</b>",
                "help_text": "<b>DOOM</b>\n\nP — you\nM/👾 — monsters\nA — ammo\nH — heal\nR — armor\nS — secret\nD — heavy demon\nB — boss\n\nStart cmd: <code>{pref}doom</code>",
                "profile": "<b>Profile</b>\nGames: <b>{gp}</b>\nKills: <b>{tk}</b>\nDeaths: <b>{td}</b>\nBest score: <b>{bs}</b>\nShots: <b>{sh}</b>\nHits: <b>{hh}</b>\nAccuracy: <b>{acc}%</b>\nDaily best: <b>{db}</b>\nAchievements: <b>{ac}</b>",
                "in_game": "🕹️ In-game",
                "difficulty_pick": "Pick difficulty:",
                "set_ok": "Settings updated.",
                "game_closed": "Game closed.",
                "shop": "🛒 Shop",
                "daily_run": "☣️ Daily Run",
                "credits": "Credits",
                "wave": "Wave",
                "shop_title": "<b>Upgrade shop</b>\nCredits: <b>{c}</b>",
                "upgrade_damage": "Damage +{v}%",
                "upgrade_armor": "Armor +{v}",
                "upgrade_med": "+1 medkit at start",
                "upgrade_mag": "Mag size +{v}",
                "buy_ok": "Bought: {n}",
                "buy_no": "Not enough credits.",
                "buy_max": "Upgrade already maxed.",
                "wave_start": "Wave {w} started.",
                "wave_done": "Wave {w} cleared.",
                "boss_wave": "Boss wave {w}!",
                "daily_started": "Daily Run started.",
                "daily_done": "Daily Run complete: {s}",
                "daily_cooldown": "Daily Run already played today.",
                "ach_title": "<b>Achievements</b>",
                "ach_unlock": "Achievement unlocked: {n}",
                "profile_daily": "Daily best: <b>{db}</b>",
                "profile_ach": "Achievements: <b>{ac}</b>",
            },
            "uk": {},
            "de": {},
            "jp": {},
            "neofit": {},
            "tiktok": {},
            "leet": {},
            "uwu": {},
        }

    async def client_ready(self, client, db):
        self.client = client
        self.db = db

    def _lang(self, st=None):
        if st and st.get("settings", {}).get("lang"):
            return st["settings"]["lang"]
        return self.db.get("Doom", "lang", "ru")

    def _tr(self, key, st=None, **fmt):
        lang = self._lang(st)
        base = self.locale.get("ru", {})
        loc = self.locale.get(lang, {})
        val = loc.get(key) or self.locale.get("en", {}).get(key) or base.get(key) or key
        return val.format(**fmt)

    def _settings(self):
        return self.db.get("Doom", "settings", {"difficulty": "normal", "view": "both", "autosave": 15, "lang": "ru"})

    def _save_settings(self, s):
        self.db.set("Doom", "settings", s)
        self.db.set("Doom", "lang", s.get("lang", "ru"))

    def _profile(self):
        return self.db.get(
            "Doom",
            "profile",
            {
                "games": 0,
                "total_kills": 0,
                "deaths": 0,
                "best_score": 0,
                "shots": 0,
                "hits": 0,
                "daily_best": 0,
                "daily_last": "",
                "achievements": [],
            },
        )

    def _save_profile(self, p):
        self.db.set("Doom", "profile", p)

    def _today_key(self):
        return time.strftime("%Y-%m-%d", time.localtime())

    def _unlock_achievement(self, st, code, title):
        if code in st["ach"]:
            return
        st["ach"].add(code)
        st["new_ach"] = title

    def _sync_achievements_to_profile(self, st):
        p = self._profile()
        old = set(p.get("achievements", []))
        old.update(st.get("ach", set()))
        p["achievements"] = sorted(old)
        self._save_profile(p)

    def _award_for_kill(self, st, etype):
        gain = 8 if etype == "imp" else 18
        st["credits"] += gain

    def _spawn_wave(self, st, wave):
        st["wave"] = wave
        count = 2 + wave
        boss = wave % 5 == 0
        if boss:
            count += 2
        enemy_pool = []
        for y in range(1, self.map_h - 1):
            for x in range(1, self.map_w - 1):
                if st["map"][y][x] == " ":
                    if abs(x - int(st["x"])) + abs(y - int(st["y"])) > 6:
                        enemy_pool.append((x, y))
        random.shuffle(enemy_pool)
        st["enemies"] = {}
        for i in range(min(count, len(enemy_pool))):
            x, y = enemy_pool[i]
            et = "demon" if boss and i % 3 == 0 else "imp"
            hp, _ = self._enemy_stats(st, et)
            if boss and i == 0:
                et = "boss"
                hp = int(130 * self.difficulty[st["difficulty"]]["enemy_hp_mul"])
            st["enemies"][f"{x}:{y}"] = {"x": x, "y": y, "type": et, "hp": hp}
        st["log"] = self._tr("boss_wave" if boss else "wave_start", st, w=wave)
        st["dirty"] = True

    def _snapshot_state(self, st):
        sv = st.copy()
        if isinstance(sv.get("ach"), set):
            sv["ach"] = sorted(sv["ach"])
        return sv

    async def safe_edit(self, call, text, reply_markup):
        try:
            await call.edit(text, reply_markup=reply_markup)
            return True
        except Exception as e:
            wait_s = getattr(e, "seconds", 0) or getattr(e, "value", 0)
            if not wait_s:
                m = re.search(r"(\d+)\s*seconds", str(e))
                if m:
                    wait_s = int(m.group(1))
            if wait_s:
                await asyncio.sleep(wait_s + 0.1)
                try:
                    await call.edit(text, reply_markup=reply_markup)
                    return True
                except Exception:
                    return False
            return False

    def _parse_map(self):
        m, enemies = [], {}
        for y, row in enumerate(self.base_map):
            r = []
            for x, c in enumerate(row):
                if c in ("E", "D"):
                    et = "imp" if c == "E" else "demon"
                    hp = 24 if et == "imp" else 44
                    enemies[f"{x}:{y}"] = {"x": x, "y": y, "type": et, "hp": hp}
                    r.append(" ")
                elif c == ".":
                    r.append(" ")
                else:
                    r.append(c)
            m.append(r)
        return m, enemies

    def _enemy_stats(self, st, etype):
        mul_hp = self.difficulty[st["difficulty"]]["enemy_hp_mul"]
        mul_dmg = self.difficulty[st["difficulty"]]["enemy_dmg_mul"]
        if etype == "imp":
            return int(24 * mul_hp), max(3, int(8 * mul_dmg))
        if etype == "demon":
            return int(44 * mul_hp), max(5, int(12 * mul_dmg))
        return int(130 * mul_hp), max(10, int(18 * mul_dmg))

    def _acc(self, st):
        return round((st["hits"] / st["shots"] * 100.0), 1) if st["shots"] else 0.0

    def _menu_buttons(self):
        return [
            [{"text": self._tr("new_game"), "callback": self.action_new_menu}],
            [{"text": self._tr("continue"), "callback": self.action_cont}],
            [{"text": self._tr("records"), "callback": self.action_records}, {"text": self._tr("settings"), "callback": self.action_settings}],
            [{"text": self._tr("daily_run"), "callback": self.action_daily_run}, {"text": self._tr("help"), "callback": self.action_help}],
        ]

    async def _show_menu(self, call_or_msg):
        text = self._tr("menu_title", t="")
        if hasattr(call_or_msg, "edit"):
            await self.safe_edit(call_or_msg, text, self._menu_buttons())
            return True

        form = await self.inline.form(text=text, message=call_or_msg, reply_markup=self._menu_buttons())
        return bool(form)

    @loader.command(
        ru_doc="Справка по игре DOOM",
        uk_doc="Довідка по грі DOOM",
        de_doc="Hilfe zum DOOM-Spiel",
        jp_doc="DOOMゲームのヘルプ",
        neofit_doc="Справка по doom",
        tiktok_doc="Гайд по doom-модулю",
        leet_doc="D00M h3lp",
        uwu_doc="DOOM hewp",
    )
    async def hdoomcmd(self, message: Message):
        pref = getattr(self, "get_prefix", lambda: ".")()
        await utils.answer(message, self._tr("help_text", pref=pref))

    @loader.command(
        ru_doc="Открыть меню DOOM",
        uk_doc="Відкрити меню DOOM",
        de_doc="DOOM-Menü öffnen",
        jp_doc="DOOMメニューを開く",
        neofit_doc="Открыть doom-меню",
        tiktok_doc="Открыть doom hub",
        leet_doc="0p3n d00m m3nu",
        uwu_doc="open doom menuu",
    )
    async def doomcmd(self, message: Message):
        try:
            ok = await self._show_menu(message)
            if ok:
                return
            await utils.answer(
                message,
                "<b>DOOM</b>\n"
                "Inline-меню не открылось (пустой ответ от inline).\n"
                "Попробуй в обычном чате (не Избранное) или проверь inline-бота.\n"
                "Для справки: <code>.hdoom</code>",
            )
        except Exception as e:
            err = utils.escape_html(str(e))[:220]
            await utils.answer(
                message,
                "<b>DOOM</b>\n"
                "Не удалось открыть inline-меню.\n"
                f"Ошибка: <code>{err}</code>\n\n"
                "Проверь: inline-бот включён и не заблокирован.\n"
                "Для справки: <code>.hdoom</code>",
            )

    def render_3d_frame(self, st):
        w = self.game_config["scr_w"]
        h = self.game_config["scr_h"]
        fov = self.game_config["fov"]
        depth = self.game_config["depth"]
        shades = self.game_config["shades"]
        px, py, pa = st["x"], st["y"], st["a"]
        enemy_pos = {(e["x"], e["y"]): e for e in st["enemies"].values()}
        screen = []

        for x in range(w):
            ray_a = (pa - fov / 2.0) + (x / float(w)) * fov
            dist_w = 0.0
            hit_w = False
            cell_hit = " "
            eye_x = math.sin(ray_a)
            eye_y = math.cos(ray_a)

            while not hit_w and dist_w < depth:
                dist_w += 0.08
                tx = int(px + eye_x * dist_w)
                ty = int(py + eye_y * dist_w)
                if tx < 0 or tx >= self.map_w or ty < 0 or ty >= self.map_h:
                    hit_w = True
                    dist_w = depth
                else:
                    if (tx, ty) in enemy_pos:
                        hit_w = True
                        et = enemy_pos[(tx, ty)]["type"]
                        cell_hit = "B" if et == "boss" else ("D" if et == "demon" else "E")
                    else:
                        c = st["map"][ty][tx]
                        if c in ("#", "A", "H", "R", "S"):
                            hit_w = True
                            cell_hit = c

            ceil = float(h / 2.0) - h / max(dist_w, 0.01)
            floor = h - ceil
            shade = " "
            if cell_hit not in ("E", "D", "B", "A", "H", "R", "S"):
                if dist_w <= depth / 5.0:
                    shade = shades[4]
                elif dist_w <= depth / 4.0:
                    shade = shades[3]
                elif dist_w <= depth / 3.0:
                    shade = shades[2]
                elif dist_w <= depth / 1.6:
                    shade = shades[1]
                elif dist_w < depth:
                    shade = shades[0]

            col = []
            for y in range(h):
                if y <= ceil:
                    col.append(" ")
                elif ceil < y <= floor:
                    if cell_hit == "E":
                        col.append("👾")
                    elif cell_hit == "D":
                        col.append("💢")
                    elif cell_hit == "B":
                        col.append("☠️")
                    elif cell_hit == "A":
                        col.append("A" if y == int(floor) else " ")
                    elif cell_hit == "H":
                        col.append("+" if y == int(floor) else " ")
                    elif cell_hit == "R":
                        col.append("R" if y == int(floor) else " ")
                    elif cell_hit == "S":
                        col.append("$" if y == int(floor) else " ")
                    else:
                        col.append(shade)
                else:
                    b = 1.0 - ((float(y) - h / 2.0) / (h / 2.0))
                    col.append("." if b < 0.25 else "-" if b < 0.5 else "=" if b < 0.75 else " ")
            screen.append(col)

        out = []
        for y in range(h):
            row = "".join(screen[x][y] for x in range(w))
            out.append(row)
        cx, cy = w // 2, h // 2
        rr = list(out[cy])
        rr[cx] = "✚"
        out[cy] = "".join(rr)
        return "\n".join(out)

    def get_mini_map(self, st):
        px, py = int(st["x"]), int(st["y"])
        enemy_map = {(e["x"], e["y"]): e for e in st["enemies"].values()}
        out = []
        for y, row in enumerate(st["map"]):
            r = []
            for x, c in enumerate(row):
                if (x, y) in enemy_map:
                    et = enemy_map[(x, y)]["type"]
                    r.append("B" if et == "boss" else ("D" if et == "demon" else "M"))
                elif x == px and y == py:
                    r.append("P")
                elif c == " ":
                    r.append(".")
                else:
                    r.append(c)
            out.append("".join(r))
        return "\n".join(out)

    def _hud_text(self, st):
        view = st["settings"]["view"]
        parts = []
        if view in ("both", "map"):
            parts.append(f"{self.strings['e_map']} <b>Mini-Map</b>:\n<pre>{self.get_mini_map(st)}</pre>")
        if view in ("both", "3d"):
            parts.append(f"{self.strings['e_action']} <b>Action</b>:\n<pre>{self.render_3d_frame(st)}</pre>")
        parts.append(
            f"{self.strings['e_hp']} HP: <b>{st['hp']}</b> | AR: <b>{st['armor']}</b> | {self.strings['e_ammo']} Ammo: <b>{st['ammo']}</b>/<b>{st['reserve']}</b> | Med: <b>{st['medkits']}</b> | {self.strings['e_kills']} Kills: <b>{st['score']}</b>"
        )
        parts.append(f"{self._tr('wave', st)}: <b>{st['wave']}</b> | {self._tr('credits', st)}: <b>{st['credits']}</b>{' | DAILY' if st.get('daily') else ''}")
        parts.append(self._tr("hud_deadline", st, k=st["score"], a=self._acc(st), ar=st["armor"]))
        parts.append(f"{self.strings['e_log']} <i>{st['log']}</i>")
        return "\n".join(parts)

    def _buttons_game(self):
        return [
            [{"text": "🔄 L", "callback": self.action_rot_l}, {"text": "⬆️", "callback": self.action_fw}, {"text": "🔄 R", "callback": self.action_rot_r}],
            [{"text": "⬅️", "callback": self.action_m_l}, {"text": "💥", "callback": self.action_shoot}, {"text": "➡️", "callback": self.action_m_r}],
            [{"text": "🤜", "callback": self.action_melee}, {"text": "🔁", "callback": self.action_reload}, {"text": "🩹", "callback": self.action_medkit}],
            [{"text": "⬇️", "callback": self.action_bw}, {"text": "💾", "callback": self.action_save}, {"text": "🛒", "callback": self.action_shop}, {"text": "🚪", "callback": self.action_exit}],
        ]

    async def do_render(self, call, st):
        if st["hp"] <= 0:
            st["running"] = False
            p = self._profile()
            p["deaths"] += 1
            p["best_score"] = max(p["best_score"], st["score"])
            p["total_kills"] += st["score"]
            p["shots"] += st["shots"]
            p["hits"] += st["hits"]
            if st["wave"] >= 10:
                self._unlock_achievement(st, "wave_10", "Wave 10")
            if st.get("daily"):
                today = self._today_key()
                p["daily_last"] = today
                p["daily_best"] = max(p.get("daily_best", 0), st["score"])
                st["log"] = self._tr("daily_done", st, s=st["score"])
            p["achievements"] = sorted(set(p.get("achievements", [])) | set(st.get("ach", set())))
            self._save_profile(p)
            dead_text = self._tr("dead", st, d=self.strings["e_dead"], s=st["score"])
            btn = [[{"text": self._tr("new_game", st), "callback": self.action_new_menu}], [{"text": self._tr("back", st), "callback": self.action_main_menu}]]
            await self.safe_edit(call, dead_text, btn)
            return

        if st.get("new_ach"):
            st["log"] = self._tr("ach_unlock", st, n=st.pop("new_ach"))
        hud = self._hud_text(st)
        if st.get("last_hud") == hud:
            return
        if await self.safe_edit(call, hud, self._buttons_game()):
            st["last_hud"] = hud

    async def game_loop(self, call):
        user_id = "doom_user"
        while user_id in self.sessions and self.sessions[user_id].get("running"):
            st = self.sessions[user_id]
            now = time.time()
            ai_tick = self.difficulty[st["difficulty"]]["enemy_ai"]

            if not st["enemies"]:
                st["log"] = self._tr("wave_done", st, w=st["wave"])
                self._spawn_wave(st, st["wave"] + 1)
                if st["wave"] >= 3:
                    self._unlock_achievement(st, "slayer_3", "Slayer III")
                if st["wave"] >= 7:
                    self._unlock_achievement(st, "slayer_7", "Slayer VII")

            if now - st.get("last_ai", 0) > ai_tick:
                moved = False
                enemy_keys = list(st["enemies"].keys())
                random.shuffle(enemy_keys)
                occupied = {(e["x"], e["y"]) for e in st["enemies"].values()}

                for key in enemy_keys:
                    e = st["enemies"].get(key)
                    if not e:
                        continue
                    ex, ey = e["x"], e["y"]
                    dist = math.hypot(st["x"] - ex, st["y"] - ey)
                    if dist < 1.45:
                        _, dmg = self._enemy_stats(st, e["type"])
                        if st["armor"] > 0:
                            blocked = min(st["armor"], int(dmg * 0.45))
                            st["armor"] -= blocked
                            dmg -= blocked
                        st["hp"] -= max(1, dmg)
                        st["log"] = self._tr("enemy_attack", st, n=max(1, dmg))
                        moved = True
                        continue

                    dx = 1 if st["x"] > ex else (-1 if st["x"] < ex else 0)
                    dy = 1 if st["y"] > ey else (-1 if st["y"] < ey else 0)
                    candidates = [(ex + dx, ey), (ex, ey + dy), (ex + dx, ey + dy)]
                    random.shuffle(candidates)
                    for nx, ny in candidates:
                        if nx <= 0 or nx >= self.map_w - 1 or ny <= 0 or ny >= self.map_h - 1:
                            continue
                        if st["map"][ny][nx] != " ":
                            continue
                        if (nx, ny) in occupied:
                            continue
                        occupied.discard((ex, ey))
                        occupied.add((nx, ny))
                        e["x"], e["y"] = nx, ny
                        st["enemies"].pop(key, None)
                        st["enemies"][f"{nx}:{ny}"] = e
                        moved = True
                        break

                if moved:
                    st["dirty"] = True
                st["last_ai"] = now

            if now - st.get("last_save", 0) >= st["settings"]["autosave"]:
                sv = self._snapshot_state(st)
                sv["running"] = False
                self.db.set("Doom", "save", sv)
                st["last_save"] = now
                st["log"] = self._tr("autosaved", st)
                st["dirty"] = True

            if st.get("dirty") and now - st.get("last_render", 0) >= 0.55:
                try:
                    await self.do_render(call, st)
                    st["last_render"] = time.time()
                    st["dirty"] = False
                except Exception:
                    pass

            await asyncio.sleep(0.1)

    def update_player(self, dx, dy, dr):
        st = self.sessions.get("doom_user")
        if not st or st["hp"] <= 0:
            return

        def is_walkable(x, y):
            r = 0.17
            points = [(x - r, y - r), (x + r, y - r), (x - r, y + r), (x + r, y + r)]
            enemy_pos = {(e["x"], e["y"]) for e in st["enemies"].values()}
            for px, py in points:
                tx, ty = int(px), int(py)
                if tx < 0 or tx >= self.map_w or ty < 0 or ty >= self.map_h:
                    return False
                if st["map"][ty][tx] == "#":
                    return False
                if (tx, ty) in enemy_pos:
                    return False
            return True

        st["a"] += dr
        old_x, old_y = st["x"], st["y"]
        if dx or dy:
            nx = st["x"] + dx
            ny = st["y"] + dy
            if is_walkable(nx, st["y"]):
                st["x"] = nx
            if is_walkable(st["x"], ny):
                st["y"] = ny

            cx, cy = int(st["x"]), int(st["y"])
            if 0 <= cx < self.map_w and 0 <= cy < self.map_h:
                c = st["map"][cy][cx]
                if c == "A":
                    st["ammo"] += 6
                    st["reserve"] += 6
                    st["log"] = self._tr("pickup_ammo", st)
                    st["map"][cy][cx] = " "
                elif c == "H":
                    st["hp"] = min(st["max_hp"], st["hp"] + 20)
                    st["log"] = self._tr("pickup_hp", st)
                    st["map"][cy][cx] = " "
                elif c == "R":
                    st["armor"] = min(100, st["armor"] + 15)
                    st["log"] = self._tr("pickup_armor", st)
                    st["map"][cy][cx] = " "
                elif c == "S":
                    st["reserve"] += 8
                    st["medkits"] += 1
                    st["log"] = self._tr("pickup_secret", st)
                    st["map"][cy][cx] = " "

            if old_x == st["x"] and old_y == st["y"] and (dx or dy):
                st["log"] = self._tr("wall", st)

        st["dirty"] = True

    async def action_shoot(self, call):
        st = self.sessions.get("doom_user")
        if not st or st["hp"] <= 0:
            return
        if st["ammo"] <= 0:
            st["log"] = self._tr("no_ammo", st)
            st["dirty"] = True
            return

        st["ammo"] -= 1
        st["shots"] += 1
        rx, ry = math.sin(st["a"]), math.cos(st["a"])
        hit_key = None
        hit_dist = 999

        for key, e in st["enemies"].items():
            ex, ey = e["x"] + 0.5, e["y"] + 0.5
            vx, vy = ex - st["x"], ey - st["y"]
            proj = vx * rx + vy * ry
            if proj <= 0:
                continue
            perp = abs(vx * ry - vy * rx)
            if perp < 0.45 and proj < hit_dist:
                # simple wall check
                steps = int(proj / 0.1)
                blocked = False
                for i in range(1, steps + 1):
                    tx = int(st["x"] + rx * i * 0.1)
                    ty = int(st["y"] + ry * i * 0.1)
                    if st["map"][ty][tx] == "#":
                        blocked = True
                        break
                if not blocked:
                    hit_key = key
                    hit_dist = proj

        if hit_key:
            e = st["enemies"].get(hit_key)
            if e:
                base = 14 if e["type"] == "imp" else 12 if e["type"] == "demon" else 9
                dmg = int(base * (1.0 + 0.12 * st["upg"]["damage"]))
                e["hp"] -= dmg
                st["hits"] += 1
                if e["hp"] <= 0:
                    etype = e["type"]
                    st["enemies"].pop(hit_key, None)
                    st["score"] += 3 if etype == "boss" else 1
                    self._award_for_kill(st, etype)
                    if etype == "boss":
                        self._unlock_achievement(st, "boss_kill", "Boss Killer")
                    st["log"] = self._tr("hit", st, f=self.strings["e_fire"])
                    if random.random() < 0.22:
                        st["reserve"] += 4
                    if random.random() < 0.11:
                        st["medkits"] += 1
                else:
                    st["log"] = self._tr("enemy_hit", st)
        else:
            st["log"] = self._tr("shot_wall", st)

        if st["shots"] >= 25 and self._acc(st) >= 70:
            self._unlock_achievement(st, "sharpshooter", "Sharpshooter")
        st["dirty"] = True

    async def action_melee(self, call):
        st = self.sessions.get("doom_user")
        if not st or st["hp"] <= 0:
            return
        nearest = None
        nd = 999
        for key, e in st["enemies"].items():
            d = math.hypot(st["x"] - e["x"], st["y"] - e["y"])
            if d < nd:
                nd = d
                nearest = key
        if nearest and nd < 1.4:
            e = st["enemies"][nearest]
            e["hp"] -= int(9 * (1.0 + 0.1 * st["upg"]["damage"]))
            st["hits"] += 1
            if e["hp"] <= 0:
                etype = e["type"]
                st["enemies"].pop(nearest, None)
                st["score"] += 3 if etype == "boss" else 1
                self._award_for_kill(st, etype)
                if etype == "boss":
                    self._unlock_achievement(st, "boss_kill", "Boss Killer")
                st["log"] = self._tr("hit", st, f=self.strings["e_fire"])
            else:
                st["log"] = self._tr("enemy_hit", st)
        else:
            st["log"] = self._tr("enemy_miss", st)
        st["dirty"] = True

    async def action_reload(self, call):
        st = self.sessions.get("doom_user")
        if not st or st["hp"] <= 0:
            return
        if st["reserve"] <= 0:
            st["log"] = self._tr("reload_none", st)
        else:
            need = st["mag_size"] - st["ammo"]
            add = min(need, st["reserve"]) if need > 0 else 0
            st["ammo"] += add
            st["reserve"] -= add
            st["log"] = self._tr("reload_done", st, n=add)
        st["dirty"] = True

    async def action_medkit(self, call):
        st = self.sessions.get("doom_user")
        if not st or st["hp"] <= 0:
            return
        if st["medkits"] <= 0:
            st["log"] = self._tr("med_none", st)
        else:
            st["medkits"] -= 1
            st["hp"] = min(st["max_hp"], st["hp"] + 30)
            st["log"] = self._tr("med_used", st)
        st["dirty"] = True

    async def action_new_menu(self, call):
        txt = f"{self.strings['e_title']} <b>DOOM</b>\n\n{self._tr('difficulty_pick')}"
        btn = [
            [{"text": "🟢 EASY", "callback": self.action_new_easy}, {"text": "🟡 NORMAL", "callback": self.action_new_normal}],
            [{"text": "🟠 HARD", "callback": self.action_new_hard}, {"text": "🔴 NIGHTMARE", "callback": self.action_new_nightmare}],
            [{"text": self._tr("back"), "callback": self.action_main_menu}],
        ]
        await self.safe_edit(call, txt, btn)

    async def _start_game(self, call, diff):
        try:
            settings = self._settings()
            settings["difficulty"] = diff
            self._save_settings(settings)

            game_map, _ = self._parse_map()
            hp0 = self.difficulty[diff]["hp"]
            ammo0 = self.difficulty[diff]["ammo"]

            p = self._profile()
            p["games"] += 1
            self._save_profile(p)

            st = {
                "x": 1.5,
                "y": 1.5,
                "a": 0.0,
                "hp": hp0,
                "max_hp": hp0,
                "armor": 0,
                "ammo": min(ammo0, 12),
                "mag_size": 12,
                "reserve": max(0, ammo0 - 12),
                "medkits": 1,
                "score": 0,
                "shots": 0,
                "hits": 0,
                "credits": 0,
                "wave": 0,
                "upg": {"damage": 0, "armor": 0, "med": 0, "mag": 0},
                "ach": set(p.get("achievements", [])),
                "new_ach": "",
                "daily": False,
                "difficulty": diff,
                "settings": settings,
                "log": self._tr("started"),
                "last_render": 0,
                "last_ai": 0,
                "last_save": 0,
                "dirty": True,
                "running": True,
                "map": game_map,
                "enemies": {},
                "last_hud": "",
            }
            st["armor"] += st["upg"]["armor"]
            st["medkits"] += st["upg"]["med"]
            st["mag_size"] += st["upg"]["mag"] * 2
            st["ammo"] = min(st["ammo"], st["mag_size"])
            self._spawn_wave(st, 1)
            self.sessions["doom_user"] = st
            asyncio.create_task(self.game_loop(call))
        except Exception:
            err = traceback.format_exc()
            await self.safe_edit(call, f"<b>CRITICAL ERROR:</b>\n<pre>{err}</pre>", [])

    async def action_new_easy(self, call):
        await self._start_game(call, "easy")

    async def action_new_normal(self, call):
        await self._start_game(call, "normal")

    async def action_new_hard(self, call):
        await self._start_game(call, "hard")

    async def action_new_nightmare(self, call):
        await self._start_game(call, "nightmare")

    async def action_cont(self, call):
        try:
            sv = self.db.get("Doom", "save", None)
            if sv:
                sv.setdefault("credits", 0)
                sv.setdefault("wave", 1)
                sv.setdefault("upg", {"damage": 0, "armor": 0, "med": 0, "mag": 0})
                sv.setdefault("ach", set())
                if isinstance(sv.get("ach"), list):
                    sv["ach"] = set(sv["ach"])
                sv.setdefault("new_ach", "")
                sv.setdefault("daily", False)
                sv.setdefault("enemies", {})
                sv["running"] = True
                sv["dirty"] = True
                sv["log"] = self._tr("loaded", sv)
                sv["last_hud"] = ""
                sv["last_save"] = time.time()
                self.sessions["doom_user"] = sv
                asyncio.create_task(self.game_loop(call))
            else:
                await self.safe_edit(call, self._tr("no_save"), [[{"text": self._tr("back"), "callback": self.action_main_menu}]])
        except Exception:
            err = traceback.format_exc()
            await self.safe_edit(call, f"<b>CRITICAL ERROR:</b>\n<pre>{err}</pre>", [])

    async def action_save(self, call):
        st = self.sessions.get("doom_user")
        if st:
            sv = self._snapshot_state(st)
            sv["running"] = False
            self.db.set("Doom", "save", sv)
            self._sync_achievements_to_profile(st)
            st["log"] = self._tr("saved", st)
            st["dirty"] = True

    async def action_exit(self, call):
        if "doom_user" in self.sessions:
            self._sync_achievements_to_profile(self.sessions["doom_user"])
            self.sessions["doom_user"]["running"] = False
            del self.sessions["doom_user"]
        await self.safe_edit(call, self._tr("game_closed"), [[{"text": self._tr("back"), "callback": self.action_main_menu}]])

    async def action_records(self, call):
        p = self._profile()
        acc = round((p["hits"] / p["shots"] * 100.0), 1) if p["shots"] else 0.0
        ach = p.get("achievements", [])
        text = self._tr(
            "profile",
            gp=p["games"],
            tk=p["total_kills"],
            td=p["deaths"],
            bs=p["best_score"],
            sh=p["shots"],
            hh=p["hits"],
            acc=acc,
            db=p.get("daily_best", 0),
            ac=len(ach),
        )
        if ach:
            text += "\n\n" + self._tr("ach_title") + "\n" + "\n".join(f"• {x}" for x in ach[-8:])
        btn = [[{"text": self._tr("back"), "callback": self.action_main_menu}]]
        if "doom_user" in self.sessions and self.sessions["doom_user"].get("running"):
            btn.insert(0, [{"text": self._tr("in_game", self.sessions["doom_user"]), "callback": self.action_refresh_game}])
        await self.safe_edit(call, text, btn)

    async def action_daily_run(self, call):
        p = self._profile()
        today = self._today_key()
        if p.get("daily_last") == today:
            await self.safe_edit(call, self._tr("daily_cooldown"), [[{"text": self._tr("back"), "callback": self.action_main_menu}]])
            return
        random.seed(int(today.replace("-", "")))
        await self._start_game(call, "hard")
        st = self.sessions.get("doom_user")
        if st:
            st["daily"] = True
            st["log"] = self._tr("daily_started", st)
            st["dirty"] = True

    async def action_shop(self, call):
        st = self.sessions.get("doom_user")
        if not st or st["hp"] <= 0:
            await self._show_menu(call)
            return
        c = st["credits"]
        text = self._tr("shop_title", st, c=c)
        dmg = 1 + st["upg"]["damage"] * 0.12
        btn = [
            [{"text": f"{self._tr('upgrade_damage', st, v=int((dmg-1)*100))} | 25", "callback": self.action_buy_damage}],
            [{"text": f"{self._tr('upgrade_armor', st, v=5)} | 20", "callback": self.action_buy_armor}],
            [{"text": f"{self._tr('upgrade_med', st)} | 30", "callback": self.action_buy_med}],
            [{"text": f"{self._tr('upgrade_mag', st, v=2)} | 22", "callback": self.action_buy_mag}],
            [{"text": self._tr("back", st), "callback": self.action_refresh_game}],
        ]
        await self.safe_edit(call, text, btn)

    async def _buy_upgrade(self, call, key, cost, max_lvl):
        st = self.sessions.get("doom_user")
        if not st or st["hp"] <= 0:
            return
        lvl = st["upg"].get(key, 0)
        if lvl >= max_lvl:
            st["log"] = self._tr("buy_max", st)
            st["dirty"] = True
            await self.action_shop(call)
            return
        if st["credits"] < cost:
            st["log"] = self._tr("buy_no", st)
            st["dirty"] = True
            await self.action_shop(call)
            return
        st["credits"] -= cost
        st["upg"][key] = lvl + 1
        if key == "armor":
            st["armor"] = min(100, st["armor"] + 5)
        elif key == "med":
            st["medkits"] += 1
        elif key == "mag":
            st["mag_size"] += 2
            st["ammo"] += 2
        st["log"] = self._tr("buy_ok", st, n=key)
        st["dirty"] = True
        if sum(st["upg"].values()) >= 6:
            self._unlock_achievement(st, "engineer", "Doom Engineer")
        await self.action_shop(call)

    async def action_buy_damage(self, call):
        await self._buy_upgrade(call, "damage", 25, 6)

    async def action_buy_armor(self, call):
        await self._buy_upgrade(call, "armor", 20, 8)

    async def action_buy_med(self, call):
        await self._buy_upgrade(call, "med", 30, 4)

    async def action_buy_mag(self, call):
        await self._buy_upgrade(call, "mag", 22, 6)

    async def action_help(self, call):
        pref = getattr(self, "get_prefix", lambda: ".")()
        txt = self._tr("help_text", pref=pref)
        await self.safe_edit(call, txt, [[{"text": self._tr("back"), "callback": self.action_main_menu}]])

    async def action_settings(self, call):
        s = self._settings()
        text = (
            f"<b>{self._tr('settings')}</b>\n\n"
            f"{self._tr('difficulty')}: <b>{s['difficulty'].upper()}</b>\n"
            f"{self._tr('view')}: <b>{s['view']}</b>\n"
            f"{self._tr('autosave')}: <b>{s['autosave']}s</b>\n"
            f"{self._tr('lang')}: <b>{s['lang']}</b>"
        )
        btn = [
            [{"text": f"{self._tr('difficulty')}: {s['difficulty'].upper()}", "callback": self.action_cycle_difficulty}],
            [{"text": f"{self._tr('view')}: {s['view']}", "callback": self.action_cycle_view}],
            [{"text": f"{self._tr('autosave')}: {s['autosave']}s", "callback": self.action_cycle_autosave}],
            [{"text": f"{self._tr('lang')}: {s['lang']}", "callback": self.action_cycle_lang}],
            [{"text": self._tr("back"), "callback": self.action_main_menu}],
        ]
        await self.safe_edit(call, text, btn)

    async def action_settings_in_game(self, call):
        await self.action_settings(call)

    async def action_cycle_difficulty(self, call):
        s = self._settings()
        seq = ["easy", "normal", "hard", "nightmare"]
        s["difficulty"] = seq[(seq.index(s["difficulty"]) + 1) % len(seq)]
        self._save_settings(s)
        await self.action_settings(call)

    async def action_cycle_view(self, call):
        s = self._settings()
        seq = ["both", "3d", "map"]
        s["view"] = seq[(seq.index(s["view"]) + 1) % len(seq)]
        self._save_settings(s)
        st = self.sessions.get("doom_user")
        if st:
            st["settings"] = s
            st["dirty"] = True
            st["log"] = self._tr("set_ok", st)
        await self.action_settings(call)

    async def action_cycle_autosave(self, call):
        s = self._settings()
        seq = [10, 15, 20, 30, 45]
        s["autosave"] = seq[(seq.index(s["autosave"]) + 1) % len(seq)] if s["autosave"] in seq else 15
        self._save_settings(s)
        await self.action_settings(call)

    async def action_cycle_lang(self, call):
        s = self._settings()
        seq = ["ru", "en", "uk", "de", "jp", "neofit", "tiktok", "leet", "uwu"]
        s["lang"] = seq[(seq.index(s["lang"]) + 1) % len(seq)] if s["lang"] in seq else "ru"
        self._save_settings(s)
        st = self.sessions.get("doom_user")
        if st:
            st["settings"] = s
            st["dirty"] = True
        await self.action_settings(call)

    async def action_main_menu(self, call):
        await self._show_menu(call)

    async def action_refresh_game(self, call):
        st = self.sessions.get("doom_user")
        if st and st.get("running"):
            st["dirty"] = True
            await self.do_render(call, st)
        else:
            await self._show_menu(call)

    async def action_rot_l(self, call):
        self.update_player(0, 0, -0.38)

    async def action_rot_r(self, call):
        self.update_player(0, 0, 0.38)

    async def action_fw(self, call):
        a = self.sessions.get("doom_user", {}).get("a", 0)
        self.update_player(math.sin(a) * 0.42, math.cos(a) * 0.42, 0)

    async def action_bw(self, call):
        a = self.sessions.get("doom_user", {}).get("a", 0)
        self.update_player(-math.sin(a) * 0.42, -math.cos(a) * 0.42, 0)

    async def action_m_l(self, call):
        a = self.sessions.get("doom_user", {}).get("a", 0) - math.pi / 2
        self.update_player(math.sin(a) * 0.36, math.cos(a) * 0.36, 0)

    async def action_m_r(self, call):
        a = self.sessions.get("doom_user", {}).get("a", 0) + math.pi / 2
        self.update_player(math.sin(a) * 0.36, math.cos(a) * 0.36, 0)
