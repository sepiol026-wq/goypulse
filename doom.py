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
# meta developer: @goymodules
# authors: @goymodules
# Description: Inline DOOM mini-game module.
# meta banner: https://raw.githubusercontent.com/sepiol026-wq/goypulse/main/assets/doom.png

"""запускает doom."""

__version__ = (1, 1, 6)

import math
import time
import asyncio
import re
import traceback
from herokutl.types import Message
from .. import loader, utils

@loader.tds
class Doom(loader.Module):
    """Inline DOOM mini-game."""

    strings = {
        "name": "Doom",
        "_cls_doc": "Inline DOOM mini-game.",
    }
    strings_ru = {"_cls_doc": "Мини-игра DOOM в инлайне."}

    def __init__(self):
        self.sessions = {}
        self.game_config = {
            "scr_w": 24,
            "scr_h": 12,
            "fov": math.pi / 3,
            "depth": 10.0,
            "shades": [" ", "░", "▒", "▓", "█"]
        }
        self.base_map = [
            "################",
            "#....A.........#",
            "#.####.E..####.#",
            "#.#..#....#..#.#",
            "#.#..######..#.#",
            "#........E...H.#",
            "######.........#",
            "#...A..........#",
            "#.######...#...#",
            "#......#...#...#",
            "#.####.#.E.#...#",
            "#..H...........#",
            "################"
        ]
        self.map_h = len(self.base_map)
        self.map_w = len(self.base_map[0])

    async def client_ready(self, client, db):
        self.client = client
        self.db = db

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

    @loader.command(ru_doc="Справка по игре DOOM")
    async def hdoomcmd(self, message: Message):
        text = (
            "<tg-emoji emoji-id=5256230583717079814>📝</tg-emoji> <b>Справка по DOOM</b>\n\n"
            "<b>Интерфейс (Карта / 3D):</b>\n"
            "<code>P</code> - Ваш персонаж\n"
            "<code>M</code> / <code>👾</code> - Монстр (Атакует вблизи)\n"
            "<code>A</code> / <code>[A]</code> - Патроны (+5)\n"
            "<code>H</code> / <code>[+]</code> - Аптечка (+25 HP)\n\n"
            "Запуск игры: <code>(префикс)doom</code>"
        )
        await utils.answer(message, text)

    @loader.command(ru_doc="Запуск меню DOOM")
    async def doomcmd(self, message: Message):
        buttons = [
            [{"text": "🟢 Новая игра", "callback": self.action_new}],
            [{"text": "🟡 Продолжить", "callback": self.action_cont}]
        ]
        await self.inline.form(
            text="<tg-emoji emoji-id=5256054975389247793>📛</tg-emoji> <b>DOOM</b>\n\nВыбери действие:",
            message=message,
            reply_markup=buttons
        )

    def render_3d_frame(self, state):
        w = self.game_config["scr_w"]
        h = self.game_config["scr_h"]
        fov = self.game_config["fov"]
        depth = self.game_config["depth"]
        shades = self.game_config["shades"]

        px, py, pa = state["x"], state["y"], state["a"]
        screen = []

        for x in range(w):
            ray_a = (pa - fov / 2.0) + (x / float(w)) * fov
            dist_w = 0.0
            hit_w = False
            cell_hit = " "

            eye_x = math.sin(ray_a)
            eye_y = math.cos(ray_a)

            while not hit_w and dist_w < depth:
                dist_w += 0.1
                test_x = int(px + eye_x * dist_w)
                test_y = int(py + eye_y * dist_w)

                if test_x < 0 or test_x >= self.map_w or test_y < 0 or test_y >= self.map_h:
                    hit_w = True
                    dist_w = depth
                else:
                    cell = state["map"][test_y][test_x]
                    if cell in ["#", "E", "A", "H"]:
                        hit_w = True
                        cell_hit = cell

            ceil = float(h / 2.0) - h / dist_w
            floor = h - ceil

            shade = " "
            if cell_hit not in ["E", "A", "H"]:
                if dist_w <= depth / 5.0: shade = shades[4]
                elif dist_w <= depth / 4.0: shade = shades[3]
                elif dist_w <= depth / 3.0: shade = shades[2]
                elif dist_w <= depth / 1.5: shade = shades[1]
                elif dist_w < depth: shade = shades[0]

            col = []
            for y in range(h):
                if y <= ceil:
                    col.append(" ")
                elif y > ceil and y <= floor:
                    if cell_hit == "E": col.append("👾")
                    elif cell_hit == "A":
                        if y == int(floor): col.append("A")
                        elif y == int(floor) - 1: col.append("[")
                        elif y == int(floor) + 1: col.append("]")
                        else: col.append(" ")
                    elif cell_hit == "H":
                        if y == int(floor): col.append("+")
                        elif y == int(floor) - 1: col.append("[")
                        elif y == int(floor) + 1: col.append("]")
                        else: col.append(" ")
                    else:
                        col.append(shade)
                else:
                    b = 1.0 - ((float(y) - h / 2.0) / (h / 2.0))
                    if b < 0.25: col.append(".")
                    elif b < 0.5: col.append("-")
                    elif b < 0.75: col.append("=")
                    else: col.append(" ")
            screen.append(col)

        out = []
        for y in range(h):
            row = "".join([screen[x][y] for x in range(w)])
            out.append(row)
        cx = w // 2
        cy = h // 2
        if 0 <= cy < len(out) and 0 <= cx < len(out[cy]):
            r = list(out[cy])
            r[cx] = "✚"
            out[cy] = "".join(r)
        return "\n".join(out)

    def get_mini_map(self, state):
        px, py = int(state["x"]), int(state["y"])
        m = []
        for y, row in enumerate(state["map"]):
            r = [" " if c == "." else c for c in row]
            r = ["M" if c == "E" else c for c in r]
            if y == py and 0 <= px < len(r):
                r[px] = "P"
            m.append("".join(r))
        return "\n".join(m)

    async def do_render(self, call, st):
        if st["hp"] <= 0:
            st["running"] = False
            dead_text = f"<tg-emoji emoji-id=5256054975389247793>📛</tg-emoji> <b>ВЫ ПОГИБЛИ</b>\n\nСчет: {st['score']}\nНажмите Новая игра, чтобы воскреснуть."
            btn = [[{"text": "🔄 Новая игра", "callback": self.action_new}]]
            await self.safe_edit(call, dead_text, btn)
            return

        frame = self.render_3d_frame(st)
        mmap = self.get_mini_map(st)

        hud = (
            f"<tg-emoji emoji-id=5253713110111365241>📍</tg-emoji> <b>Mini-Map</b>:\n"
            f"<pre>{mmap}</pre>\n"
            f"<tg-emoji emoji-id=5256079005731271025>📟</tg-emoji> <b>Action</b>:\n"
            f"<pre>{frame}</pre>\n"
            f"<tg-emoji emoji-id=5253549669425882943>🔋</tg-emoji> HP: <b>{st['hp']}</b> | <tg-emoji emoji-id=5256094480498436162>📦</tg-emoji> Ammo: <b>{st['ammo']}</b> | <tg-emoji emoji-id=5256054975389247793>📛</tg-emoji> Kills: <b>{st['score']}</b>\n"
            f"<tg-emoji emoji-id=5253590213917158323>💬</tg-emoji> <i>{st['log']}</i>"
        )

        btn = [
            [{"text": "🔄 L", "callback": self.action_rot_l}, {"text": "⬆️", "callback": self.action_fw}, {"text": "🔄 R", "callback": self.action_rot_r}],
            [{"text": "⬅️", "callback": self.action_m_l}, {"text": "💥", "callback": self.action_shoot}, {"text": "➡️", "callback": self.action_m_r}],
            [{"text": "⬇️", "callback": self.action_bw}, {"text": "💾", "callback": self.action_save}, {"text": "🚪", "callback": self.action_exit}]
        ]
        if st.get("last_hud") == hud:
            return
        if await self.safe_edit(call, hud, btn):
            st["last_hud"] = hud

    async def game_loop(self, call):
        user_id = "doom_user"
        while user_id in self.sessions and self.sessions[user_id].get("running"):
            st = self.sessions[user_id]
            now = time.time()

            if now - st.get("last_ai", 0) > 0.7:
                moved = False
                enemies = []
                for y in range(self.map_h):
                    for x in range(self.map_w):
                        if st["map"][y][x] == "E":
                            enemies.append((x, y))

                for ex, ey in enemies:
                    dist = math.hypot(st["x"] - ex, st["y"] - ey)
                    if dist < 1.5:
                        st["hp"] -= 8
                        st["log"] = "Монстр атакует! -8 HP"
                        moved = True
                    else:
                        dx = 1 if st["x"] > ex else (-1 if st["x"] < ex else 0)
                        dy = 1 if st["y"] > ey else (-1 if st["y"] < ey else 0)

                        if dx != 0 and st["map"][ey][ex+dx] == " ":
                            st["map"][ey][ex] = " "
                            st["map"][ey][ex+dx] = "E"
                            moved = True
                        elif dy != 0 and st["map"][ey+dy][ex] == " ":
                            st["map"][ey][ex] = " "
                            st["map"][ey+dy][ex] = "E"
                            moved = True

                if moved: st["dirty"] = True
                st["last_ai"] = now

            if now - st.get("last_save", 0) >= 15:
                sv = st.copy()
                sv["running"] = False
                self.db.set("Doom", "save", sv)
                st["last_save"] = now
                if st.get("log") != "Автосохранение выполнено.":
                    st["log"] = "Автосохранение выполнено."
                    st["dirty"] = True

            if st.get("dirty") and now - st.get("last_render", 0) >= 0.6:
                try:
                    await self.do_render(call, st)
                    st["last_render"] = time.time()
                    st["dirty"] = False
                except Exception:
                    pass

            await asyncio.sleep(0.12)

    def update_player(self, dx, dy, dr):
        if "doom_user" not in self.sessions: return
        st = self.sessions["doom_user"]
        if st["hp"] <= 0: return

        def is_walkable(x, y):
            r = 0.17
            points = [(x-r, y-r), (x+r, y-r), (x-r, y+r), (x+r, y+r)]
            for px, py in points:
                tx, ty = int(px), int(py)
                if tx < 0 or tx >= self.map_w or ty < 0 or ty >= self.map_h:
                    return False
                if st["map"][ty][tx] in ["#", "E"]:
                    return False
            return True

        st["a"] += dr
        if dx or dy:
            old_x, old_y = st["x"], st["y"]
            nx = st["x"] + dx
            if is_walkable(nx, st["y"]):
                st["x"] = nx
            ny = st["y"] + dy
            if is_walkable(st["x"], ny):
                st["y"] = ny

            cell_x, cell_y = int(st["x"]), int(st["y"])
            if 0 <= cell_x < self.map_w and 0 <= cell_y < self.map_h:
                cell = st["map"][cell_y][cell_x]
                if cell == "A":
                    st["ammo"] += 5
                    st["log"] = "Патроны! +5"
                    st["map"][cell_y][cell_x] = " "
                elif cell == "H":
                    st["hp"] = min(100, st["hp"] + 25)
                    st["log"] = "Аптечка! +25 HP"
                    st["map"][cell_y][cell_x] = " "
            if old_x == st["x"] and old_y == st["y"] and (dx or dy):
                st["log"] = "Упёрлись в стену."
        st["dirty"] = True

    async def action_shoot(self, call):
        st = self.sessions.get("doom_user")
        if not st or st["hp"] <= 0: return

        if st["ammo"] <= 0:
            st["log"] = "Клик! Нет патронов!"
            st["dirty"] = True
            return

        st["ammo"] -= 1
        hit = False
        rx, ry = math.sin(st["a"]), math.cos(st["a"])
        d = 0.0

        while d < self.game_config["depth"]:
            d += 0.1
            fx, fy = st["x"] + rx * d, st["y"] + ry * d
            tx, ty = int(fx), int(fy)
            if 0 <= tx < self.map_w and 0 <= ty < self.map_h:
                cell = st["map"][ty][tx]
                if cell == "E":
                    st["map"][ty][tx] = " "
                    st["score"] += 1
                    st["log"] = "<tg-emoji emoji-id=5253877736207821121>🔥</tg-emoji> Монстр разорван!"
                    hit = True
                    break
                if cell == "#":
                    break
            else:
                break

        if not hit: st["log"] = "Выстрел в стену."
        st["dirty"] = True

    async def action_new(self, call):
        try:
            m = []
            for row in self.base_map:
                m.append(list(row.replace(".", " ")))

            self.sessions["doom_user"] = {
                "x": 1.5, "y": 1.5, "a": 0.0,
                "hp": 100, "ammo": 10, "score": 0,
                "log": "Добро пожаловать в Ад.",
                "last_render": 0, "last_ai": 0,
                "last_save": 0,
                "dirty": True, "running": True,
                "map": m,
                "last_hud": ""
            }
            asyncio.create_task(self.game_loop(call))
        except Exception as e:
            err = traceback.format_exc()
            await self.safe_edit(call, f"<b>CRITICAL ERROR:</b>\n<pre>{err}</pre>", [])

    async def action_cont(self, call):
        try:
            sv = self.db.get("Doom", "save", None)
            if sv:
                sv["running"] = True
                sv["dirty"] = True
                sv["log"] = "Игра загружена."
                sv["last_hud"] = ""
                sv["last_save"] = time.time()
                self.sessions["doom_user"] = sv
                asyncio.create_task(self.game_loop(call))
            else:
                await self.safe_edit(call, "Нет сохранений! Запустите модуль заново.", [])
        except Exception as e:
            err = traceback.format_exc()
            await self.safe_edit(call, f"<b>CRITICAL ERROR:</b>\n<pre>{err}</pre>", [])

    async def action_save(self, call):
        if "doom_user" in self.sessions:
            st = self.sessions["doom_user"].copy()
            st["running"] = False
            self.db.set("Doom", "save", st)
            st["log"] = "Сохранено!"
            st["dirty"] = True

    async def action_exit(self, call):
        if "doom_user" in self.sessions:
            self.sessions["doom_user"]["running"] = False
            del self.sessions["doom_user"]
        await self.safe_edit(call, "Игра завершена.", [])

    async def action_rot_l(self, call): self.update_player(0, 0, -0.4)
    async def action_rot_r(self, call): self.update_player(0, 0, 0.4)
    async def action_fw(self, call):
        a = self.sessions.get("doom_user", {}).get("a", 0)
        self.update_player(math.sin(a)*0.45, math.cos(a)*0.45, 0)
    async def action_bw(self, call):
        a = self.sessions.get("doom_user", {}).get("a", 0)
        self.update_player(-math.sin(a)*0.45, -math.cos(a)*0.45, 0)
    async def action_m_l(self, call):
        a = self.sessions.get("doom_user", {}).get("a", 0) - math.pi/2
        self.update_player(math.sin(a)*0.4, math.cos(a)*0.4, 0)
    async def action_m_r(self, call):
        a = self.sessions.get("doom_user", {}).get("a", 0) + math.pi/2
        self.update_player(math.sin(a)*0.4, math.cos(a)*0.4, 0)

_cls_doc_Doom = (Doom.__doc__ or "").strip()
if _cls_doc_Doom:
    Doom.strings.setdefault("_cls_doc", _cls_doc_Doom)
if not hasattr(Doom, "strings_uk") and hasattr(Doom, "strings_ua"):
    Doom.strings_uk = dict(getattr(Doom, "strings_ua"))
for _loc in ("ru", "uk", "de", "jp", "neofit", "tiktok", "leet", "uwu"):
    _attr = f"strings_{_loc}"
    if not hasattr(Doom, _attr):
        setattr(Doom, _attr, dict(getattr(Doom, "strings", {})))
    _d = getattr(Doom, _attr)
    if isinstance(_d, dict) and _cls_doc_Doom:
        _d.setdefault("_cls_doc", _cls_doc_Doom)
for _name in dir(Doom):
    _fn = getattr(Doom, _name, None)
    if not callable(_fn) or not getattr(_fn, "is_command", False):
        continue
    _base = (
        getattr(_fn, "en_doc", None)
        or getattr(_fn, "ru_doc", None)
        or getattr(_fn, "uk_doc", None)
        or getattr(_fn, "de_doc", None)
        or getattr(_fn, "jp_doc", None)
        or getattr(_fn, "neofit_doc", None)
        or getattr(_fn, "tiktok_doc", None)
        or getattr(_fn, "leet_doc", None)
        or getattr(_fn, "uwu_doc", None)
        or getattr(_fn, "__doc__", None)
        or ""
    ).strip()
    if not _base:
        continue
    for _doc in ("en_doc", "ru_doc", "uk_doc", "de_doc", "jp_doc", "neofit_doc", "tiktok_doc", "leet_doc", "uwu_doc"):
        if not getattr(_fn, _doc, None):
            setattr(_fn, _doc, _base)
_i18n_boot_Doom = True

