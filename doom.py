# requires: herokutl
# meta developer: @samsepi0l_ovf
# authors: @samsepi0l_ovf
# Description: Inline DOOM mini-game module.
# meta banner: https://raw.githubusercontent.com/sepiol026-wq/goypulse/main/banner.png

__version__ = (1, 0, 0)

import math
import time
import asyncio
import traceback
from herokutl.types import Message
from .. import loader, utils

@loader.tds
class Doom(loader.Module):
    strings = {"name": "Doom"}

    def __init__(self):
        self.sessions = {}
        self.config = {
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

    @loader.command(ru_doc="Справка по игре DOOM")
    async def hdoomcmd(self, message: Message):
        text = (
            "📖 <b>Справка по DOOM</b>\n\n"
            "<b>Интерфейс (Карта / 3D):</b>\n"
            "<code>P</code> - Ваш персонаж\n"
            "<code>E</code> / <code>Ж</code> - Монстр (Атакует вблизи)\n"
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
        w = self.config["scr_w"]
        h = self.config["scr_h"]
        fov = self.config["fov"]
        depth = self.config["depth"]
        shades = self.config["shades"]

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
                    if cell_hit == "E": col.append("Ж")
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
        return "\n".join(out)

    def get_mini_map(self, state):
        px, py = int(state["x"]), int(state["y"])
        m = []
        for y, row in enumerate(state["map"]):
            r = [" " if c == "." else c for c in row]
            if y == py and 0 <= px < len(r):
                r[px] = "P"
            m.append("".join(r))
        return "\n".join(m)

    async def do_render(self, call, st):
        if st["hp"] <= 0:
            st["running"] = False
            dead_text = f"<tg-emoji emoji-id=5256054975389247793>📛</tg-emoji> <b>ВЫ ПОГИБЛИ</b>\n\nСчет: {st['score']}\nНажмите Новая игра, чтобы воскреснуть."
            btn = [[{"text": "🔄 Новая игра", "callback": self.action_new}]]
            await call.edit(dead_text, reply_markup=btn)
            return

        frame = self.render_3d_frame(st)
        mmap = self.get_mini_map(st)

        hud = (
            f"🗺 <b>Mini-Map</b>:\n"
            f"<pre>{mmap}</pre>\n"
            f"📺 <b>Action</b>:\n"
            f"<pre>{frame}</pre>\n"
            f"❤️ HP: <b>{st['hp']}</b> | 🔫 Ammo: <b>{st['ammo']}</b> | <tg-emoji emoji-id=5256054975389247793>📛</tg-emoji> Kills: <b>{st['score']}</b>\n"
            f"<tg-emoji emoji-id=5253590213917158323>💬</tg-emoji> <i>{st['log']}</i>"
        )

        btn = [
            [{"text": "🔄 L", "callback": self.action_rot_l}, {"text": "⬆️", "callback": self.action_fw}, {"text": "🔄 R", "callback": self.action_rot_r}],
            [{"text": "⬅️", "callback": self.action_m_l}, {"text": "💥", "callback": self.action_shoot}, {"text": "➡️", "callback": self.action_m_r}],
            [{"text": "⬇️", "callback": self.action_bw}, {"text": "💾", "callback": self.action_save}, {"text": "🚪", "callback": self.action_exit}]
        ]
        await call.edit(hud, reply_markup=btn)

    async def game_loop(self, call):
        user_id = "doom_user"
        while user_id in self.sessions and self.sessions[user_id].get("running"):
            st = self.sessions[user_id]
            now = time.time()

            if now - st.get("last_ai", 0) > 1.5:
                moved = False
                enemies = []
                for y in range(self.map_h):
                    for x in range(self.map_w):
                        if st["map"][y][x] == "E":
                            enemies.append((x, y))

                for ex, ey in enemies:
                    dist = math.hypot(st["x"] - ex, st["y"] - ey)
                    if dist < 1.5:
                        st["hp"] -= 15
                        st["log"] = "Монстр кусает! -15 HP"
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

            if st.get("dirty") and now - st.get("last_render", 0) >= 1.0:
                try:
                    await self.do_render(call, st)
                    st["last_render"] = time.time()
                    st["dirty"] = False
                except Exception:
                    pass

            await asyncio.sleep(0.1)

    def update_player(self, dx, dy, dr):
        if "doom_user" not in self.sessions: return
        st = self.sessions["doom_user"]
        if st["hp"] <= 0: return

        st["a"] += dr
        if dx or dy:
            nx, ny = st["x"] + dx, st["y"] + dy
            if 0 <= int(nx) < self.map_w and 0 <= int(ny) < self.map_h:
                cell = st["map"][int(ny)][int(nx)]
                if cell not in ["#", "E"]:
                    st["x"], st["y"] = nx, ny
                    if cell == "A":
                        st["ammo"] += 5
                        st["log"] = "Патроны! +5"
                        st["map"][int(ny)][int(nx)] = " "
                    elif cell == "H":
                        st["hp"] = min(100, st["hp"] + 25)
                        st["log"] = "Аптечка! +25 HP"
                        st["map"][int(ny)][int(nx)] = " "
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

        for d in range(1, int(self.config["depth"])):
            tx, ty = int(st["x"] + rx * d), int(st["y"] + ry * d)
            if 0 <= tx < self.map_w and 0 <= ty < self.map_h:
                if st["map"][ty][tx] == "E":
                    st["map"][ty][tx] = " "
                    st["score"] += 1
                    st["log"] = "💥 Монстр разорван!"
                    hit = True
                    break
                elif st["map"][ty][tx] == "#":
                    break

        if not hit: st["log"] = "Выстрел в стену."
        st["dirty"] = True

    async def action_new(self, call):
        try:
            m = []
            for row in self.base_map:
                m.append(list(row.replace(".", " ")))

            self.sessions["doom_user"] = {
                "x": 2.0, "y": 2.0, "a": 0.0,
                "hp": 100, "ammo": 10, "score": 0,
                "log": "Добро пожаловать в Ад.",
                "last_render": 0, "last_ai": 0,
                "dirty": True, "running": True,
                "map": m
            }
            asyncio.create_task(self.game_loop(call))
        except Exception as e:
            err = traceback.format_exc()
            await call.edit(f"<b>CRITICAL ERROR:</b>\n<pre>{err}</pre>", reply_markup=[])

    async def action_cont(self, call):
        try:
            sv = self.db.get("Doom", "save", None)
            if sv:
                sv["running"] = True
                sv["dirty"] = True
                sv["log"] = "Игра загружена."
                self.sessions["doom_user"] = sv
                asyncio.create_task(self.game_loop(call))
            else:
                await call.edit("Нет сохранений! Запустите модуль заново.", reply_markup=[])
        except Exception as e:
            err = traceback.format_exc()
            await call.edit(f"<b>CRITICAL ERROR:</b>\n<pre>{err}</pre>", reply_markup=[])

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
        await call.edit("Игра завершена.", reply_markup=[])

    async def action_rot_l(self, call): self.update_player(0, 0, -0.4)
    async def action_rot_r(self, call): self.update_player(0, 0, 0.4)
    async def action_fw(self, call):
        a = self.sessions.get("doom_user", {}).get("a", 0)
        self.update_player(math.sin(a)*0.8, math.cos(a)*0.8, 0)
    async def action_bw(self, call):
        a = self.sessions.get("doom_user", {}).get("a", 0)
        self.update_player(-math.sin(a)*0.8, -math.cos(a)*0.8, 0)
    async def action_m_l(self, call):
        a = self.sessions.get("doom_user", {}).get("a", 0) - math.pi/2
        self.update_player(math.sin(a)*0.6, math.cos(a)*0.6, 0)
    async def action_m_r(self, call):
        a = self.sessions.get("doom_user", {}).get("a", 0) + math.pi/2
        self.update_player(math.sin(a)*0.6, math.cos(a)*0.6, 0)
