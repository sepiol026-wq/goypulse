# ====================================================================================================================
#   ██████╗  ██████╗ ██╗   ██╗███╗   ███╗ ██████╗ ██████╗ ██╗   ██╗██╗     ███████╗███████╗
#  ██╔════╝ ██╔═══██╗╚██╗ ██╔╝████╗ ████║██╔═══██╗██╔══██╗██║   ██║██║     ██╔════╝██╔════╝
#  ██║  ███╗██║   ██║ ╚████╔╝ ██╔████╔██║██║   ██║██║  ██║██║   ██║██║     █████╗  ███████╗
#  ██║   ██║██║   ██║  ╚██╔╝  ██║╚██╔╝██║██║   ██║██║  ██║██║   ██║██║     ██╔══╝  ╚════██║
#  ╚██████╔╝╚██████╔╝   ██║   ██║ ╚═╝ ██║╚██████╔╝██████╔╝╚██████╔╝███████╗███████╗███████║
#   ╚═════╝  ╚═════╝    ╚═╝   ╚═╝     ╚═╝ ╚═════╝ ╚═════╝  ╚═════╝ ╚══════╝╚══════╝╚══════╝
#
#   OFFICIAL USERNAMES: @goymodules | @samsepi0l_ovf
#   MODULE: keyscanner
#
#   THIS MODULE IS LICENSED UNDER GNU AGPLv3, PROTECTED AGAINST UNAUTHORIZED COPYING/RESALE,
#   AND ITS ORIGINAL AUTHORSHIP BELONGS TO @samsepi0l_ovf.
#   ALL OFFICIAL UPDATES, RELEASE NOTES, AND PATCHES ARE PUBLISHED IN THE TELEGRAM CHANNEL @goymodules.
# ====================================================================================================================
# meta banner: https://raw.githubusercontent.com/sepiol026-wq/GoyModules/refs/heads/main/assets/keyscanner.png
# meta developer: @GoyModules
# requires: aiohttp
__version__ = (1, 0)
import re
import aiohttp
import asyncio
import json
import io
from herokutl.types import Message
from .. import loader, utils

@loader.tds
class KeyScanner(loader.Module):
    """Spizdi ALL AI API KEYS in your chat"""
    
    strings = {
        "name": "KeyScanner",
        "scanning": "<tg-emoji emoji-id=5256025060942031560>🐢</tg-emoji> <b>Fast scanning via search...</b>\n<tg-emoji emoji-id=5253526631221307799>📂</tg-emoji> Searching up to {limit} messages per prefix.",
        "found": "<tg-emoji emoji-id=5255813619702049821>✅</tg-emoji> <b>Scan complete!</b>\n<tg-emoji emoji-id=5253877736207821121>🔥</tg-emoji> Valid keys found: <b>{valid_count}</b>\n<tg-emoji emoji-id=5253549669425882943>🔋</tg-emoji> Saved to database.",
        "auto_on": "<tg-emoji emoji-id=5253884483601442590>🔔</tg-emoji> Auto-scan <b>enabled</b> for this chat.",
        "auto_off": "<tg-emoji emoji-id=5253690110561494560>🔇</tg-emoji> Auto-scan <b>disabled</b> for this chat.",
        "db_stats": "<tg-emoji emoji-id=5256094480498436162>📦</tg-emoji> <b>Database:</b> {total} keys\n\n<tg-emoji emoji-id=5253952855185829086>⚙️</tg-emoji> <b>Management Menu:</b>",
        "stats": "<tg-emoji emoji-id=5253713110111365241>📍</tg-emoji> <b>Providers Stats:</b>\n{stats_text}",
        "exported": "<tg-emoji emoji-id=5256113064821926998>©</tg-emoji> <b>Keys exported to Saved Messages!</b>",
        "empty": "<tg-emoji emoji-id=5253864872780769235>❗️</tg-emoji> Database is empty.",
        "deleted": "<tg-emoji emoji-id=5255831443816327915>🗑</tg-emoji> Key removed.",
        "not_found": "<tg-emoji emoji-id=5253864872780769235>❗️</tg-emoji> Key not found.",
        "btn_export": "⬇️ Export",
        "btn_stats": "📍 Stats",
        "btn_clear": "🗑 Clear All",
        "btn_list": "📝 Key List",
        "btn_check_all": "🔃 Validate All",
        "btn_back": "⬅️ Back",
        "btn_exp_json": "📄 JSON",
        "btn_exp_txt": "📄 TXT",
        "btn_clr_inv": "🗑 Clear Invalid",
        "new_key_auto": "<tg-emoji emoji-id=5253884483601442590>🔔</tg-emoji> <b>Auto-caught key!</b>\nProvider: <b>{provider}</b>",
        "list_title": "<tg-emoji emoji-id=5256230583717079814>📝</tg-emoji> <b>Keys List (Page {page}/{total_pages}):</b>",
        "key_info": "<tg-emoji emoji-id=5253713110111365241>📍</tg-emoji> <b>Key Info:</b>\n\n<b>Provider:</b> {provider}\n<b>Key:</b> <code>{key}</code>",
        "btn_check_single": "🔃 Check Key",
        "btn_del_single": "🗑 Delete Key",
        "checking_all": "<tg-emoji emoji-id=5253464392850221514>🔃</tg-emoji> <b>Validating {total} keys...</b> Please wait.",
        "check_res_all": "<tg-emoji emoji-id=5255813619702049821>✅</tg-emoji> <b>Validation Complete</b>\n\n<b>Total:</b> {total}\n<b>Valid:</b> {v}\n<b>Invalid:</b> {i}\n\n<tg-emoji emoji-id=5253713110111365241>📍</tg-emoji> <b>Providers:</b>\n{prov_stats}",
        "check_res_single": "<tg-emoji emoji-id=5253464392850221514>🔃</tg-emoji> <b>Validation Result:</b>\n\n<b>Provider:</b> {provider}\n<b>Status:</b> {status}",
        "status_valid": "<tg-emoji emoji-id=5255813619702049821>✅</tg-emoji> Valid",
        "status_invalid": "<tg-emoji emoji-id=5253864872780769235>❗️</tg-emoji> Invalid",
        "importing": "<tg-emoji emoji-id=5253464392850221514>🔃</tg-emoji> <b>Importing keys...</b>",
        "imported": "<tg-emoji emoji-id=5255813619702049821>✅</tg-emoji> <b>Successfully imported {count} unique keys.</b>",
        "import_err": "<tg-emoji emoji-id=5253864872780769235>❗️</tg-emoji> Reply to a TXT/JSON file or provide a raw URL."
    }

    strings_ru = {
        "scanning": "<tg-emoji emoji-id=5256025060942031560>🐢</tg-emoji> <b>Быстрый поиск ключей...</b>\n<tg-emoji emoji-id=5253526631221307799>📂</tg-emoji> Поиск до {limit} сообщений на префикс.",
        "found": "<tg-emoji emoji-id=5255813619702049821>✅</tg-emoji> <b>Сканирование завершено!</b>\n<tg-emoji emoji-id=5253877736207821121>🔥</tg-emoji> Новых валидных ключей: <b>{valid_count}</b>\n<tg-emoji emoji-id=5253549669425882943>🔋</tg-emoji> Сохранено.",
        "auto_on": "<tg-emoji emoji-id=5253884483601442590>🔔</tg-emoji> Авто-ловля <b>включена</b>.",
        "auto_off": "<tg-emoji emoji-id=5253690110561494560>🔇</tg-emoji> Авто-ловля <b>выключена</b>.",
        "db_stats": "<tg-emoji emoji-id=5256094480498436162>📦</tg-emoji> <b>База ключей:</b> {total}\n\n<tg-emoji emoji-id=5253952855185829086>⚙️</tg-emoji> <b>Управление:</b>",
        "stats": "<tg-emoji emoji-id=5253713110111365241>📍</tg-emoji> <b>Статистика провайдеров:</b>\n{stats_text}",
        "exported": "<tg-emoji emoji-id=5256113064821926998>©</tg-emoji> <b>Ключи выгружены в Избранное!</b>",
        "empty": "<tg-emoji emoji-id=5253864872780769235>❗️</tg-emoji> База пуста.",
        "deleted": "<tg-emoji emoji-id=5255831443816327915>🗑</tg-emoji> Ключ удален.",
        "not_found": "<tg-emoji emoji-id=5253864872780769235>❗️</tg-emoji> Ключ не найден.",
        "btn_export": "⬇️ Выгрузить",
        "btn_stats": "📍 Статистика",
        "btn_clear": "🗑 Очистить все",
        "btn_list": "📝 Список",
        "btn_check_all": "🔃 Проверить все",
        "btn_back": "⬅️ Назад",
        "btn_exp_json": "📄 JSON",
        "btn_exp_txt": "📄 TXT",
        "btn_clr_inv": "🗑 Удалить невалид",
        "new_key_auto": "<tg-emoji emoji-id=5253884483601442590>🔔</tg-emoji> <b>Пойман новый ключ!</b>\nПровайдер: <b>{provider}</b>",
        "list_title": "<tg-emoji emoji-id=5256230583717079814>📝</tg-emoji> <b>Список (Стр. {page}/{total_pages}):</b>",
        "key_info": "<tg-emoji emoji-id=5253713110111365241>📍</tg-emoji> <b>Информация:</b>\n\n<b>Провайдер:</b> {provider}\n<b>Ключ:</b> <code>{key}</code>",
        "btn_check_single": "🔃 Проверить",
        "btn_del_single": "🗑 Удалить",
        "checking_all": "<tg-emoji emoji-id=5253464392850221514>🔃</tg-emoji> <b>Проверяю {total} ключей...</b>",
        "check_res_all": "<tg-emoji emoji-id=5255813619702049821>✅</tg-emoji> <b>Проверка завершена</b>\n\n<b>Всего:</b> {total}\n<b>Валидно:</b> {v}\n<b>Невалидно:</b> {i}\n\n<tg-emoji emoji-id=5253713110111365241>📍</tg-emoji> <b>Провайдеры:</b>\n{prov_stats}",
        "check_res_single": "<tg-emoji emoji-id=5253464392850221514>🔃</tg-emoji> <b>Результат проверки:</b>\n\n<b>Провайдер:</b> {provider}\n<b>Статус:</b> {status}",
        "status_valid": "<tg-emoji emoji-id=5255813619702049821>✅</tg-emoji> Валид",
        "status_invalid": "<tg-emoji emoji-id=5253864872780769235>❗️</tg-emoji> Невалид",
        "importing": "<tg-emoji emoji-id=5253464392850221514>🔃</tg-emoji> <b>Импорт ключей...</b>",
        "imported": "<tg-emoji emoji-id=5255813619702049821>✅</tg-emoji> <b>Успешно импортировано {count} новых ключей.</b>",
        "import_err": "<tg-emoji emoji-id=5253864872780769235>❗️</tg-emoji> Сделайте реплай на файл или укажите ссылку на raw."
    }

    strings_uk = {
        "db_stats": "<tg-emoji emoji-id=5256094480498436162>📦</tg-emoji> <b>База:</b> {total}\n\n<tg-emoji emoji-id=5253952855185829086>⚙️</tg-emoji> <b>Керування:</b>",
        "list_title": "<tg-emoji emoji-id=5256230583717079814>📝</tg-emoji> <b>Список (Стор. {page}/{total_pages}):</b>",
        "btn_export": "⬇️ Експорт",
        "btn_stats": "📍 Статистика",
        "btn_clear": "🗑 Очистити",
        "btn_list": "📝 Список",
        "btn_check_all": "🔃 Перевірити всі",
        "btn_back": "⬅️ Назад",
        "btn_clr_inv": "🗑 Видалити невалід",
        "imported": "<tg-emoji emoji-id=5255813619702049821>✅</tg-emoji> <b>Імпортовано: {count}</b>",
        "exported": "<tg-emoji emoji-id=5256113064821926998>©</tg-emoji> <b>Ключі вивантажені у Збережені!</b>",
        "import_err": "<tg-emoji emoji-id=5253864872780769235>❗️</tg-emoji> Реплай на файл або посилання."
    }

    strings_de = {
        "db_stats": "<tg-emoji emoji-id=5256094480498436162>📦</tg-emoji> <b>Datenbank:</b> {total}\n\n<tg-emoji emoji-id=5253952855185829086>⚙️</tg-emoji> <b>Verwaltung:</b>",
        "list_title": "<tg-emoji emoji-id=5256230583717079814>📝</tg-emoji> <b>Liste (Seite {page}/{total_pages}):</b>",
        "btn_back": "⬅️ Zurück"
    }

    strings_ja = {
        "db_stats": "<tg-emoji emoji-id=5256094480498436162>📦</tg-emoji> <b>データベース:</b> {total}\n\n<tg-emoji emoji-id=5253952855185829086>⚙️</tg-emoji> <b>管理:</b>",
        "list_title": "<tg-emoji emoji-id=5256230583717079814>📝</tg-emoji> <b>リスト (ページ {page}/{total_pages}):</b>",
        "btn_back": "⬅️ 戻る"
    }

    def __init__(self):
        self.key_regex = re.compile(r"\b(sk-[a-zA-Z0-9\-_]{20,}|sk-ant-api[a-zA-Z0-9\-_]{50,}|sk-or-v1-[a-zA-Z0-9]{40,}|AIza[0-9A-Za-z\-_]{35}|gsk_[a-zA-Z0-9]{20,})\b")
        self.search_queries = ["sk-", "AIza", "gsk_"]
        self._invalid_keys_cache = []

    async def client_ready(self, client, db):
        self.client = client
        self._keys = self.get("keys_v2", {})
        self._auto_chats = self.get("auto_v2", [])

    def _save(self):
        self.set("keys_v2", self._keys)
        self.set("auto_v2", self._auto_chats)

    def _get_main_markup(self):
        return [
            [
                {"text": self.strings["btn_list"], "callback": self.ks_list, "args": (0,)},
                {"text": self.strings["btn_check_all"], "callback": self.ks_val_all}
            ],
            [
                {"text": self.strings["btn_export"], "callback": self.ks_exp_menu},
                {"text": self.strings["btn_stats"], "callback": self.ks_stats}
            ],
            [
                {"text": self.strings["btn_clear"], "callback": self.ks_clr_all}
            ]
        ]

    async def _validate_key(self, session, key):
        headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
        try:
            if key.startswith("sk-or-v1-"):
                async with session.get("https://openrouter.ai/api/v1/auth/key", headers=headers, timeout=5) as r:
                    return "OpenRouter", r.status == 200
            elif key.startswith("gsk_"):
                async with session.get("https://api.groq.com/openai/v1/models", headers=headers, timeout=5) as r:
                    return "Groq", r.status == 200
            elif key.startswith("AIza"):
                async with session.get(f"https://generativelanguage.googleapis.com/v1beta/models?key={key}", timeout=5) as r:
                    return "Gemini", r.status == 200
            elif key.startswith("sk-ant-"):
                return "Anthropic", True 
            elif key.startswith("sk-"):
                async with session.get("https://api.openai.com/v1/models", headers=headers, timeout=5) as r:
                    if r.status == 200: return "OpenAI", True
                    return "OpenAI-Like", True
        except Exception:
            pass
        return "Unknown", False

    @loader.command(
        ru_doc="[лимит] - Поиск ключей через поиск сообщений.",
        en_doc="[limit] - Fast key scan via Telegram search."
    )
    async def scanllm(self, message: Message):
        args = utils.get_args_raw(message)
        limit = int(args) if args.isdigit() else 500
        
        msg = await utils.answer(message, self.strings["scanning"].format(limit=limit))
        found_keys = set()
        
        for query in self.search_queries:
            async for chat_msg in self.client.iter_messages(message.to_id, search=query, limit=limit):
                if getattr(chat_msg, "raw_text", None):
                    found_keys.update(self.key_regex.findall(chat_msg.raw_text))
        
        valid_count = 0
        if found_keys:
            async with aiohttp.ClientSession() as session:
                tasks = [self._validate_key(session, k) for k in found_keys]
                results = await asyncio.gather(*tasks)
                
                for key, (provider, is_valid) in zip(found_keys, results):
                    if is_valid and key not in self._keys:
                        self._keys[key] = provider
                        valid_count += 1
                        
            self._save()
        await utils.answer(msg, self.strings["found"].format(valid_count=valid_count))

    @loader.command(ru_doc="Вкл/выкл авто-ловлю", en_doc="Toggle auto-scan")
    async def autokeys(self, message: Message):
        chat_id = message.chat_id
        if chat_id in self._auto_chats:
            self._auto_chats.remove(chat_id)
            await utils.answer(message, self.strings["auto_off"])
        else:
            self._auto_chats.append(chat_id)
            await utils.answer(message, self.strings["auto_on"])
        self._save()

    @loader.command(
        ru_doc="<реплай/ссылка> - Импорт ключей",
        en_doc="<reply/link> - Import keys"
    )
    async def ksimport(self, message: Message):
        msg = await utils.answer(message, self.strings["importing"])
        text_data = ""
        reply = await message.get_reply_message()
        
        if reply and reply.file:
            try:
                dl = await self.client.download_media(reply, bytes)
                text_data = dl.decode('utf-8', errors='ignore')
            except Exception:
                pass
        else:
            args = utils.get_args_raw(message)
            if args.startswith("http"):
                try:
                    async with aiohttp.ClientSession() as sess:
                        async with sess.get(args) as r:
                            text_data = await r.text()
                except Exception:
                    pass
                    
        if not text_data:
            return await utils.answer(msg, self.strings["import_err"])
            
        found = self.key_regex.findall(text_data)
        count = 0
        if found:
            unique_keys = set(found)
            async with aiohttp.ClientSession() as session:
                tasks = [self._validate_key(session, k) for k in unique_keys]
                results = await asyncio.gather(*tasks)
                
                for key, (provider, is_valid) in zip(unique_keys, results):
                    if is_valid and key not in self._keys:
                        self._keys[key] = provider
                        count += 1
            self._save()
            
        await utils.answer(msg, self.strings["imported"].format(count=count))

    @loader.command(ru_doc="Меню ключей", en_doc="Keys menu")
    async def mykeys(self, message: Message):
        if not self._keys:
            return await utils.answer(message, self.strings["empty"])
            
        await self.inline.form(
            text=self.strings["db_stats"].format(total=len(self._keys)),
            message=message,
            reply_markup=self._get_main_markup()
        )

    @loader.watcher(only_messages=True)
    async def watcher(self, message: Message):
        if getattr(message, "chat_id", None) not in self._auto_chats or not getattr(message, "raw_text", None):
            return
            
        matches = self.key_regex.findall(message.raw_text)
        if not matches:
            return
            
        async with aiohttp.ClientSession() as session:
            for key in matches:
                if key in self._keys:
                    continue
                provider, is_valid = await self._validate_key(session, key)
                if is_valid:
                    self._keys[key] = provider
                    self._save()
                    await self.client.send_message(message.chat_id, self.strings["new_key_auto"].format(provider=provider))

    # сука модуль хуйня

    async def ks_list(self, call, page):
        keys_list = sorted(list(self._keys.keys()))
        per_page = 6
        total_pages = max(1, (len(keys_list) + per_page - 1) // per_page)
        if page < 0: page = total_pages - 1
        if page >= total_pages: page = 0
        
        start = page * per_page
        end = start + per_page
        current_keys = keys_list[start:end]
        
        markup = []
        for i, k in enumerate(current_keys):
            idx = start + i
            prov = self._keys[k]
            short_k = f"{k[:8]}...{k[-4:]}" if len(k) > 15 else k
            markup.append([{"text": f"[{prov}] {short_k}", "callback": self.ks_key_menu, "args": (idx,)}])
        
        nav_row = []
        if total_pages > 1:
            nav_row.append({"text": "◀️", "callback": self.ks_list, "args": (page-1,)})
            nav_row.append({"text": "▶️", "callback": self.ks_list, "args": (page+1,)})
        
        if nav_row: markup.append(nav_row)
        markup.append([{"text": self.strings["btn_back"], "callback": self.ks_back}])
        
        text = self.strings["list_title"].format(page=page+1, total_pages=total_pages)
        await call.edit(text=text, reply_markup=markup)

    async def ks_key_menu(self, call, idx):
        keys_list = sorted(list(self._keys.keys()))
        if idx >= len(keys_list): return
        k = keys_list[idx]
        p = self._keys[k]
        
        markup = [
            [
                {"text": self.strings["btn_check_single"], "callback": self.ks_val_single, "args": (idx,)},
                {"text": self.strings["btn_del_single"], "callback": self.ks_del_single, "args": (idx,)}
            ],
            [{"text": self.strings["btn_back"], "callback": self.ks_list, "args": (0,)}]
        ]
        await call.edit(text=self.strings["key_info"].format(provider=p, key=k), reply_markup=markup)

    async def ks_val_single(self, call, idx):
        keys_list = sorted(list(self._keys.keys()))
        if idx >= len(keys_list): return
        k = keys_list[idx]
        
        async with aiohttp.ClientSession() as session:
            prov, is_valid = await self._validate_key(session, k)
        
        status = self.strings["status_valid"] if is_valid else self.strings["status_invalid"]
        text = self.strings["check_res_single"].format(provider=prov, status=status)
        markup = [[{"text": self.strings["btn_back"], "callback": self.ks_key_menu, "args": (idx,)}]]
        await call.edit(text=text, reply_markup=markup)

    async def ks_del_single(self, call, idx):
        keys_list = sorted(list(self._keys.keys()))
        if idx < len(keys_list):
            k = keys_list[idx]
            if k in self._keys:
                del self._keys[k]
                self._save()
        await call.edit(text=self.strings["deleted"], reply_markup=[[{"text": self.strings["btn_back"], "callback": self.ks_list, "args": (0,)}]])

    async def ks_val_all(self, call):
        await call.edit(text=self.strings["checking_all"].format(total=len(self._keys)))
        
        keys_list = sorted(list(self._keys.keys()))
        valid_count = 0
        invalid_count = 0
        prov_stats = {}
        self._invalid_keys_cache.clear()
        
        async with aiohttp.ClientSession() as session:
            tasks = [self._validate_key(session, k) for k in keys_list]
            results = await asyncio.gather(*tasks)
            
            for k, (prov, is_valid) in zip(keys_list, results):
                if prov not in prov_stats:
                    prov_stats[prov] = {"total": 0, "valid": 0}
                prov_stats[prov]["total"] += 1
                
                if is_valid:
                    valid_count += 1
                    prov_stats[prov]["valid"] += 1
                    self._keys[k] = prov 
                else:
                    invalid_count += 1
                    self._invalid_keys_cache.append(k)
                    
        self._save()
        
        stats_str = ""
        for p, s in prov_stats.items():
            stats_str += f"<b>[{p}]:</b> {s['total']} | {s['valid']} valid\n"
            
        text = self.strings["check_res_all"].format(
            total=len(self._keys), v=valid_count, i=invalid_count, prov_stats=stats_str
        )
        
        markup = []
        if invalid_count > 0:
            markup.append([{"text": self.strings["btn_clr_inv"], "callback": self.ks_clr_inv}])
        markup.append([{"text": self.strings["btn_back"], "callback": self.ks_back}])
        
        await call.edit(text=text, reply_markup=markup)

    async def ks_clr_inv(self, call):
        for k in self._invalid_keys_cache:
            if k in self._keys:
                del self._keys[k]
        self._save()
        self._invalid_keys_cache.clear()
        await call.edit(text=self.strings["deleted"], reply_markup=[[{"text": self.strings["btn_back"], "callback": self.ks_back}]])

    async def ks_stats(self, call):
        providers = {}
        for p in self._keys.values():
            providers[p] = providers.get(p, 0) + 1
        stats_text = "\n".join([f"<b>{p}</b>: {c}" for p, c in providers.items()])
        await call.edit(
            text=self.strings["stats"].format(stats_text=stats_text),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self.ks_back}]]
        )

    async def ks_exp_menu(self, call):
        markup = [
            [
                {"text": self.strings["btn_exp_json"], "callback": self.ks_exp_json},
                {"text": self.strings["btn_exp_txt"], "callback": self.ks_exp_txt}
            ],
            [{"text": self.strings["btn_back"], "callback": self.ks_back}]
        ]
        await call.edit(text="<tg-emoji emoji-id=5255890718659979335>⬇️</tg-emoji> <b>Select export format:</b>", reply_markup=markup)

    async def ks_exp_json(self, call):
        file_data = io.BytesIO(json.dumps(self._keys, indent=4).encode('utf-8'))
        file_data.name = "keys_export.json"
        await self.client.send_file("me", file=file_data, caption="<tg-emoji emoji-id=5256113064821926998>©</tg-emoji> <b>Exported Keys</b>", parse_mode="html")
        await call.edit(
            text=self.strings["exported"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self.ks_back}]]
        )

    async def ks_exp_txt(self, call):
        lines = [f"{k} | {p}" for k, p in self._keys.items()]
        file_data = io.BytesIO("\n".join(lines).encode('utf-8'))
        file_data.name = "keys_export.txt"
        await self.client.send_file("me", file=file_data, caption="<tg-emoji emoji-id=5256113064821926998>©</tg-emoji> <b>Exported Keys</b>", parse_mode="html")
        await call.edit(
            text=self.strings["exported"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self.ks_back}]]
        )

    async def ks_clr_all(self, call):
        self._keys.clear()
        self._save()
        await call.edit(text=self.strings["empty"], reply_markup=[[{"text": self.strings["btn_back"], "callback": self.ks_back}]])

    async def ks_back(self, call):
        await call.edit(
            text=self.strings["db_stats"].format(total=len(self._keys)),
            reply_markup=self._get_main_markup()
        )
