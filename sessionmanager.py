# meta developer: @goymodules
# authors: @goymodules
# Description: Session Control Center for Heroku UserBot (heroku-tl-new/Telethon compatible)

import asyncio
import base64
import os
import sqlite3
import tempfile
import time
import uuid
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

from telethon.errors import FloodWaitError, SessionPasswordNeededError, UserDeactivatedBanError
from telethon.crypto.authkey import AuthKey
from telethon.sessions import StringSession
from telethon import TelegramClient
from telethon.tl.types import Message

from .. import loader, utils


STATUS_UNKNOWN = "unknown"
STATUS_VALID = "valid"
STATUS_INVALID = "invalid"
STATUS_SPAM_TEMP = "spam_temp"
STATUS_SPAM_PERM = "spam_perm"
STATUS_FROZEN = "frozen"

ALLOWED_STATUSES_FOR_ACTION = {STATUS_VALID}


@dataclass
class SessionRecord:
    sid: str
    label: str
    source_type: str
    session_string: str
    backend: str = "string"  # string | sqlite_b64
    state: str = STATUS_UNKNOWN
    cooldown_until: float = 0.0
    last_check_at: float = 0.0
    last_error: str = ""
    spam_blocked: bool = False
    frozen: bool = False


@dataclass
class TaskRecord:
    tid: str
    kind: str
    started_at: float
    state: str
    processed: int = 0
    success: int = 0
    failed: int = 0
    skipped: int = 0
    note: str = ""


@loader.tds
class SessionManagerMod(loader.Module):
    """Session Manager for Heroku UserBot + heroku-tl-new."""

    strings = {
        "name": "SessionManager",
        "need_api": "❌ Не удалось получить API credentials из клиента Heroku.",
        "import_on": "📥 Импорт включён на 10 минут. Отправляй string-сессии текстом/файлами. Потом жми <b>Finish Import</b> в панели.",
        "import_off": "✅ Импорт завершён. Принято: <code>{}</code>, добавлено: <code>{}</code>, дубликатов: <code>{}</code>, ошибок: <code>{}</code>",
        "panel_title": "🧩 <b>SessionManager</b>\n\nВсего: <code>{total}</code>\nValid: <code>{valid}</code> | Invalid: <code>{invalid}</code>\nTemp SB: <code>{temp}</code> | Perm SB: <code>{perm}</code> | Frozen: <code>{frozen}</code>",
        "validation_done": "✅ Валидация завершена. Проверено: <code>{}</code>",
        "no_sessions": "⚠️ Сессий пока нет.",
        "broadcast_started": "🚀 Задача рассылки запущена: <code>{}</code>",
        "task_stopped": "🛑 Задача остановлена: <code>{}</code>",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue("api_id_override", 0, "Override API ID (optional)", validator=loader.validators.Integer(minimum=0)),
            loader.ConfigValue("api_hash_override", "", "Override API HASH (optional)", validator=loader.validators.Hidden()),
            loader.ConfigValue("parallel", 5, "Concurrency for checks/actions", validator=loader.validators.Integer(minimum=1, maximum=100)),
        )
        self._sessions: List[SessionRecord] = []
        self._tasks: Dict[str, TaskRecord] = {}
        self._task_handles: Dict[str, asyncio.Task] = {}
        self._import_mode: bool = False
        self._import_deadline: float = 0.0
        self._import_payloads: List[Tuple[str, str]] = []
        self._owner_id: int = 0
        self._lock = asyncio.Lock()
        self._api_id: int = 0
        self._api_hash: str = ""

    async def client_ready(self, client, db):
        self._db = db
        self._client = client
        self._owner_id = (await client.get_me()).id
        self._api_id, self._api_hash = self._resolve_api_credentials()
        self._load_state()

    def _resolve_api_credentials(self) -> Tuple[int, str]:
        # Prefer explicit override; otherwise reuse main Heroku userbot client credentials.
        if int(self.config["api_id_override"] or 0) and str(self.config["api_hash_override"] or ""):
            return int(self.config["api_id_override"]), str(self.config["api_hash_override"])
        client = getattr(self, "_client", None)
        if not client:
            return 0, ""
        api_id = int(getattr(client, "api_id", 0) or getattr(client, "_api_id", 0) or 0)
        api_hash = str(getattr(client, "api_hash", "") or getattr(client, "_api_hash", "") or "")
        return api_id, api_hash

    def _load_state(self):
        raw = self._db.get(self.strings("name"), "sessions", None)
        if raw is None:
            # backward compatibility with old module key
            raw = self._db.get("SessionCenter", "sessions", [])
        self._sessions = []
        for item in raw:
            try:
                self._sessions.append(SessionRecord(**item))
            except Exception:
                continue

    def _save_state(self):
        self._db.set(self.strings("name"), "sessions", [asdict(s) for s in self._sessions])

    def _stats(self) -> Dict[str, int]:
        data = {
            "total": len(self._sessions),
            "valid": 0,
            "invalid": 0,
            "temp": 0,
            "perm": 0,
            "frozen": 0,
        }
        for s in self._sessions:
            if s.state == STATUS_VALID:
                data["valid"] += 1
            elif s.state == STATUS_INVALID:
                data["invalid"] += 1
            elif s.state == STATUS_SPAM_TEMP:
                data["temp"] += 1
            elif s.state == STATUS_SPAM_PERM:
                data["perm"] += 1
            elif s.state == STATUS_FROZEN:
                data["frozen"] += 1
        return data

    def _is_owner(self, message: Message) -> bool:
        return bool(getattr(message, "sender_id", 0) == self._owner_id)

    def _api_ready(self) -> bool:
        if not self._api_id or not self._api_hash:
            self._api_id, self._api_hash = self._resolve_api_credentials()
        return bool(self._api_id and self._api_hash)

    async def _render_panel(self, message_or_call):
        st = self._stats()
        text = self.strings("panel_title").format(**st)
        markup = [
            [{"text": "📥 Import", "callback": self._cb_import_on}, {"text": "✅ Finish Import", "callback": self._cb_import_finish}],
            [{"text": "🔎 Validate All", "callback": self._cb_validate_all}, {"text": "🧹 Delete Invalid", "callback": self._cb_delete_invalid}],
            [{"text": "📣 Broadcast", "callback": self._cb_broadcast_help}, {"text": "🧰 Active Tasks", "callback": self._cb_tasks}],
            [{"text": "♻️ Refresh", "callback": self._cb_refresh}],
        ]
        if isinstance(message_or_call, Message):
            await self.inline.form(text, message=message_or_call, reply_markup=markup)
            return
        try:
            await message_or_call.edit(text, reply_markup=markup)
        except TypeError:
            await self.inline.form(text, reply_markup=markup)

    async def _create_client(self, session_string: str):
        return TelegramClient(StringSession(session_string), int(self._api_id), self._api_hash)

    async def _create_client_for_record(self, rec: SessionRecord):
        if rec.backend == "sqlite_b64":
            raw = base64.b64decode(rec.session_string.encode("utf-8"))
            tmp = tempfile.NamedTemporaryFile(prefix="sc_", suffix=".session", delete=False)
            tmp.write(raw)
            tmp.flush()
            tmp.close()
            return TelegramClient(tmp.name, int(self._api_id), self._api_hash), tmp.name
        return TelegramClient(StringSession(rec.session_string), int(self._api_id), self._api_hash), None

    async def _quick_validate(self, rec: SessionRecord) -> SessionRecord:
        rec.last_check_at = time.time()
        rec.cooldown_until = 0
        rec.spam_blocked = False
        rec.frozen = False
        tmp_path = None
        try:
            client, tmp_path = await self._create_client_for_record(rec)
            await client.connect()
            if not await client.is_user_authorized():
                rec.state = STATUS_INVALID
                rec.last_error = "unauthorized"
            else:
                me = await client.get_me()
                if getattr(me, "restricted", False):
                    rec.state = STATUS_FROZEN
                    rec.frozen = True
                    rec.last_error = "account_restricted"
                else:
                    rec.state = STATUS_VALID
                    rec.last_error = ""
            await client.disconnect()
            if tmp_path:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
        except FloodWaitError as e:
            rec.state = STATUS_SPAM_TEMP
            rec.spam_blocked = True
            rec.cooldown_until = time.time() + int(getattr(e, "seconds", 60))
            rec.last_error = f"flood_wait_{getattr(e, 'seconds', 0)}"
        except (SessionPasswordNeededError, UserDeactivatedBanError) as e:
            rec.state = STATUS_FROZEN
            rec.frozen = True
            rec.last_error = e.__class__.__name__
        except Exception as e:
            low = str(e).lower()
            if "user_deactivated" in low or "banned" in low:
                rec.state = STATUS_SPAM_PERM
                rec.spam_blocked = True
            else:
                rec.state = STATUS_INVALID
            rec.last_error = str(e)[:200]
        finally:
            if tmp_path:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
        return rec

    def _accept_for_action(self, rec: SessionRecord) -> bool:
        if rec.state not in ALLOWED_STATUSES_FOR_ACTION:
            return False
        if rec.cooldown_until and rec.cooldown_until > time.time():
            return False
        return True

    def _normalize_candidate(self, raw: str) -> Optional[SessionRecord]:
        data = (raw or "").strip()
        if not data:
            return None
        if data.startswith("#"):
            return None
        for pref in ("telethon:", "herokutl:", "heroku-tl-new:"):
            if data.lower().startswith(pref):
                data = data[len(pref):].strip()
        if len(data) < 100:
            return None
        sid = uuid.uuid4().hex[:12]
        label = f"acc_{sid[:6]}"
        return SessionRecord(sid=sid, label=label, source_type="string", session_string=data)

    def _convert_sqlite_session_to_string(self, path: str) -> Tuple[Optional[str], str]:
        """
        Convert Telethon/HerokuTL sqlite *.session file to StringSession.
        """
        if not os.path.exists(path):
            return None, "file_not_found"
        try:
            conn = sqlite3.connect(path)
            cur = conn.cursor()
            cur.execute("SELECT dc_id, server_address, port, auth_key FROM sessions LIMIT 1")
            row = cur.fetchone()
            conn.close()
            if not row:
                return None, "no_sessions_row"
            dc_id, _, _, auth_key = row
            if not auth_key:
                return None, "auth_key_missing"
            ss = StringSession()
            ss.set_dc(int(dc_id), "149.154.167.50", 443)
            ss.auth_key = AuthKey(data=auth_key)
            out = ss.save()
            if not out:
                return None, "save_failed"
            return out, "ok"
        except Exception as e:
            return None, str(e)[:200]

    def _build_record_from_sqlite_file(self, path: str) -> Tuple[Optional[SessionRecord], str]:
        if not os.path.exists(path):
            return None, "file_not_found"
        try:
            with open(path, "rb") as f:
                raw = f.read()
            if not raw:
                return None, "empty_file"
            # Quick schema check for Telethon/HerokuTL sqlite session
            conn = sqlite3.connect(path)
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sessions'")
            has_sessions = cur.fetchone() is not None
            conn.close()
            if not has_sessions:
                return None, "not_telethon_session"
            sid = uuid.uuid4().hex[:12]
            record = SessionRecord(
                sid=sid,
                label=f"acc_{sid[:6]}",
                source_type="telethon_sqlite",
                session_string=base64.b64encode(raw).decode("utf-8"),
                backend="sqlite_b64",
            )
            return record, "ok"
        except Exception as e:
            return None, str(e)[:200]

    async def _read_text_file(self, path: str) -> str:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()

    async def _consume_import_payloads(self) -> Tuple[int, int, int, int]:
        parsed = 0
        added = 0
        dup = 0
        err = 0
        existing = {f"{getattr(s, 'backend', 'string')}:{s.session_string}" for s in self._sessions}
        for source, payload in self._import_payloads:
            if source == "sqlite_session_file":
                parsed += 1
                rec, reason = self._build_record_from_sqlite_file(payload)
                try:
                    os.remove(payload)
                except Exception:
                    pass
                if not rec:
                    err += 1
                    continue
                uniq_key = f"{rec.backend}:{rec.session_string}"
                if uniq_key in existing:
                    dup += 1
                    continue
                self._sessions.append(rec)
                existing.add(uniq_key)
                added += 1
                continue
            lines = [payload] if source == "text" else payload.splitlines()
            for line in lines:
                parsed += 1
                rec = self._normalize_candidate(line)
                if not rec:
                    err += 1
                    continue
                uniq_key = f"{rec.backend}:{rec.session_string}"
                if uniq_key in existing:
                    dup += 1
                    continue
                self._sessions.append(rec)
                existing.add(uniq_key)
                added += 1
        self._import_payloads.clear()
        self._save_state()
        return parsed, added, dup, err

    async def watcher(self, message: Message):
        if not self._import_mode:
            return
        if not self._is_owner(message):
            return
        if time.time() > self._import_deadline:
            self._import_mode = False
            self._import_payloads.clear()
            return
        if getattr(message, "raw_text", None):
            text = (message.raw_text or "").strip()
            if text and not text.startswith("."):
                self._import_payloads.append(("text", text))
                return
        if getattr(message, "file", None):
            try:
                path = await message.download_media(file="sessioncenter_import")
                if not path:
                    return
                file_name = (getattr(getattr(message, "file", None), "name", "") or "").lower()
                if path.endswith(".session") or file_name.endswith(".session"):
                    self._import_payloads.append(("sqlite_session_file", path))
                    return
                content = await self._read_text_file(path)
                self._import_payloads.append(("file", content))
                try:
                    os.remove(path)
                except Exception:
                    pass
            except Exception:
                return

    @loader.command(ru_doc="Конвертер: reply на .session/строку -> string session")
    async def scconvert(self, message: Message):
        if not self._api_ready():
            await utils.answer(message, self.strings("need_api"))
            return
        rep = await message.get_reply_message()
        if not rep:
            await utils.answer(message, "❌ Ответь на сообщение со строкой сессии или файлом <code>.session</code>.")
            return
        if getattr(rep, "file", None):
            path = await rep.download_media(file="sessioncenter_convert")
            if not path:
                await utils.answer(message, "❌ Не удалось скачать файл.")
                return
            conv, reason = self._convert_sqlite_session_to_string(path)
            try:
                os.remove(path)
            except Exception:
                pass
            if not conv:
                await utils.answer(message, f"❌ Конвертация не удалась: <code>{utils.escape_html(reason)}</code>")
                return
            await utils.answer(
                message,
                "✅ Конвертировано (Telethon/HerokuTL .session -> string):\n"
                f"<code>{utils.escape_html(conv)}</code>",
            )
            return

        raw = (rep.raw_text or "").strip()
        rec = self._normalize_candidate(raw)
        if not rec:
            await utils.answer(message, "❌ Это не похоже на string session.")
            return
        await utils.answer(message, f"✅ Detected string session:\n<code>{utils.escape_html(rec.session_string)}</code>")

    @loader.command(ru_doc="Открыть панель SessionManager")
    async def scpanel(self, message: Message):
        await self._render_panel(message)

    @loader.command(ru_doc="Включить режим импорта")
    async def scimport(self, message: Message):
        if not self._api_ready():
            await utils.answer(message, self.strings("need_api"))
            return
        self._import_mode = True
        self._import_deadline = time.time() + 600
        self._import_payloads.clear()
        await utils.answer(message, self.strings("import_on"))

    @loader.command(ru_doc="Завершить импорт и добавить сессии")
    async def scdone(self, message: Message):
        self._import_mode = False
        parsed, added, dup, err = await self._consume_import_payloads()
        await utils.answer(message, self.strings("import_off").format(parsed, added, dup, err))

    @loader.command(ru_doc="Провалидировать все сессии")
    async def scvalidate(self, message: Message):
        if not self._api_ready():
            await utils.answer(message, self.strings("need_api"))
            return
        if not self._sessions:
            await utils.answer(message, self.strings("no_sessions"))
            return
        sem = asyncio.Semaphore(int(self.config["parallel"]))

        async def run_one(rec: SessionRecord):
            async with sem:
                return await self._quick_validate(rec)

        self._sessions = await asyncio.gather(*(run_one(s) for s in self._sessions))
        self._save_state()
        await utils.answer(message, self.strings("validation_done").format(len(self._sessions)))

    @loader.command(ru_doc="Удалить invalid сессии")
    async def scdelinvalid(self, message: Message):
        before = len(self._sessions)
        self._sessions = [s for s in self._sessions if s.state != STATUS_INVALID]
        self._save_state()
        await utils.answer(message, f"🧹 Удалено invalid: <code>{before - len(self._sessions)}</code>")

    @loader.command(ru_doc="Рассылка: .scbroadcast <target> | <text> | <count|inf> | <delay_sec>")
    async def scbroadcast(self, message: Message):
        if not self._api_ready():
            await utils.answer(message, self.strings("need_api"))
            return
        args = utils.get_args_raw(message)
        parts = [x.strip() for x in args.split("|")]
        if len(parts) < 4:
            await utils.answer(message, "❌ Формат: <code>.scbroadcast target | text | count|inf | delay</code>")
            return

        target, text, count_raw, delay_raw = parts[:4]
        try:
            delay = max(0.0, float(delay_raw))
        except Exception:
            delay = 1.0

        infinite = count_raw.lower() in {"inf", "infinite", "∞"}
        count = 0 if infinite else max(1, int(count_raw))

        task_id = uuid.uuid4().hex[:10]
        rec = TaskRecord(tid=task_id, kind="broadcast", started_at=time.time(), state="running", note=f"target={target}")
        self._tasks[task_id] = rec

        async def runner():
            try:
                loops = 0
                while True:
                    if self._tasks[task_id].state != "running":
                        break
                    loops += 1
                    if not infinite and loops > count:
                        break
                    sem = asyncio.Semaphore(int(self.config["parallel"]))

                    async def send_one(srec: SessionRecord):
                        if not self._accept_for_action(srec):
                            self._tasks[task_id].skipped += 1
                            return
                        async with sem:
                            self._tasks[task_id].processed += 1
                            try:
                                client, tmp_path = await self._create_client_for_record(srec)
                                await client.connect()
                                await client.send_message(target, text)
                                await client.disconnect()
                                if tmp_path:
                                    try:
                                        os.remove(tmp_path)
                                    except Exception:
                                        pass
                                self._tasks[task_id].success += 1
                            except FloodWaitError as e:
                                srec.state = STATUS_SPAM_TEMP
                                srec.cooldown_until = time.time() + int(getattr(e, "seconds", 60))
                                srec.last_error = f"flood_wait_{getattr(e, 'seconds', 0)}"
                                self._tasks[task_id].failed += 1
                            except Exception as e:
                                srec.last_error = str(e)[:200]
                                if "deactivated" in srec.last_error.lower() or "banned" in srec.last_error.lower():
                                    srec.state = STATUS_SPAM_PERM
                                self._tasks[task_id].failed += 1

                    await asyncio.gather(*(send_one(s) for s in self._sessions))
                    self._save_state()
                    if delay > 0:
                        await asyncio.sleep(delay)

                if self._tasks[task_id].state == "running":
                    self._tasks[task_id].state = "finished"
            except Exception as e:
                self._tasks[task_id].state = "error"
                self._tasks[task_id].note = str(e)[:200]

        self._task_handles[task_id] = asyncio.create_task(runner())
        await utils.answer(message, self.strings("broadcast_started").format(task_id))

    @loader.command(ru_doc="Показать активные задачи")
    async def sctasks(self, message: Message):
        if not self._tasks:
            await utils.answer(message, "🧰 Задач нет")
            return
        lines = ["🧰 <b>Active Tasks</b>"]
        for t in self._tasks.values():
            lines.append(
                f"• <code>{t.tid}</code> [{t.kind}] {t.state} | ok={t.success} fail={t.failed} skip={t.skipped} proc={t.processed}"
            )
        await utils.answer(message, "\n".join(lines))

    @loader.command(ru_doc="Остановить задачу: .scstop <task_id>")
    async def scstop(self, message: Message):
        tid = utils.get_args_raw(message).strip()
        if not tid or tid not in self._tasks:
            await utils.answer(message, "❌ Укажи валидный task_id")
            return
        self._tasks[tid].state = "stopped"
        handle = self._task_handles.get(tid)
        if handle and not handle.done():
            handle.cancel()
        await utils.answer(message, self.strings("task_stopped").format(tid))

    async def _cb_refresh(self, call):
        await self._render_panel(call)

    async def _cb_import_on(self, call):
        self._import_mode = True
        self._import_deadline = time.time() + 600
        self._import_payloads.clear()
        await call.answer("Импорт включен", show_alert=False)
        await self._render_panel(call)

    async def _cb_import_finish(self, call):
        self._import_mode = False
        parsed, added, dup, err = await self._consume_import_payloads()
        await call.answer(f"done: {added} added", show_alert=False)
        await call.edit(self.strings("import_off").format(parsed, added, dup, err))

    async def _cb_validate_all(self, call):
        if not self._sessions:
            await call.answer("Нет сессий", show_alert=True)
            return
        sem = asyncio.Semaphore(int(self.config["parallel"]))

        async def run_one(rec: SessionRecord):
            async with sem:
                return await self._quick_validate(rec)

        self._sessions = await asyncio.gather(*(run_one(s) for s in self._sessions))
        self._save_state()
        await call.answer("Validation done", show_alert=False)
        await self._render_panel(call)

    async def _cb_delete_invalid(self, call):
        before = len(self._sessions)
        self._sessions = [s for s in self._sessions if s.state != STATUS_INVALID]
        self._save_state()
        await call.answer(f"Deleted {before - len(self._sessions)}", show_alert=False)
        await self._render_panel(call)

    async def _cb_broadcast_help(self, call):
        await call.edit(
            "📣 <b>Broadcast</b>\n"
            "Запуск из команды:\n"
            "<code>.scbroadcast target | text | count|inf | delay</code>\n\n"
            "Пример:\n"
            "<code>.scbroadcast @chatusername | test msg | 5 | 2</code>"
        )

    async def _cb_tasks(self, call):
        if not self._tasks:
            await call.edit("🧰 Активных задач нет")
            return
        lines = ["🧰 <b>Active Tasks</b>"]
        for t in self._tasks.values():
            lines.append(
                f"• <code>{t.tid}</code> [{t.kind}] {t.state} | ok={t.success} fail={t.failed} skip={t.skipped} proc={t.processed}"
            )
        await call.edit("\n".join(lines))
