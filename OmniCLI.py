# ====================================================================================================================
#   ██████╗ ███╗   ███╗███╗   ██╗██╗ ██████╗██╗     ██╗
#  ██╔═══██╗████╗ ████║████╗  ██║██║██╔════╝██║     ██║
#  ██║   ██║██╔████╔██║██╔██╗ ██║██║██║     ██║     ██║
#  ██║   ██║██║╚██╔╝██║██║╚██╗██║██║██║     ██║     ██║
#  ╚██████╔╝██║ ╚═╝ ██║██║ ╚████║██║╚██████╗███████╗██║
#   ╚═════╝ ╚═╝     ╚═╝╚═╝  ╚═══╝╚═╝ ╚═════╝╚══════╝╚═╝
#
#   OFFICIAL USERNAMES: @goymodules | @samsepi0l_ovf
#   MODULE: OmniCLI
#
#   THIS MODULE IS LICENSED UNDER GNU AGPLv3, PROTECTED AGAINST UNAUTHORIZED COPYING/RESALE,
#   AND ITS ORIGINAL AUTHORSHIP BELONGS TO @samsepi0l_ovf.
#   ALL OFFICIAL UPDATES, RELEASE NOTES, AND PATCHES ARE PUBLISHED IN THE TELEGRAM CHANNEL @goymodules.
# ====================================================================================================================

# requires: telethon
# meta developer: @goymodules
# authors: @goymodules
# Description: Unified multi-provider coding AI CLI for Heroku/Hikka.
# meta banner: https://raw.githubusercontent.com/sepiol026-wq/goypulse/main/assets/QwenCLI.png

__version__ = (1, 0, 0)

import asyncio
import contextlib
import logging
import os
import shutil
import tempfile
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from telethon.tl.types import Message

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

DB_HISTORY_KEY = "omnicli_history_v1"
DB_PROVIDER_KEY = "omnicli_provider"
DB_MODEL_KEY = "omnicli_model"
DB_SYSTEM_PROMPT_KEY = "omnicli_system_prompt"

DEFAULT_TIMEOUT = 420
DEFAULT_MAX_TURNS = 12


@dataclass
class ProviderSpec:
    key: str
    title: str
    binary: str
    install_hint: str
    env_keys: Tuple[str, ...]
    supports_model: bool
    non_interactive_flags: Tuple[str, ...]
    continue_flags: Tuple[str, ...]
    extra_flags: Tuple[str, ...]


PROVIDERS: Dict[str, ProviderSpec] = {
    "qwen": ProviderSpec(
        key="qwen",
        title="Qwen CLI",
        binary="qwen",
        install_hint="npm i -g @qwen-code/qwen-code",
        env_keys=("QWEN_API_KEY",),
        supports_model=True,
        non_interactive_flags=("-p",),
        continue_flags=("-c",),
        extra_flags=(),
    ),
    "codex": ProviderSpec(
        key="codex",
        title="OpenAI Codex CLI",
        binary="codex",
        install_hint="npm i -g @openai/codex",
        env_keys=("OPENAI_API_KEY", "CODEX_API_KEY"),
        supports_model=True,
        non_interactive_flags=("exec", "--skip-git-repo-check"),
        continue_flags=(),
        extra_flags=(),
    ),
    "gemini": ProviderSpec(
        key="gemini",
        title="Google Gemini CLI",
        binary="gemini",
        install_hint="npm i -g @google/gemini-cli",
        env_keys=("GEMINI_API_KEY",),
        supports_model=True,
        non_interactive_flags=("-p",),
        continue_flags=("-c",),
        extra_flags=(),
    ),
    "claude": ProviderSpec(
        key="claude",
        title="Claude Code",
        binary="claude",
        install_hint="npm i -g @anthropic-ai/claude-code",
        env_keys=("ANTHROPIC_API_KEY",),
        supports_model=True,
        non_interactive_flags=("-p",),
        continue_flags=("-c",),
        extra_flags=(),
    ),
}


@loader.tds
class OmniCLI(loader.Module):
    """OmniCLI: один интерфейс для Qwen/Codex/Gemini/Claude CLI"""

    strings = {
        "name": "OmniCLI",
        "processing": "<tg-emoji emoji-id=5255971360965930740>🕔</tg-emoji> <b>Обработка...</b>",
        "provider_set": "<tg-emoji emoji-id=5330561907671727296>✅</tg-emoji> <b>Провайдер OmniCLI:</b> <code>{}</code>",
        "provider_unknown": "<tg-emoji emoji-id=5332431395266524007>❗️</tg-emoji> <b>Неизвестный провайдер:</b> <code>{}</code>",
        "provider_missing": "<tg-emoji emoji-id=5332431395266524007>❗️</tg-emoji> <b>Бинарник не найден:</b> <code>{}</code>\n<i>{}</i>",
        "auth_missing": "<tg-emoji emoji-id=5332431395266524007>❗️</tg-emoji> <b>Похоже, не настроена авторизация.</b>\nПроверь ENV: <code>{}</code>",
        "request_failed": "<tg-emoji emoji-id=5350470691701407492>⛔</tg-emoji> <b>Ошибка запроса:</b>\n<code>{}</code>",
        "timeout": "<tg-emoji emoji-id=5350470691701407492>⛔</tg-emoji> <b>Таймаут выполнения ({0}с).</b>",
        "response": "<tg-emoji emoji-id=5256230583717079814>📝</tg-emoji> <b>{}</b>\n\n{}",
        "providers": "<tg-emoji emoji-id=5256079005731271025>📟</tg-emoji> <b>Провайдеры OmniCLI:</b>\n{}",
        "provider_line": "• <code>{}</code> — {}",
        "model_set": "<tg-emoji emoji-id=5330561907671727296>✅</tg-emoji> <b>Модель:</b> <code>{}</code>",
        "model_show": "<tg-emoji emoji-id=5956561916573782596>📝</tg-emoji> <b>Текущая модель:</b> <code>{}</code>",
        "prompt_set": "<tg-emoji emoji-id=5330561907671727296>✅</tg-emoji> <b>Системный промпт сохранён.</b>",
        "prompt_cleared": "<tg-emoji emoji-id=5370872568041471196>🗑</tg-emoji> <b>Системный промпт очищен.</b>",
        "prompt_show": "<tg-emoji emoji-id=5956561916573782596>📝</tg-emoji> <b>Системный промпт:</b>\n\n{}",
        "history_cleared": "<tg-emoji emoji-id=6007942490076745785>🧹</tg-emoji> <b>Память очищена.</b>",
        "help": (
            "<b>OmniCLI команды:</b>\n"
            "• <code>.om &lt;запрос&gt;</code> — отправить запрос в выбранный CLI\n"
            "• <code>.omprovider [qwen|codex|gemini|claude]</code> — выбрать провайдера\n"
            "• <code>.omproviders</code> — список провайдеров\n"
            "• <code>.ommodel [model]</code> — показать/установить модель\n"
            "• <code>.omprompt [text|-c]</code> — системный промпт\n"
            "• <code>.omclear</code> — очистить память чата\n"
            "• <code>.omauth</code> — статус авторизации/бинарника\n"
            "• <code>.ompatch &lt;fix&gt;</code> — исправить прошлый ответ"
        ),
        "usage_patch": "<b>Использование:</b> <code>.ompatch &lt;что исправить&gt;</code>",
        "usage_om": "<b>Использование:</b> <code>.om &lt;запрос&gt;</code>",
        "auth_ok": "<tg-emoji emoji-id=5330561907671727296>✅</tg-emoji> <b>{}</b>\nБинарник: <code>{}</code>\nENV: <code>{}</code>",
        "btn_clear": "🗑 Очистить",
        "btn_regen": "🔃 Другой ответ",
        "btn_retry": "🔁 Повтор",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue("omni_path", "", "Путь к бинарнику Omni/CLI (опционально)", validator=loader.validators.String()),
            loader.ConfigValue("default_provider", "codex", "Провайдер по умолчанию", validator=loader.validators.Choice(list(PROVIDERS.keys()))),
            loader.ConfigValue("request_timeout", DEFAULT_TIMEOUT, "Таймаут запроса (сек.)", validator=loader.validators.Integer(minimum=30, maximum=1800)),
            loader.ConfigValue("max_history", DEFAULT_MAX_TURNS, "Сколько пар хранить в памяти чата", validator=loader.validators.Integer(minimum=0, maximum=60)),
            loader.ConfigValue("show_buttons", True, "Показывать кнопки действий", validator=loader.validators.Boolean()),
        )
        self._chat_locks: Dict[int, asyncio.Lock] = {}
        self._last_query: Dict[int, str] = {}

    def _db_get(self, key, default=None):
        return self._db.get(self.strings("name"), key, default)

    def _db_set(self, key, value):
        self._db.set(self.strings("name"), key, value)

    def _get_provider(self) -> str:
        saved = self._db_get(DB_PROVIDER_KEY)
        if saved in PROVIDERS:
            return saved
        default = self.config["default_provider"]
        return default if default in PROVIDERS else "codex"

    def _get_model(self) -> str:
        return self._db_get(DB_MODEL_KEY, "") or ""

    def _get_prompt(self) -> str:
        return self._db_get(DB_SYSTEM_PROMPT_KEY, "") or ""

    def _resolve_binary(self, provider: ProviderSpec) -> Optional[str]:
        explicit = (self.config["omni_path"] or "").strip()
        if explicit:
            return explicit if os.path.isfile(explicit) else None
        return shutil.which(provider.binary)

    def _has_auth(self, provider: ProviderSpec) -> bool:
        return any(os.environ.get(k) for k in provider.env_keys)

    def _chat_history(self) -> Dict[str, List[Dict[str, str]]]:
        return self._db_get(DB_HISTORY_KEY, {}) or {}

    def _save_chat_history(self, history):
        self._db_set(DB_HISTORY_KEY, history)

    def _build_prompt(self, chat_id: int, query: str) -> str:
        prompt_parts = []
        system_prompt = self._get_prompt().strip()
        if system_prompt:
            prompt_parts.append(f"[SYSTEM]\n{system_prompt}\n")
        history = self._chat_history().get(str(chat_id), [])
        if history:
            prompt_parts.append("[HISTORY]")
            for item in history:
                role = item.get("role", "user").upper()
                content = item.get("content", "")
                prompt_parts.append(f"{role}: {content}")
            prompt_parts.append("")
        prompt_parts.append(f"[USER]\n{query}")
        return "\n".join(prompt_parts)

    def _append_history(self, chat_id: int, user_text: str, answer_text: str):
        history = self._chat_history()
        key = str(chat_id)
        entries = history.get(key, [])
        entries.append({"role": "user", "content": user_text.strip()})
        entries.append({"role": "assistant", "content": answer_text.strip()})
        limit = self.config["max_history"]
        if limit > 0:
            entries = entries[-(limit * 2):]
        history[key] = entries
        self._save_chat_history(history)

    async def _run_cli(self, provider: ProviderSpec, query: str, chat_id: int) -> Tuple[bool, str]:
        binary = self._resolve_binary(provider)
        if not binary:
            return False, self.strings("provider_missing").format(provider.binary, provider.install_hint)
        if not self._has_auth(provider):
            return False, self.strings("auth_missing").format(", ".join(provider.env_keys)
            )

        model = self._get_model().strip()
        packed_prompt = self._build_prompt(chat_id, query)

        cmd = [binary]
        cmd.extend(provider.non_interactive_flags)

        if provider.key == "codex":
            if model:
                cmd.extend(["-m", model])
            cmd.append(packed_prompt)
        else:
            if model and provider.supports_model:
                cmd.extend(["-m", model])
            cmd.append(packed_prompt)

        env = os.environ.copy()
        tmpdir = tempfile.mkdtemp(prefix="omnicli_")
        env["TMPDIR"] = tmpdir

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=self.config["request_timeout"])
            except asyncio.TimeoutError:
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()
                return False, self.strings("timeout").format(self.config["request_timeout"])

            out = (stdout or b"").decode("utf-8", errors="ignore").strip()
            err = (stderr or b"").decode("utf-8", errors="ignore").strip()
            if proc.returncode != 0:
                return False, err[-1800:] or "Unknown CLI error"
            if not out:
                return False, err[-1800:] or "Empty response from provider"
            return True, out[-12000:]
        finally:
            with contextlib.suppress(Exception):
                shutil.rmtree(tmpdir, ignore_errors=True)

    async def _send_result(self, message: Message, provider: ProviderSpec, query: str, answer_text: str):
        chat_id = utils.get_chat_id(message)
        self._last_query[chat_id] = query
        self._append_history(chat_id, query, answer_text)
        text = self.strings("response").format(provider.title, utils.escape_html(answer_text))
        if self.config["show_buttons"]:
            markup = [
                [
                    {"text": self.strings("btn_regen"), "callback": self._btn_regen, "args": (chat_id,)},
                    {"text": self.strings("btn_retry"), "callback": self._btn_retry, "args": (chat_id,)},
                ],
                [
                    {"text": self.strings("btn_clear"), "callback": self._btn_clear, "args": (chat_id,)},
                ],
            ]
            await self.inline.form(text, message=message, reply_markup=markup, disable_security=True)
            return
        await utils.answer(message, text)

    async def _process_query(self, message: Message, query: str):
        chat_id = utils.get_chat_id(message)
        lock = self._chat_locks.setdefault(chat_id, asyncio.Lock())
        if lock.locked():
            return await utils.answer(message, "<b>В этом чате уже есть активный запрос.</b>")

        provider = PROVIDERS[self._get_provider()]
        status = await utils.answer(message, self.strings("processing"))
        async with lock:
            ok, payload = await self._run_cli(provider, query, chat_id)
            if not ok:
                return await utils.answer(status, self.strings("request_failed").format(utils.escape_html(payload)))
            await self._send_result(status, provider, query, payload)

    async def _btn_clear(self, call: InlineCall, chat_id: int):
        history = self._chat_history()
        history.pop(str(chat_id), None)
        self._save_chat_history(history)
        with contextlib.suppress(Exception):
            await call.answer("Memory cleared")
        await call.edit(self.strings("history_cleared"), reply_markup=None)

    async def _btn_retry(self, call: InlineCall, chat_id: int):
        query = self._last_query.get(chat_id)
        if not query:
            return await call.answer("No last query", show_alert=True)
        msg = await self._client.send_message(chat_id, f"<code>.om {utils.escape_html(query)}</code>", parse_mode="html")
        await self._process_query(msg, query)

    async def _btn_regen(self, call: InlineCall, chat_id: int):
        query = self._last_query.get(chat_id)
        if not query:
            return await call.answer("No last query", show_alert=True)
        with contextlib.suppress(Exception):
            await call.answer("Regenerating")
        await self._process_query(await call.get_message(), query)

    @loader.command(ru_doc="<запрос> - выполнить запрос через OmniCLI")
    async def omcmd(self, message: Message):
        query = utils.get_args_raw(message)
        if not query:
            return await utils.answer(message, self.strings("usage_om"))
        await self._process_query(message, query)

    @loader.command(ru_doc="[provider] - выбрать/показать провайдера OmniCLI")
    async def omprovidercmd(self, message: Message):
        arg = utils.get_args_raw(message).strip().lower()
        if not arg:
            return await utils.answer(message, self.strings("provider_set").format(self._get_provider()))
        if arg not in PROVIDERS:
            return await utils.answer(message, self.strings("provider_unknown").format(arg))
        self._db_set(DB_PROVIDER_KEY, arg)
        await utils.answer(message, self.strings("provider_set").format(arg))

    @loader.command(ru_doc="показать список поддерживаемых провайдеров")
    async def omproviderscmd(self, message: Message):
        lines = [self.strings("provider_line").format(key, spec.title) for key, spec in PROVIDERS.items()]
        await utils.answer(message, self.strings("providers").format("\n".join(lines)))

    @loader.command(ru_doc="[model] - показать/установить модель")
    async def ommodelcmd(self, message: Message):
        arg = utils.get_args_raw(message).strip()
        if not arg:
            model = self._get_model() or "auto"
            return await utils.answer(message, self.strings("model_show").format(model))
        self._db_set(DB_MODEL_KEY, arg)
        await utils.answer(message, self.strings("model_set").format(utils.escape_html(arg)))

    @loader.command(ru_doc="[текст|-c] - системный промпт")
    async def ompromptcmd(self, message: Message):
        arg = utils.get_args_raw(message)
        if not arg:
            prompt = self._get_prompt()
            if not prompt:
                prompt = "(пусто)"
            return await utils.answer(message, self.strings("prompt_show").format(utils.escape_html(prompt)))
        if arg.strip() == "-c":
            self._db_set(DB_SYSTEM_PROMPT_KEY, "")
            return await utils.answer(message, self.strings("prompt_cleared"))
        self._db_set(DB_SYSTEM_PROMPT_KEY, arg)
        await utils.answer(message, self.strings("prompt_set"))

    @loader.command(ru_doc="очистить историю текущего чата")
    async def omclearcmd(self, message: Message):
        history = self._chat_history()
        history.pop(str(utils.get_chat_id(message)), None)
        self._save_chat_history(history)
        await utils.answer(message, self.strings("history_cleared"))

    @loader.command(ru_doc="проверить бинарник и авторизацию выбранного провайдера")
    async def omauthcmd(self, message: Message):
        provider = PROVIDERS[self._get_provider()]
        binary = self._resolve_binary(provider)
        env_line = ", ".join(provider.env_keys)
        if not binary:
            return await utils.answer(message, self.strings("provider_missing").format(provider.binary, provider.install_hint))
        if not self._has_auth(provider):
            return await utils.answer(message, self.strings("auth_missing").format(env_line))
        await utils.answer(message, self.strings("auth_ok").format(provider.title, binary, env_line))

    @loader.command(ru_doc="показать help по OmniCLI")
    async def omhelpcmd(self, message: Message):
        await utils.answer(message, self.strings("help"))

    @loader.command(ru_doc="<что исправить> - patch-запрос по последнему ответу")
    async def ompatchcmd(self, message: Message):
        patch = utils.get_args_raw(message).strip()
        if not patch:
            return await utils.answer(message, self.strings("usage_patch"))
        chat_id = utils.get_chat_id(message)
        history = self._chat_history().get(str(chat_id), [])
        if not history:
            return await utils.answer(message, "<b>Нет истории для patch-запроса.</b>")
        last_assistant = ""
        for entry in reversed(history):
            if entry.get("role") == "assistant":
                last_assistant = entry.get("content", "")
                break
        query = f"Исправь прошлый ответ с учётом требования:\n{patch}\n\nПрошлый ответ:\n{last_assistant}"
        await self._process_query(message, query)
