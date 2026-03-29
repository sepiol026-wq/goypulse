# requires: telethon pytz markdown-it-py
# meta developer: @samsepi0l_ovf
# authors: @goy_ai
# Description: QwenCLI — продвинутый интерфейс для Qwen CLI с поддержкой AI-анализа и авто-ответов.
# meta banner: https://raw.githubusercontent.com/sepiol026-wq/goypulse/main/banner.png
__version__ = (1, 0, 0)

import asyncio
import contextlib
import io
import json
import logging
import os
import platform
import random
import re
import shutil
import signal
import stat
import tarfile
import tempfile
import uuid
import zipfile
from datetime import datetime
from hashlib import sha256
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

try:
    import pytz
except ImportError:
    pytz = None

try:
    from markdown_it import MarkdownIt
except ImportError:
    MarkdownIt = None

from telethon import types as tg_types
from telethon.errors.rpcerrorlist import (
    ChannelPrivateError,
    ChatAdminRequiredError,
    UserNotParticipantError,
)
from telethon.tl.types import (
    DocumentAttributeFilename,
    DocumentAttributeSticker,
    Message,
)
from telethon.utils import get_display_name, get_peer_id

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

DB_HISTORY_KEY = "qwencli_conversations_v1"
DB_GAUTO_HISTORY_KEY = "qwencli_auto_conversations_v1"
DB_IMPERSONATION_KEY = "qwencli_impersonation_chats"
DB_PRESETS_KEY = "qwencli_prompt_presets"
DB_MEMORY_DISABLED_KEY = "qwencli_memory_disabled_chats"

QWEN_TIMEOUT = 300

TEXT_MIME_TYPES = {
    "text/plain",
    "text/markdown",
    "text/html",
    "text/css",
    "text/csv",
    "application/json",
    "application/xml",
    "application/x-python",
    "text/x-python",
    "application/javascript",
    "application/x-sh",
}


class QwenCLI(loader.Module):
    """Qwen CLI для Heroku"""

    strings = {
        "name": "QwenCLI",
        "cfg_qwen_path_doc": "Путь до бинарника qwen. При необходимости укажите полный путь.",
        "cfg_qwen_model_doc": "Модель Qwen CLI. Для Qwen OAuth обычно: coder-model или vision-model.",
        "cfg_auth_type_doc": "Тип авторизации для Qwen CLI: qwen-oauth.",
        "cfg_buttons_doc": "Включить интерактивные кнопки.",
        "cfg_system_instruction_doc": "Системный промпт для Qwen CLI.",
        "cfg_max_history_length_doc": "Макс. число пар вопрос-ответ в памяти. 0 — без лимита.",
        "cfg_timezone_doc": "Ваш часовой пояс.",
        "cfg_proxy_doc": "Прокси для Qwen CLI. Формат: http://user:pass@host:port",
        "cfg_auto_reply_chats_doc": "Чаты для авто-ответа. IDs или @username через запятую/новую строку.",
        "cfg_memory_disabled_chats_doc": "Чаты, где память отключена. IDs или @username через запятую/новую строку.",
        "cfg_impersonation_prompt_doc": "Промпт для режима авто-ответа. {my_name} и {chat_history} будут заменены.",
        "cfg_impersonation_history_limit_doc": "Сколько последних сообщений из чата отправлять как контекст для авто-ответа.",
        "cfg_impersonation_reply_chance_doc": "Вероятность ответа в режиме авто-ответа.",
        "cfg_inline_pagination_doc": "Использовать инлайн-пагинацию для длинных ответов.",
        "cfg_chat_recording_doc": "Разрешить Qwen CLI сохранять свои session records в runtime-home.",
        "cfg_approval_mode_doc": "Режим подтверждений Qwen CLI.",
        "cfg_max_concurrent_requests_doc": "Максимум одновременно выполняемых Qwen CLI запросов.",
        "cfg_auto_bootstrap_doc": "Автоматически пытаться установить локальные Node.js и Qwen CLI в user-space при отсутствии бинарника.",
        "qwen_not_found": "<tg-emoji emoji-id=5332431395266524007>❗️</tg-emoji> <b>Команда <code>qwen</code> не найдена в системе.</b>\nПроверьте PATH или заполните <code>qwen_path</code> в cfg.",
        "qwen_auth_missing": "<tg-emoji emoji-id=5332431395266524007>❗️</tg-emoji> <b>Qwen CLI не готов к работе.</b>\nНастройте авторизацию.",
        "qwen_oauth_missing": "<tg-emoji emoji-id=5332431395266524007>❗️</tg-emoji> <b>Qwen OAuth не настроен.</b>\nЗапустите <code>.qwauth qwen</code> и подтвердите вход в браузере.",
        "processing": "<tg-emoji emoji-id=5332688668102525212>⌛️</tg-emoji> <b>Обработка...</b>",
        "queue_wait": "<tg-emoji emoji-id=5415941463764667665>⏳</tg-emoji> <b>Ожидаю свободный слот выполнения...</b>",
        "bootstrap_wait": "<tg-emoji emoji-id=5415941463764667665>⏳</tg-emoji> <b>Подготавливаю локальный Qwen CLI runtime...</b>",
        "request_busy_same_chat": "<tg-emoji emoji-id=5409235172979672859>⚠️</tg-emoji> <b>В этом чате уже выполняется запрос.</b> Дождитесь завершения текущего.",
        "request_busy_global": "<tg-emoji emoji-id=5409235172979672859>⚠️</tg-emoji> <b>Qwen CLI сейчас занят другим запросом.</b> Попробуйте чуть позже.",
        "generic_error": "<tg-emoji emoji-id=5332431395266524007>❗️</tg-emoji> <b>Ошибка:</b>\n<code>{}</code>",
        "bootstrap_done": "<tg-emoji emoji-id=5330561907671727296>✅</tg-emoji> <b>Локальный Qwen CLI подготовлен.</b>",
        "bootstrap_verify_fail": "<tg-emoji emoji-id=5332431395266524007>❗️</tg-emoji> <b>Установка завершилась, но верификация Qwen CLI не прошла.</b>\n<code>{}</code>",
        "qwen_auth_running": "<tg-emoji emoji-id=5472308992514464048>🔐</tg-emoji> <b>Подготавливаю вход в Qwen...</b>",
        "qwen_auth_step": (
            "<tg-emoji emoji-id=5472308992514464048>🔐</tg-emoji> <b>Вход в Qwen</b>\n\n"
            "1. Откройте ссылку:\n<code>{}</code>\n\n"
            "2. Войдите в аккаунт и подтвердите доступ.\n\n"
            "<i>Я дождусь подтверждения автоматически.</i>"
        ),
        "qwen_auth_done": "<tg-emoji emoji-id=5330561907671727296>✅</tg-emoji> <b>Qwen OAuth успешно авторизован.</b>",
        "qwen_auth_failed": "<tg-emoji emoji-id=5332431395266524007>❗️</tg-emoji> <b>Qwen OAuth не завершился успешно.</b>\n<code>{}</code>",
        "qwen_auth_already": "<tg-emoji emoji-id=5330561907671727296>✅</tg-emoji> <b>Qwen OAuth уже настроен.</b>",
        "question_prefix": "<tg-emoji emoji-id=5312103894875143512>💬</tg-emoji> <b>Запрос:</b>",
        "response_prefix": "<tg-emoji emoji-id=5330529399064266580>✨</tg-emoji> <b>{}:</b>",
        "memory_status": "<tg-emoji emoji-id=5350445475948414299>🧠</tg-emoji> [{}/{}]",
        "memory_status_unlimited": "<tg-emoji emoji-id=5350445475948414299>🧠</tg-emoji> [{}/∞]",
        "memory_cleared": "<tg-emoji emoji-id=6007942490076745785>🧹</tg-emoji> <b>Память диалога очищена.</b>",
        "memory_cleared_auto": "<tg-emoji emoji-id=6007942490076745785>🧹</tg-emoji> <b>Память авто-ответа в этом чате очищена.</b>",
        "no_memory_to_clear": "<tg-emoji emoji-id=5278753302023004775>ℹ️</tg-emoji> <b>В этом чате нет истории.</b>",
        "no_auto_memory_to_clear": "<tg-emoji emoji-id=5278753302023004775>ℹ️</tg-emoji> <b>В этом чате нет истории авто-ответа.</b>",
        "memory_chats_title": "<tg-emoji emoji-id=5350445475948414299>🧠</tg-emoji> <b>Чаты с историей ({}):</b>",
        "memory_chat_line": "  • {} (<code>{}</code>)",
        "no_memory_found": "<tg-emoji emoji-id=5278753302023004775>ℹ️</tg-emoji> Память пуста.",
        "media_reply_placeholder": "[запрос по медиа]",
        "btn_clear": "🧹 Очистить",
        "btn_regenerate": "🔄 Другой ответ",
        "btn_retry_request": "🔄 Повторить запрос",
        "btn_cancel_request": "❌ Отменить запрос",
        "no_last_request": "Последний запрос не найден для повторной генерации.",
        "request_cancelled": "<tg-emoji emoji-id=5350470691701407492>⛔</tg-emoji>️ <b>Запрос отменен.</b>",
        "memory_fully_cleared": "<tg-emoji emoji-id=6007942490076745785>🧹</tg-emoji> <b>Вся память полностью очищена (затронуто {} чатов).</b>",
        "auto_memory_fully_cleared": "<tg-emoji emoji-id=6007942490076745785>🧹</tg-emoji> <b>Вся память авто-ответа очищена (затронуто {} чатов).</b>",
        "no_memory_to_fully_clear": "<tg-emoji emoji-id=5278753302023004775>ℹ️</tg-emoji> <b>Память и так пуста.</b>",
        "no_auto_memory_to_fully_clear": "<tg-emoji emoji-id=5278753302023004775>ℹ️</tg-emoji> <b>Память авто-ответа и так пуста.</b>",
        "response_too_long": "Ответ был слишком длинным и отправлен файлом.",
        "qwen_files_only": "<tg-emoji emoji-id=5377844313575150051>📎</tg-emoji> <b>Qwen создал файлы. Отправляю их ниже.</b>",
        "qwen_file_caption": "<tg-emoji emoji-id=5377844313575150051>📎</tg-emoji> <b>Файл от Qwen:</b> <code>{}</code>",
        "qwen_status_title": "<tg-emoji emoji-id=5276127848644503161>🤖</tg-emoji> <b>Qwen active</b>{} · {}",
        "qwen_status_phase": "{} <code>{}</code>",
        "qwen_status_step": "<tg-emoji emoji-id=5269528017213887051>🏃‍♂️</tg-emoji> step <code>{}</code> · <tg-emoji emoji-id=5936170807716745162>🎛</tg-emoji> <code>{}s</code>",
        "qwen_status_tokens": "<tg-emoji emoji-id=5255713220546538619>💳</tg-emoji> in <code>{}</code>{} / out <code>{}</code> / total <code>{}</code>",
        "qwen_status_tool": "<tg-emoji emoji-id=5962952497197748583>🔧</tg-emoji> <code>{}</code>{}",
        "qwen_status_final_error": "<tg-emoji emoji-id=5350470691701407492>⛔</tg-emoji> error: <code>{}</code>",
        "qwclear_usage": "<tg-emoji emoji-id=5278753302023004775>ℹ️</tg-emoji> <b>Использование:</b> <code>.qwclear [auto]</code>",
        "qwreset_usage": "<tg-emoji emoji-id=5278753302023004775>ℹ️</tg-emoji> <b>Использование:</b> <code>.qwreset [auto]</code>",
        "auto_mode_on": "<tg-emoji emoji-id=5359441070201513074>🎭</tg-emoji> <b>Режим авто-ответа включен в этом чате.</b>\nЯ буду отвечать на сообщения с вероятностью {}%.",
        "auto_mode_off": "<tg-emoji emoji-id=5359441070201513074>🎭</tg-emoji> <b>Режим авто-ответа выключен в этом чате.</b>",
        "auto_mode_chats_title": "<tg-emoji emoji-id=5359441070201513074>🎭</tg-emoji> <b>Чаты с активным авто-ответом ({}):</b>",
        "no_auto_mode_chats": "<tg-emoji emoji-id=5278753302023004775>ℹ️</tg-emoji> Нет чатов с включенным режимом авто-ответа.",
        "auto_mode_usage": "<tg-emoji emoji-id=5278753302023004775>ℹ️</tg-emoji> <b>Использование:</b> <code>.qwauto on/off</code> или <code>.qwauto [id/username] on/off</code>",
        "auto_chat_not_found": "<tg-emoji emoji-id=5408830797513784663>🚫</tg-emoji> <b>Не удалось найти чат:</b> <code>{}</code>",
        "auto_state_updated": "<tg-emoji emoji-id=5359441070201513074>🎭</tg-emoji> <b>Режим авто-ответа для чата {} {}</b>",
        "auto_enabled": "включен",
        "auto_disabled": "выключен",
        "qwch_usage": "<tg-emoji emoji-id=5278753302023004775>ℹ️</tg-emoji> <b>Использование:</b>\n<code>.qwch &lt;кол-во&gt; &lt;вопрос&gt;</code>\n<code>.qwch &lt;id чата&gt; &lt;кол-во&gt; &lt;вопрос&gt;</code>",
        "qwch_processing": "<tg-emoji emoji-id=5332688668102525212>⌛️</tg-emoji> <b>Анализирую {} сообщений...</b>",
        "qwch_result_caption": "Анализ последних {} сообщений",
        "qwch_result_caption_from_chat": "Анализ последних {} сообщений из чата <b>{}</b>",
        "qwch_chat_error": "<tg-emoji emoji-id=5332431395266524007>❗️</tg-emoji> <b>Ошибка доступа к чату</b> <code>{}</code>: <i>{}</i>",
        "qwprompt_usage": "<tg-emoji emoji-id=5278753302023004775>ℹ️</tg-emoji> <b>Использование:</b>\n<code>.qwprompt &lt;текст/пресет&gt;</code> — установить.\n<code>.qwprompt -c</code> — очистить.\n<code>.qwpresets</code> — база пресетов.",
        "qwprompt_updated": "<tg-emoji emoji-id=5330561907671727296>✅</tg-emoji> <b>Системный промпт обновлен.</b>\nДлина: {} символов.",
        "qwprompt_cleared": "<tg-emoji emoji-id=5370872568041471196>🗑</tg-emoji> <b>Системный промпт очищен.</b>",
        "qwprompt_current": "<tg-emoji emoji-id=5956561916573782596>📝</tg-emoji> <b>Текущий системный промпт:</b>",
        "qwprompt_file_error": "<tg-emoji emoji-id=5332431395266524007>❗️</tg-emoji> <b>Ошибка чтения файла:</b> {}",
        "qwprompt_file_too_big": "<tg-emoji emoji-id=5332431395266524007>❗️</tg-emoji> <b>Файл слишком большой</b> (лимит 1 МБ).",
        "qwprompt_not_text": "<tg-emoji emoji-id=5332431395266524007>❗️</tg-emoji> Это не похоже на текстовый файл.",
        "qwmodel_usage": "<tg-emoji emoji-id=5278753302023004775>ℹ️</tg-emoji> <b>Использование:</b> <code>.qwmodel [модель]</code> или <code>.qwmodel -s</code>",
        "qwauth_usage": (
            "<tg-emoji emoji-id=5278753302023004775>ℹ️</tg-emoji> <b>Авторизация:</b>\n"
            "• <code>.qwauth status</code> — показать статус\n"
            "• <code>.qwauth qwen</code> — вход в Qwen через Telegram и браузер"
        ),
        "qwpresets_usage": (
            "<tg-emoji emoji-id=5278753302023004775>ℹ️</tg-emoji> <b>Управление пресетами:</b>\n"
            "• <code>.qwpresets save [Имя] текст</code> — сохранить.\n"
            "• <code>.qwpresets load 1</code> или <code>имя</code> — загрузить.\n"
            "• <code>.qwpresets del 1</code> или <code>имя</code> — удалить.\n"
            "• <code>.qwpresets list</code> — список."
        ),
        "qwpreset_loaded": "<tg-emoji emoji-id=5330561907671727296>✅</tg-emoji> <b>Установлен пресет:</b> [<code>{}</code>]\nДлина: {} симв.",
        "qwpreset_saved": "<tg-emoji emoji-id=5872695159631647090>💾</tg-emoji> <b>Пресет сохранен.</b>\n🏷 <b>Имя:</b> {}\n№ <b>Индекс:</b> {}",
        "qwpreset_deleted": "<tg-emoji emoji-id=5370872568041471196>🗑</tg-emoji> <b>Пресет удален:</b> {}",
        "qwpreset_not_found": "<tg-emoji emoji-id=5408830797513784663>🚫</tg-emoji> Пресет с таким именем или индексом не найден.",
        "qwpreset_list_head": "<tg-emoji emoji-id=5256230583717079814>📋</tg-emoji> <b>Ваши пресеты:</b>\n",
        "qwpreset_empty": "<tg-emoji emoji-id=5872695159631647090>💾</tg-emoji> Список пресетов пуст.",
        "unsupported_media": "<tg-emoji emoji-id=5409235172979672859>⚠️</tg-emoji> <b>Этот тип медиа пока не поддерживается для Qwen CLI:</b> <code>{}</code>",
        "auth_type_updated": "<tg-emoji emoji-id=5330561907671727296>✅</tg-emoji> <b>Auth type переключен:</b> <code>{}</code>",
        "status_title": "<tg-emoji emoji-id=5472308992514464048>🔐</tg-emoji> <b>Статус модуля:</b>",
        "status_auth_type": "• Auth type: <code>{}</code>",
        "status_qwen": "• Qwen CLI: {}",
        "status_model": "• Модель: <code>{}</code>",
        "status_set": "настроен",
        "status_missing": "не настроен",
        "status_ready": "готов",
        "status_not_ready": "не готов",
        "qwen_models_note": (
            "<tg-emoji emoji-id=5256230583717079814>📋</tg-emoji> <b>Быстрый список моделей:</b>\n"
            "• <code>coder-model</code> — обычные текстовые и кодовые задачи\n"
            "• <code>vision-model</code> — задачи с изображениями\n\n"
            "Если у вас настроен другой runtime-модель-id, его тоже можно указать вручную."
        ),
    }

    _PHASE_EMOJI = {
        "starting": "<tg-emoji emoji-id=5415941463764667665>⏳</tg-emoji>",
        "thinking": "<tg-emoji emoji-id=5350445475948414299>🧠</tg-emoji>",
        "running tool": "⚙️",
        "writing answer": "✍️",
        "completed": "<tg-emoji emoji-id=5330561907671727296>✅</tg-emoji>",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "qwen_path",
                "",
                self.strings["cfg_qwen_path_doc"],
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "qwen_model",
                "coder-model",
                self.strings["cfg_qwen_model_doc"],
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "auth_type",
                "qwen-oauth",
                self.strings["cfg_auth_type_doc"],
                validator=loader.validators.Choice(["qwen-oauth"]),
            ),
            loader.ConfigValue(
                "interactive_buttons",
                True,
                self.strings["cfg_buttons_doc"],
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "system_instruction",
                "",
                self.strings["cfg_system_instruction_doc"],
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "max_history_length",
                100,
                self.strings["cfg_max_history_length_doc"],
                validator=loader.validators.Integer(minimum=0),
            ),
            loader.ConfigValue(
                "timezone",
                "Europe/Moscow",
                self.strings["cfg_timezone_doc"],
            ),
            loader.ConfigValue(
                "proxy",
                "",
                self.strings["cfg_proxy_doc"],
            ),
            loader.ConfigValue(
                "auto_reply_chats",
                "",
                self.strings["cfg_auto_reply_chats_doc"],
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "memory_disabled_chats",
                "",
                self.strings["cfg_memory_disabled_chats_doc"],
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "impersonation_prompt",
                (
                    "Ты — это я, пользователь Telegram по имени {my_name}. Твоя задача — отвечать в чате от моего имени, "
                    "основываясь на последних сообщениях. Веди себя естественно, как обычный человек, а не как ассистент. "
                    "Не признавайся, что ты — ИИ.\n"
                    "Правила:\n"
                    "- Отвечай кратко и по делу.\n"
                    "- Используй естественный живой язык.\n"
                    "- Не отвечай на каждое сообщение.\n"
                    "- Не используй префиксы и кавычки.\n\n"
                    "ИСТОРИЯ ЧАТА:\n{chat_history}\n\n{my_name}:"
                ),
                self.strings["cfg_impersonation_prompt_doc"],
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "impersonation_history_limit",
                20,
                self.strings["cfg_impersonation_history_limit_doc"],
                validator=loader.validators.Integer(minimum=5, maximum=100),
            ),
            loader.ConfigValue(
                "impersonation_reply_chance",
                0.25,
                self.strings["cfg_impersonation_reply_chance_doc"],
                validator=loader.validators.Float(minimum=0.0, maximum=1.0),
            ),
            loader.ConfigValue(
                "auto_in_pm",
                False,
                "Разрешить авто-ответы в личных сообщениях.",
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "inline_pagination",
                False,
                self.strings["cfg_inline_pagination_doc"],
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "chat_recording",
                False,
                self.strings["cfg_chat_recording_doc"],
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "approval_mode",
                "yolo",
                self.strings["cfg_approval_mode_doc"],
                validator=loader.validators.Choice(
                    ["plan", "default", "auto-edit", "yolo"]
                ),
            ),
            loader.ConfigValue(
                "max_concurrent_requests",
                1,
                self.strings["cfg_max_concurrent_requests_doc"],
                validator=loader.validators.Integer(minimum=1, maximum=4),
            ),
            loader.ConfigValue(
                "auto_bootstrap",
                True,
                self.strings["cfg_auto_bootstrap_doc"],
                validator=loader.validators.Boolean(),
            ),
        )
        self.prompt_presets = []
        self.conversations = {}
        self.auto_conversations = {}
        self.last_requests = {}
        self.impersonation_chats = set()
        self.memory_disabled_chats = set()
        self.pager_cache = {}
        self._cfg_sync_cache = {}
        self._request_semaphore = asyncio.Semaphore(
            int(self.config["max_concurrent_requests"])
        )
        self._active_processes = {}
        self._chat_running = set()
        self._runtime_limits_cache = {
            "max_concurrent_requests": int(self.config["max_concurrent_requests"])
        }
        self._install_lock = asyncio.Lock()

    async def client_ready(self, client, db):
        self.client = client
        self.db = db
        self.me = await client.get_me()
        self.conversations = self._load_history_from_db(DB_HISTORY_KEY)
        self.auto_conversations = self._load_history_from_db(DB_GAUTO_HISTORY_KEY)
        self.prompt_presets = self.db.get(self.strings["name"], DB_PRESETS_KEY, [])
        if isinstance(self.prompt_presets, dict):
            self.prompt_presets = [
                {"name": k, "content": v} for k, v in self.prompt_presets.items()
            ]
        self.impersonation_chats = set(
            self.db.get(self.strings["name"], DB_IMPERSONATION_KEY, [])
        )
        self.memory_disabled_chats = set(
            self.db.get(self.strings["name"], DB_MEMORY_DISABLED_KEY, [])
        )
        self._migrate_runtime_lists_to_config()
        self._request_semaphore = asyncio.Semaphore(
            int(self.config["max_concurrent_requests"])
        )
        self._runtime_limits_cache["max_concurrent_requests"] = int(
            self.config["max_concurrent_requests"]
        )
        await self._sync_runtime_config(force=True)

    async def on_unload(self):
        procs = list(self._active_processes.values())
        self._active_processes.clear()
        self._chat_running.clear()
        for proc in procs:
            with contextlib.suppress(Exception):
                await self._terminate_process(proc)

    @loader.command()
    async def qw(self, message: Message):
        """[текст или reply] — спросить у Qwen CLI."""
        await self._sync_runtime_config()
        status_msg = await self._create_processing_status(
            message, self.strings["processing"]
        )
        payload, warnings = await self._prepare_request_payload(message)
        if warnings and status_msg:
            with contextlib.suppress(Exception):
                await self._edit_processing_status(
                    status_msg,
                    f"{self.strings['processing']}\n\n" + "\n".join(warnings),
                )
        if not payload:
            return await self._answer_html(
                status_msg,
                "<tg-emoji emoji-id=5409235172979672859>⚠️</tg-emoji> <i>Нужен текст, reply или поддерживаемое вложение.</i>",
            )
        await self._send_request(
            message=message, payload=payload, status_msg=status_msg
        )

    @loader.command()
    async def qwauth(self, message: Message):
        """status | type <auth> | apikey <key> | baseurl <url> | clear"""
        args = utils.get_args_raw(message).strip()
        if not args or args == "status":
            return await self._answer_html(message, await self._format_auth_status())
        parts = args.split(maxsplit=1)
        action = parts[0].lower()
        if action == "clear":
            with contextlib.suppress(Exception):
                os.unlink(os.path.join(self._get_user_qwen_dir(), "oauth_creds.json"))
            return await self._answer_html(message, "Авторизация очищена.")
        if action == "qwen":
            self.config["auth_type"] = "qwen-oauth"
            ready, _ = await self._get_qwen_status_for_runtime()
            if ready:
                return await self._answer_html(
                    message, self.strings["qwen_auth_already"]
                )
            status_msg = await self._answer_html(
                message, self.strings["qwen_auth_running"]
            )
            ok, info = await self._run_qwen_device_auth(status_msg)
            if ok:
                ready, verify_info = await self._get_qwen_status_for_runtime()
                if not ready:
                    ok = False
                    info = verify_info
            key = "qwen_auth_done" if ok else "qwen_auth_failed"
            return await self._answer_html(
                status_msg,
                self.strings[key]
                if ok
                else self.strings[key].format(utils.escape_html(info)),
            )
        if action == "type":
            if len(parts) < 2 or parts[1].strip() not in {"qwen-oauth"}:
                return await self._answer_html(message, self.strings["qwauth_usage"])
            self.config["auth_type"] = parts[1].strip()
            return await self._answer_html(
                message,
                self.strings["auth_type_updated"].format(
                    utils.escape_html(self.config["auth_type"])
                ),
            )
        await self._answer_html(message, self.strings["qwauth_usage"])

    @loader.command()
    async def qwinstall(self, message: Message):
        """— установить локальные Node.js и Qwen CLI в user-space."""
        status_msg = await self._answer_html(message, self.strings["bootstrap_wait"])
        try:
            await self._ensure_qwen_cli_available(force=True)
            await self._answer_html(status_msg, self.strings["bootstrap_done"])
        except Exception as e:
            await self._answer_html(status_msg, self._handle_error(e))

    @loader.command()
    async def qwch(self, message: Message):
        """<[id чата]> <кол-во> <вопрос> — проанализировать историю чата."""
        await self._sync_runtime_config()
        args_str = utils.get_args_raw(message)
        if not args_str:
            return await self._answer_html(message, self.strings["qwch_usage"])
        parts = args_str.split()
        target_chat_id = utils.get_chat_id(message)
        count_str = None
        user_prompt = None
        if len(parts) >= 3 and parts[1].isdigit():
            try:
                entity = await self.client.get_entity(
                    int(parts[0]) if parts[0].lstrip("-").isdigit() else parts[0]
                )
                target_chat_id = entity.id
                count_str = parts[1]
                user_prompt = " ".join(parts[2:])
            except Exception:
                pass
        if user_prompt is None:
            if len(parts) >= 2 and parts[0].isdigit():
                count_str = parts[0]
                user_prompt = " ".join(parts[1:])
            else:
                return await self._answer_html(message, self.strings["qwch_usage"])
        try:
            count = int(count_str)
        except Exception:
            return await self._answer_html(
                message,
                "<tg-emoji emoji-id=5332431395266524007>❗️</tg-emoji> Кол-во должно быть числом.",
            )

        status_msg = await self._answer_html(
            message, self.strings["qwch_processing"].format(count)
        )
        try:
            entity = await self.client.get_entity(target_chat_id)
            chat_name = utils.escape_html(get_display_name(entity))
            chat_log = await self._get_recent_chat_text(
                target_chat_id, count=count, skip_last=False
            )
        except (
            ValueError,
            TypeError,
            ChatAdminRequiredError,
            UserNotParticipantError,
            ChannelPrivateError,
        ) as e:
            return await self._answer_html(
                status_msg,
                self.strings["qwch_chat_error"].format(
                    target_chat_id, e.__class__.__name__
                ),
            )
        except Exception as e:
            return await self._answer_html(
                status_msg, self.strings["qwch_chat_error"].format(target_chat_id, e)
            )

        prompt = (
            "Проанализируй следующую историю чата и ответь на вопрос пользователя. "
            "Отвечай только на основе переданной истории.\n\n"
            f'ВОПРОС ПОЛЬЗОВАТЕЛЯ: "{user_prompt}"\n\n'
            f"ИСТОРИЯ ЧАТА:\n---\n{chat_log}\n---"
        )
        payload = {"text": prompt, "files": [], "display_prompt": user_prompt}
        try:
            result = await self._run_qwen_request(
                target_chat_id,
                payload,
                system_prompt=self.config["system_instruction"].strip() or None,
                history_override=[],
            )
            header = self.strings["qwch_result_caption_from_chat"].format(
                count, chat_name
            )
            resp_html = self._markdown_to_html(result["text"])
            text = (
                f"<b>{header}</b>\n\n"
                f"{self.strings['question_prefix']}\n"
                f"<blockquote expandable='true'>{utils.escape_html(user_prompt)}</blockquote>\n\n"
                f"{self.strings['response_prefix'].format(utils.escape_html(result['label']))}\n"
                f"{self._format_response_with_smart_separation(resp_html)}"
            )
            if len(text) > 4096:
                f = io.BytesIO(result["text"].encode("utf-8"))
                f.name = "analysis.txt"
                await status_msg.delete()
                await message.reply(
                    file=f,
                    caption=f"<tg-emoji emoji-id=5956561916573782596>📝</tg-emoji> {header}",
                )
            else:
                await self._answer_html(status_msg, text)
        except Exception as e:
            await self._answer_html(status_msg, self._handle_error(e))

    @loader.command()
    async def qwprompt(self, message: Message):
        """<текст/-c/ответ на файл> — установить системный промпт."""
        await self._sync_runtime_config()
        args = utils.get_args_raw(message)
        reply = await message.get_reply_message()
        if args == "-c":
            self.config["system_instruction"] = ""
            return await self._answer_html(message, self.strings["qwprompt_cleared"])

        new_prompt = None
        preset = self._find_preset(args)
        if preset:
            new_prompt = preset["content"]
        elif reply and reply.file:
            if reply.file.size > 1024 * 1024:
                return await self._answer_html(
                    message, self.strings["qwprompt_file_too_big"]
                )
            try:
                file_data = await self.client.download_file(reply.media, bytes)
                try:
                    new_prompt = file_data.decode("utf-8")
                except UnicodeDecodeError:
                    return await self._answer_html(
                        message, self.strings["qwprompt_not_text"]
                    )
            except Exception as e:
                return await self._answer_html(
                    message, self.strings["qwprompt_file_error"].format(e)
                )
        elif args:
            new_prompt = args

        if new_prompt is not None:
            self.config["system_instruction"] = new_prompt
            return await self._answer_html(
                message, self.strings["qwprompt_updated"].format(len(new_prompt))
            )

        current_prompt = self.config["system_instruction"]
        if not current_prompt:
            return await self._answer_html(message, self.strings["qwprompt_usage"])
        if len(current_prompt) > 4000:
            file = io.BytesIO(current_prompt.encode("utf-8"))
            file.name = "system_instruction.txt"
            await self.client.send_file(
                message.chat_id,
                file=file,
                caption=self.strings["qwprompt_current"],
                reply_to=self._get_reply_target_id(message),
                parse_mode="html",
            )
        else:
            await self._answer_html(
                message,
                f"{self.strings['qwprompt_current']}\n<code>{utils.escape_html(current_prompt)}</code>",
            )

    @loader.command()
    async def qwauto(self, message: Message):
        """<on/off/[id]> — вкл/выкл авто-ответ в чате."""
        await self._sync_runtime_config()
        args = utils.get_args_raw(message).split()
        if not args:
            return await self._answer_html(message, self.strings["auto_mode_usage"])
        chat_id = utils.get_chat_id(message)
        state = args[0].lower()
        target = chat_id
        if len(args) == 2:
            try:
                entity = await self.client.get_entity(args[0])
                target = entity.id
                state = args[1].lower()
            except Exception:
                return await self._answer_html(
                    message, self.strings["auto_chat_not_found"].format(args[0])
                )
        if state == "on":
            await self._update_chat_list_config("auto_reply_chats", target, True)
            txt = (
                self.strings["auto_mode_on"].format(
                    int(self.config["impersonation_reply_chance"] * 100)
                )
                if target == chat_id
                else self.strings["auto_state_updated"].format(
                    f"<code>{target}</code>", self.strings["auto_enabled"]
                )
            )
            return await self._answer_html(message, txt)
        if state == "off":
            await self._update_chat_list_config("auto_reply_chats", target, False)
            txt = (
                self.strings["auto_mode_off"]
                if target == chat_id
                else self.strings["auto_state_updated"].format(
                    f"<code>{target}</code>", self.strings["auto_disabled"]
                )
            )
            return await self._answer_html(message, txt)
        await self._answer_html(message, self.strings["auto_mode_usage"])

    @loader.command()
    async def qwautochats(self, message: Message):
        """— показать чаты с активным авто-ответом."""
        await self._sync_runtime_config()
        if not self.impersonation_chats:
            return await self._answer_html(message, self.strings["no_auto_mode_chats"])
        out = [
            self.strings["auto_mode_chats_title"].format(len(self.impersonation_chats))
        ]
        for cid in self.impersonation_chats:
            try:
                entity = await self.client.get_entity(cid)
                name = utils.escape_html(get_display_name(entity))
                out.append(self.strings["memory_chat_line"].format(name, cid))
            except Exception:
                out.append(
                    self.strings["memory_chat_line"].format("Неизвестный чат", cid)
                )
        await self._answer_html(message, "\n".join(out))

    @loader.command()
    async def qwclear(self, message: Message):
        """[auto] — очистить память в чате. auto для авто-ответа."""
        await self._sync_runtime_config()
        args = utils.get_args_raw(message)
        chat_id = utils.get_chat_id(message)
        if args == "auto":
            if str(chat_id) in self.auto_conversations:
                self._clear_history(chat_id, auto=True)
                return await self._answer_html(
                    message, self.strings["memory_cleared_auto"]
                )
            return await self._answer_html(
                message, self.strings["no_auto_memory_to_clear"]
            )
        if not args:
            if str(chat_id) in self.conversations:
                self._clear_history(chat_id)
                return await self._answer_html(message, self.strings["memory_cleared"])
            return await self._answer_html(message, self.strings["no_memory_to_clear"])
        await self._answer_html(message, self.strings["qwclear_usage"])

    @loader.command()
    async def qwpresets(self, message: Message):
        """<save/load/del/list> — управление пресетами."""
        await self._sync_runtime_config()
        args = utils.get_args_raw(message)
        if not args:
            return await self._answer_html(message, self.strings["qwpresets_usage"])
        match = re.match(
            r"^(\w+)(?:\s+\[(.+?)\]|\s+(\S+))?(?:\s+(.*))?$", args, re.DOTALL
        )
        if not match:
            return await self._answer_html(message, self.strings["qwpresets_usage"])
        action = match.group(1).lower()
        name = match.group(2) or match.group(3)
        content = match.group(4)

        if action == "list":
            if not self.prompt_presets:
                return await self._answer_html(message, self.strings["qwpreset_empty"])
            text = self.strings["qwpreset_list_head"]
            for idx, preset in enumerate(self.prompt_presets, 1):
                text += f"<b>{idx}.</b> <code>{utils.escape_html(preset['name'])}</code> ({len(preset['content'])} симв.)\n"
            return await self._answer_html(message, text)

        if action == "save":
            if not name:
                return await self._answer_html(
                    message, "❌ Укажите имя: <code>.qwpresets save [Имя] текст</code>"
                )
            reply = await message.get_reply_message()
            if not content and reply:
                if reply.text:
                    content = reply.text
                elif reply.file:
                    try:
                        content = (
                            await self.client.download_file(reply.media, bytes)
                        ).decode("utf-8", errors="ignore")
                    except Exception:
                        pass
            if not content:
                return await self._answer_html(message, "❌ Нет текста для сохранения.")
            existing = self._find_preset(name)
            if existing:
                existing["content"] = content
            else:
                self.prompt_presets.append({"name": name, "content": content})
            self.db.set(self.strings["name"], DB_PRESETS_KEY, self.prompt_presets)
            return await self._answer_html(
                message,
                self.strings["qwpreset_saved"].format(name, len(self.prompt_presets)),
            )

        if action == "load":
            target = self._find_preset(name)
            if not target:
                return await self._answer_html(
                    message, self.strings["qwpreset_not_found"]
                )
            self.config["system_instruction"] = target["content"]
            return await self._answer_html(
                message,
                self.strings["qwpreset_loaded"].format(
                    target["name"], len(target["content"])
                ),
            )

        if action == "del":
            target = self._find_preset(name)
            if not target:
                return await self._answer_html(
                    message, self.strings["qwpreset_not_found"]
                )
            self.prompt_presets.remove(target)
            self.db.set(self.strings["name"], DB_PRESETS_KEY, self.prompt_presets)
            return await self._answer_html(
                message, self.strings["qwpreset_deleted"].format(target["name"])
            )

        await self._answer_html(message, self.strings["qwpresets_usage"])

    @loader.command()
    async def qwmemdel(self, message: Message):
        """[N] — удалить последние N пар сообщений из памяти."""
        await self._sync_runtime_config()
        try:
            pairs = int(utils.get_args_raw(message) or 1)
        except Exception:
            pairs = 1
        cid = utils.get_chat_id(message)
        hist = self._get_structured_history(cid)
        if pairs > 0 and len(hist) >= pairs * 2:
            self.conversations[str(cid)] = hist[: -(pairs * 2)]
            self._save_history_sync()
            return await self._answer_html(
                message,
                f"<tg-emoji emoji-id=6007942490076745785>🧹</tg-emoji> Удалено последних <b>{pairs}</b> пар сообщений из памяти.",
            )
        await self._answer_html(message, "Недостаточно истории для удаления.")

    @loader.command()
    async def qwmemchats(self, message: Message):
        """— показать список чатов с активной памятью."""
        await self._sync_runtime_config()
        if not self.conversations:
            return await self._answer_html(message, self.strings["no_memory_found"])
        out = [self.strings["memory_chats_title"].format(len(self.conversations))]
        shown = set()
        for cid in list(self.conversations.keys()):
            if not str(cid).lstrip("-").isdigit():
                continue
            chat_id = int(cid)
            if chat_id in shown:
                continue
            shown.add(chat_id)
            try:
                entity = await self.client.get_entity(chat_id)
                name = get_display_name(entity)
            except Exception:
                name = f"Unknown ({chat_id})"
            out.append(self.strings["memory_chat_line"].format(name, chat_id))
        if len(out) == 1:
            return await self._answer_html(message, self.strings["no_memory_found"])
        await self._answer_html(message, "\n".join(out))

    @loader.command()
    async def qwmemexport(self, message: Message):
        """[<id/@юз чата>] [auto] [-s] — экспорт истории."""
        await self._sync_runtime_config()
        args = utils.get_args_raw(message).split()
        save_to_self = "-s" in args
        if save_to_self:
            args.remove("-s")
        auto = "auto" in args
        if auto:
            args.remove("auto")
        src_id = (
            int(args[0])
            if args and args[0].lstrip("-").isdigit()
            else utils.get_chat_id(message)
        )
        hist = self._get_structured_history(src_id, auto=auto)
        if not hist:
            return await self._answer_html(message, "История для экспорта пуста.")
        data = json.dumps(hist, ensure_ascii=False, indent=2)
        f = io.BytesIO(data.encode("utf-8"))
        f.name = f"qwencli_{'auto_' if auto else ''}{src_id}.json"
        dest = "me" if save_to_self else message.chat_id
        caption = "Экспорт истории авто-ответа" if auto else "Экспорт памяти"
        if src_id != utils.get_chat_id(message):
            caption += f" из чата <code>{src_id}</code>"
        await self.client.send_file(dest, f, caption=caption, parse_mode="html")
        if save_to_self:
            return await self._answer_html(
                message,
                "<tg-emoji emoji-id=5872695159631647090>💾</tg-emoji> История экспортирована в избранное.",
            )
        if args:
            await message.delete()

    @loader.command()
    async def qwmemimport(self, message: Message):
        """[auto] — импорт истории из json-файла (ответом)."""
        await self._sync_runtime_config()
        reply = await message.get_reply_message()
        if not reply or not reply.document:
            return await self._answer_html(message, "Ответьте на json-файл с памятью.")
        auto = "auto" in utils.get_args_raw(message)
        try:
            raw = await self.client.download_media(reply, bytes)
            hist = json.loads(raw)
            if not isinstance(hist, list):
                raise ValueError("JSON должен содержать список.")
            cid = utils.get_chat_id(message)
            target = self.auto_conversations if auto else self.conversations
            target[str(cid)] = hist
            self._save_history_sync(auto)
            await self._answer_html(message, "Память успешно импортирована.")
        except Exception as e:
            await self._answer_html(
                message, f"Ошибка импорта: {utils.escape_html(str(e))}"
            )

    @loader.command()
    async def qwmemfind(self, message: Message):
        """[слово] — поиск в памяти текущего чата."""
        await self._sync_runtime_config()
        query = utils.get_args_raw(message).lower().strip()
        if not query:
            return await self._answer_html(message, "Укажите слово для поиска.")
        cid = utils.get_chat_id(message)
        hist = self._get_structured_history(cid)
        found = [
            f"{entry['role']}: {utils.escape_html(str(entry.get('content', ''))[:200])}"
            for entry in hist
            if query in str(entry.get("content", "")).lower()
        ]
        if not found:
            return await self._answer_html(message, "Ничего не найдено.")
        await self._answer_html(message, "\n\n".join(found[:10]))

    @loader.command()
    async def qwmem(self, message: Message):
        """— переключить память в этом чате."""
        await self._sync_runtime_config()
        chat_id = str(utils.get_chat_id(message))
        is_enabled = self._is_memory_enabled(chat_id)
        await self._update_chat_list_config(
            "memory_disabled_chats", chat_id, is_enabled
        )
        await self._answer_html(
            message,
            "Память в этом чате отключена."
            if is_enabled
            else "Память в этом чате включена.",
        )

    @loader.command()
    async def qwmemshow(self, message: Message):
        """[auto] — показать память чата."""
        await self._sync_runtime_config()
        auto = "auto" in utils.get_args_raw(message)
        cid = utils.get_chat_id(message)
        hist = self._get_structured_history(cid, auto=auto)
        if not hist:
            return await self._answer_html(message, "Память пуста.")
        out = []
        for entry in hist[-40:]:
            role = entry.get("role")
            content = utils.escape_html(str(entry.get("content", ""))[:300])
            if role == "user":
                out.append(content)
            else:
                out.append(f"<b>Assistant:</b> {content}")
        await self._answer_html(
            message, "<blockquote expandable='true'>" + "\n".join(out) + "</blockquote>"
        )

    @loader.command()
    async def qwmodel(self, message: Message):
        """[model] [-s] — узнать/сменить модель."""
        await self._sync_runtime_config()
        args_raw = utils.get_args_raw(message).strip()
        if not args_raw:
            return await self._answer_html(
                message,
                (
                    f"<tg-emoji emoji-id=5350445475948414299>🧠</tg-emoji> <b>Модель:</b> <code>{utils.escape_html(self.config['qwen_model'] or 'coder-model')}</code>\n"
                    f"<tg-emoji emoji-id=5472308992514464048>🔐</tg-emoji> <b>Auth type:</b> <code>{utils.escape_html(self.config['auth_type'])}</code>"
                ),
            )
        if args_raw == "-s":
            return await self._answer_html(message, self.strings["qwen_models_note"])
        self.config["qwen_model"] = args_raw
        await self._answer_html(
            message,
            f"<tg-emoji emoji-id=5330561907671727296>✅</tg-emoji> <b>Qwen model:</b> <code>{utils.escape_html(args_raw)}</code>",
        )

    @loader.command()
    async def qwreset(self, message: Message):
        """[auto] — очистить всю память."""
        await self._sync_runtime_config()
        if utils.get_args_raw(message) == "auto":
            if not self.auto_conversations:
                return await self._answer_html(
                    message, self.strings["no_auto_memory_to_fully_clear"]
                )
            count = len(self.auto_conversations)
            self.auto_conversations.clear()
            self._save_history_sync(True)
            return await self._answer_html(
                message, self.strings["auto_memory_fully_cleared"].format(count)
            )
        if not self.conversations:
            return await self._answer_html(
                message, self.strings["no_memory_to_fully_clear"]
            )
        count = len(self.conversations)
        self.conversations.clear()
        self._save_history_sync(False)
        await self._answer_html(
            message, self.strings["memory_fully_cleared"].format(count)
        )

    @loader.callback_handler()
    async def qwencli_callback_handler(self, call: InlineCall):
        if not call.data.startswith("qwencli:"):
            return
        parts = call.data.split(":")
        action = parts[1]
        if action == "noop":
            await call.answer()
            return
        if action == "pg":
            uid = parts[2]
            page = int(parts[3])
            await self._render_page(uid, page, call)

    @loader.watcher(only_incoming=True, ignore_edited=True)
    async def watcher(self, message: Message):
        await self._sync_runtime_config()
        if not hasattr(message, "chat_id"):
            return
        cid = utils.get_chat_id(message)
        if cid not in self.impersonation_chats:
            return
        if message.is_private and not self.config["auto_in_pm"]:
            return
        if message.out or (
            isinstance(message.from_id, tg_types.PeerUser)
            and message.from_id.user_id == self.me.id
        ):
            return
        sender = await message.get_sender()
        if isinstance(sender, tg_types.User) and sender.bot:
            return
        if random.random() > self.config["impersonation_reply_chance"]:
            return
        payload, warnings = await self._prepare_request_payload(message)
        if warnings:
            logger.warning("qwauto warnings: %s", warnings)
        if not payload:
            return
        resp = await self._send_request(
            message=message, payload=payload, impersonation_mode=True
        )
        if resp and resp.strip():
            clean = resp.strip()
            await asyncio.sleep(random.uniform(2, 8))
            with contextlib.suppress(Exception):
                await self.client.send_read_acknowledge(cid, message=message)
            async with message.client.action(cid, "typing"):
                await asyncio.sleep(
                    min(25.0, max(1.5, len(clean) * random.uniform(0.06, 0.15)))
                )
            await message.reply(clean)

    async def _send_request(
        self,
        message,
        payload: dict,
        regeneration: bool = False,
        call: InlineCall = None,
        status_msg=None,
        chat_id_override: int = None,
        impersonation_mode: bool = False,
    ):
        msg_obj = None
        if regeneration:
            chat_id = chat_id_override
            base_message_id = message
            try:
                msg_obj = await self.client.get_messages(chat_id, ids=base_message_id)
            except Exception:
                msg_obj = None
            current_payload, display_prompt = self.last_requests.get(
                f"{chat_id}:{base_message_id}",
                (
                    payload,
                    payload.get("display_prompt")
                    or self.strings["media_reply_placeholder"],
                ),
            )
        else:
            chat_id = utils.get_chat_id(message)
            base_message_id = message.id
            msg_obj = message
            current_payload = payload
            display_prompt = (
                payload.get("display_prompt") or self.strings["media_reply_placeholder"]
            )
            self.last_requests[f"{chat_id}:{base_message_id}"] = (
                current_payload,
                display_prompt,
            )

        if chat_id in self._chat_running:
            if impersonation_mode:
                return None
            if call:
                with contextlib.suppress(Exception):
                    return await call.answer(
                        re.sub(r"<.*?>", "", self.strings["request_busy_same_chat"]),
                        show_alert=True,
                    )
            target_entity = status_msg or msg_obj or message
            return await self._answer_html(
                target_entity, self.strings["request_busy_same_chat"]
            )

        try:
            if impersonation_mode:
                my_name = get_display_name(self.me)
                chat_history_text = await self._get_recent_chat_text(chat_id)
                system_prompt = self.config["impersonation_prompt"].format(
                    my_name=my_name, chat_history=chat_history_text
                )
            else:
                system_prompt = self.config["system_instruction"].strip() or None

            result = await self._run_qwen_request_guarded(
                chat_id=chat_id,
                payload=current_payload,
                system_prompt=system_prompt,
                auto=impersonation_mode,
                history_override=None,
                status_entity=call or status_msg,
            )
            raw_result_text = result.get("text", "").strip()
            generated_files = result.get("files") or []
            result_text = raw_result_text or (
                self.strings["qwen_files_only"] if generated_files else ""
            )
            label = result["label"]
            model_name = result["model"]

            await self._sync_runtime_config()
            if result_text and self._is_memory_enabled(str(chat_id)):
                self._update_history(
                    chat_id,
                    current_payload,
                    result_text,
                    regeneration=regeneration,
                    message=msg_obj,
                    auto=impersonation_mode,
                )

            if impersonation_mode:
                return raw_result_text

            hist_len = len(self._get_structured_history(chat_id)) // 2
            mem_ind = self.strings["memory_status"].format(
                hist_len, self.config["max_history_length"]
            )
            if self.config["max_history_length"] <= 0:
                mem_ind = self.strings["memory_status_unlimited"].format(hist_len)

            response_html = self._markdown_to_html(result_text)
            formatted_body = self._format_response_with_smart_separation(response_html)
            question_html = (
                f"<blockquote>{utils.escape_html(display_prompt[:250])}</blockquote>"
            )
            model_info = f"<i>{utils.escape_html(label)}: <code>{utils.escape_html(model_name)}</code></i>"
            reply_target_id = self._get_reply_target_id(
                msg_obj, fallback=base_message_id
            )
            text_to_send = (
                f"{mem_ind}\n{model_info}\n\n"
                f"{self.strings['question_prefix']}\n{question_html}\n\n"
                f"{self.strings['response_prefix'].format(utils.escape_html(label))}\n{formatted_body}"
            )
            buttons = (
                self._get_inline_buttons(chat_id, base_message_id)
                if self.config["interactive_buttons"]
                else None
            )

            if len(result_text) > 3500 and self.config["inline_pagination"]:
                chunks = self._paginate_text(result_text, 3000)
                uid = uuid.uuid4().hex[:6]
                header = (
                    f"{mem_ind}\n{model_info}\n\n"
                    f"{self.strings['question_prefix']}\n<blockquote>{utils.escape_html(display_prompt[:100])}</blockquote>\n\n"
                    f"{self.strings['response_prefix'].format(utils.escape_html(label))}\n"
                )
                self.pager_cache[uid] = {
                    "chunks": chunks,
                    "total": len(chunks),
                    "header": header,
                    "chat_id": chat_id,
                    "msg_id": base_message_id,
                }
                await self._render_page(uid, 0, call or status_msg)
            elif len(text_to_send) > 4096:
                file = io.BytesIO(result_text.encode("utf-8"))
                file.name = "qwen_response.txt"
                if call:
                    await call.answer(
                        "Ответ длинный, отправляю файлом...", show_alert=False
                    )
                    await self.client.send_file(
                        call.chat_id,
                        file,
                        caption=self.strings["response_too_long"],
                        reply_to=call.message_id,
                        parse_mode="html",
                    )
                elif status_msg:
                    await status_msg.delete()
                    await self.client.send_file(
                        chat_id,
                        file,
                        caption=self.strings["response_too_long"],
                        reply_to=reply_target_id,
                        parse_mode="html",
                    )
            else:
                if call:
                    await self._edit_html(call, text_to_send, reply_markup=buttons)
                elif status_msg:
                    await self._answer_html(
                        status_msg, text_to_send, reply_markup=buttons
                    )

            if generated_files:
                await self._send_qwen_generated_files(
                    chat_id, generated_files, reply_target_id
                )
        except Exception as e:
            error_text = self._handle_error(e)
            if impersonation_mode:
                logger.error("qwauto backend error: %s", error_text)
            elif call:
                await self._edit_html(
                    call,
                    error_text,
                    reply_markup=self._get_error_buttons(chat_id, base_message_id),
                )
            elif status_msg:
                buttons = self._get_error_buttons(chat_id, base_message_id)
                try:
                    await self._answer_html(
                        status_msg, error_text, reply_markup=buttons
                    )
                except Exception:
                    target_message = msg_obj or status_msg
                    await self._answer_html(
                        target_message, error_text, reply_markup=buttons
                    )
        return None if impersonation_mode else ""

    async def _run_qwen_request_guarded(
        self,
        chat_id: int,
        payload: dict,
        system_prompt: str = None,
        auto: bool = False,
        history_override=None,
        status_entity=None,
    ):
        if auto and self._request_semaphore.locked():
            raise RuntimeError(self.strings["request_busy_global"])
        if not auto and self._request_semaphore.locked() and status_entity is not None:
            with contextlib.suppress(Exception):
                await self._edit_processing_status(
                    status_entity, self.strings["queue_wait"]
                )

        await self._request_semaphore.acquire()
        self._chat_running.add(chat_id)
        try:
            try:
                return await self._run_qwen_request(
                    chat_id=chat_id,
                    payload=payload,
                    system_prompt=system_prompt,
                    auto=auto,
                    history_override=history_override,
                    status_entity=status_entity,
                    lean_mode=False,
                )
            except Exception as e:
                message = str(e)
                if (
                    "WebAssembly.instantiate(): Out of memory" in message
                    or "Cannot allocate Wasm memory" in message
                ):
                    await self._ensure_qwen_cli_available(force=True)
                    self._pin_detected_qwen_path()
                    return await self._run_qwen_request(
                        chat_id=chat_id,
                        payload=payload,
                        system_prompt=system_prompt,
                        auto=auto,
                        history_override=history_override,
                        status_entity=status_entity,
                        lean_mode=True,
                    )
                raise
        finally:
            self._chat_running.discard(chat_id)
            self._request_semaphore.release()

    async def _run_qwen_request(
        self,
        chat_id: int,
        payload: dict,
        system_prompt: str = None,
        auto: bool = False,
        history_override=None,
        status_entity=None,
        lean_mode: bool = False,
    ):
        await self._ensure_qwen_cli_available()
        qwen_path = self._get_qwen_binary()
        if not qwen_path:
            raise RuntimeError(self.strings["qwen_not_found"])
        ready, status = await self._get_qwen_status_for_runtime()
        if not ready:
            raise RuntimeError(status or self.strings["qwen_auth_missing"])

        selected_model = (self.config["qwen_model"] or "coder-model").strip()
        prompt, file_specs = self._build_qwen_prompt(
            chat_id,
            payload,
            system_prompt=system_prompt,
            auto=auto,
            history_override=history_override,
        )
        env = self._build_subprocess_env()

        with tempfile.TemporaryDirectory(prefix="qwencli_") as tempdir:
            runtime_home = self._prepare_qwen_runtime_home(tempdir)
            env["HOME"] = runtime_home
            args = self._build_qwen_args(
                qwen_path=qwen_path,
                prompt=prompt,
                file_specs=file_specs,
                selected_model=selected_model,
                lean_mode=lean_mode,
            )
            input_paths = set()
            for spec in file_specs:
                abs_path = os.path.join(tempdir, spec["name"])
                os.makedirs(os.path.dirname(abs_path), exist_ok=True)
                with open(abs_path, "wb") as file_obj:
                    file_obj.write(spec["data"])
                input_paths.add(os.path.abspath(abs_path))

            creation_kwargs = {
                "cwd": tempdir,
                "stdin": asyncio.subprocess.DEVNULL,
                "stdout": asyncio.subprocess.PIPE,
                "stderr": asyncio.subprocess.PIPE,
                "env": env,
            }
            if os.name != "nt":
                creation_kwargs["start_new_session"] = True

            proc = await asyncio.create_subprocess_exec(*args, **creation_kwargs)
            request_id = uuid.uuid4().hex[:10]
            self._active_processes[request_id] = proc
            progress_state = self._make_qwen_progress_state()
            progress_state["model"] = selected_model or "coder-model"
            stdout_lines = []
            stderr_lines = []
            stdout_task = asyncio.create_task(
                self._read_qwen_stdout_stream(
                    proc.stdout,
                    stdout_lines,
                    progress_state,
                    status_entity if not auto else None,
                )
            )
            stderr_task = asyncio.create_task(
                self._read_qwen_stderr_stream(proc.stderr, stderr_lines, progress_state)
            )
            try:
                await asyncio.wait_for(proc.wait(), timeout=QWEN_TIMEOUT)
            except asyncio.TimeoutError:
                await self._terminate_process(proc)
                await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
                raise RuntimeError(f"Qwen CLI превысил таймаут ({QWEN_TIMEOUT} сек).")
            finally:
                self._active_processes.pop(request_id, None)
            await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
            if status_entity and not auto:
                await self._update_qwen_status_message(
                    status_entity, progress_state, force=True
                )

            final_text = progress_state["final_text"].strip()
            generated_files = self._collect_qwen_generated_files(
                tempdir,
                ignored_names={".qwen", "runtime-home", "input"},
                ignored_paths=input_paths,
            )
            stderr_text = "\n".join(stderr_lines).strip()
            stdout_text = "\n".join(stdout_lines).strip()
            if progress_state["final_error"]:
                raise RuntimeError(progress_state["final_error"])
            if proc.returncode != 0 and not final_text and not generated_files:
                raise RuntimeError(
                    stderr_text
                    or stdout_text
                    or f"Qwen не вернул ответ (код {proc.returncode})."
                )
            if not final_text and not generated_files:
                raise RuntimeError("Qwen не вернул ответ. Попробуйте ещё раз.")

        return {
            "text": final_text,
            "model": selected_model or "coder-model",
            "label": "Qwen CLI",
            "files": generated_files,
        }

    def _build_qwen_args(
        self,
        qwen_path: str,
        prompt: str,
        file_specs: list,
        selected_model: str,
        lean_mode: bool = False,
    ) -> list:
        args = [
            qwen_path,
            "--prompt",
            prompt,
            "--output-format",
            "stream-json",
            "--approval-mode",
            self.config["approval_mode"],
            "--auth-type",
            self.config["auth_type"],
            "--chat-recording",
            "false"
            if lean_mode
            else ("true" if self.config["chat_recording"] else "false"),
            "--channel",
            "CI",
        ]
        if not lean_mode:
            args.append("--include-partial-messages")
        if any(spec.get("type") == "image" for spec in file_specs):
            args.extend(["--vlm-switch-mode", "once"])
        if selected_model:
            args.extend(["--model", selected_model])
        if self.config["proxy"].strip():
            args.extend(["--proxy", self.config["proxy"].strip()])
        return args

    def _make_qwen_progress_state(self) -> dict:
        return {
            "started_at": asyncio.get_running_loop().time(),
            "last_status_at": 0.0,
            "last_status_text": "",
            "phase": "starting",
            "step": 0,
            "active_tool": "",
            "last_exit_code": None,
            "final_error": "",
            "input_tokens": 0,
            "output_tokens": 0,
            "cached_tokens": 0,
            "total_tokens": 0,
            "session_id": "",
            "model": "",
            "final_text": "",
            "tool_use_ids": {},
        }

    @staticmethod
    def _fmt_num(n: int) -> str:
        return f"{int(n):,}"

    def _update_qwen_progress_state(self, state: dict, payload: dict):
        msg_type = payload.get("type")
        if payload.get("session_id"):
            state["session_id"] = payload["session_id"]

        if msg_type == "system":
            state["phase"] = "starting"
            return

        if msg_type == "stream_event":
            event = payload.get("event") or {}
            event_type = event.get("type")
            if event_type == "message_start":
                state["phase"] = "thinking"
            elif event_type == "content_block_start":
                block = event.get("content_block") or {}
                block_type = block.get("type")
                if block_type == "tool_use":
                    state["phase"] = "running tool"
                    state["step"] += 1
                    state["active_tool"] = block.get("name") or state["active_tool"]
                elif block_type == "text":
                    state["phase"] = "writing answer"
                elif block_type == "thinking":
                    state["phase"] = "thinking"
            elif event_type == "tool_progress":
                state["phase"] = "running tool"
            elif event_type == "message_stop":
                if state["phase"] != "completed":
                    state["phase"] = "thinking"
            return

        if msg_type == "assistant":
            blocks = (payload.get("message") or {}).get("content") or []
            usage = (payload.get("message") or {}).get("usage") or {}
            self._apply_qwen_usage(state, usage)
            if blocks and all(block.get("type") == "text" for block in blocks):
                state["phase"] = "writing answer"
                state["final_text"] += self._extract_text_from_blocks(blocks)
            for block in blocks:
                if block.get("type") == "tool_use":
                    tool_name = block.get("name") or "tool"
                    tool_id = block.get("id")
                    state["phase"] = "running tool"
                    state["step"] += 1
                    state["active_tool"] = tool_name
                    if tool_id:
                        state["tool_use_ids"][tool_id] = tool_name
            return

        if msg_type == "user":
            blocks = (payload.get("message") or {}).get("content") or []
            for block in blocks:
                if block.get("type") == "tool_result":
                    tool_use_id = block.get("tool_use_id")
                    state["active_tool"] = state["tool_use_ids"].get(
                        tool_use_id, state["active_tool"]
                    )
                    state["last_exit_code"] = 1 if block.get("is_error") else 0
                    state["phase"] = "thinking"
            return

        if msg_type == "result":
            state["phase"] = "completed"
            self._apply_qwen_usage(state, payload.get("usage") or {})
            if payload.get("is_error"):
                state["final_error"] = (
                    (payload.get("error") or {}).get("message") or "Unknown error"
                ).strip()
            else:
                state["final_text"] = (
                    payload.get("result") or state["final_text"]
                ).strip()

    def _apply_qwen_usage(self, state: dict, usage: dict):
        input_tokens = usage.get("input_tokens")
        output_tokens = usage.get("output_tokens")
        cached_tokens = usage.get("cache_read_input_tokens") or usage.get(
            "cached_input_tokens"
        )
        total_tokens = usage.get("total_tokens")
        if isinstance(input_tokens, int):
            state["input_tokens"] = input_tokens
        if isinstance(output_tokens, int):
            state["output_tokens"] = output_tokens
        if isinstance(cached_tokens, int):
            state["cached_tokens"] = cached_tokens
        if isinstance(total_tokens, int):
            state["total_tokens"] = total_tokens
        else:
            state["total_tokens"] = state["input_tokens"] + state["output_tokens"]

    def _extract_text_from_blocks(self, blocks) -> str:
        parts = []
        for block in blocks:
            if block.get("type") == "text" and block.get("text"):
                parts.append(block["text"])
        return "".join(parts)

    async def _run_qwen_device_auth(self, status_msg):
        device_code_endpoint = "https://chat.qwen.ai/api/v1/oauth2/device/code"
        token_endpoint = "https://chat.qwen.ai/api/v1/oauth2/token"
        client_id = "f0304373b74a44d2b584a3fb70ca9e56"
        scope = "openid profile email model.completion"
        grant_type = "urn:ietf:params:oauth:grant-type:device_code"
        code_verifier = self._generate_code_verifier()
        code_challenge = self._generate_code_challenge(code_verifier)
        try:
            device_auth = await asyncio.to_thread(
                self._post_form_json,
                device_code_endpoint,
                {
                    "client_id": client_id,
                    "scope": scope,
                    "code_challenge": code_challenge,
                    "code_challenge_method": "S256",
                },
            )
            verification_url = device_auth.get(
                "verification_uri_complete"
            ) or device_auth.get("verification_uri")
            device_code = device_auth.get("device_code")
            expires_in = int(device_auth.get("expires_in") or 600)
            if not verification_url or not device_code:
                return False, json.dumps(device_auth, ensure_ascii=False)[:600]
            with contextlib.suppress(Exception):
                await self._edit_html(
                    status_msg,
                    self.strings["qwen_auth_step"].format(
                        utils.escape_html(verification_url)
                    ),
                )
            poll_interval = 2.0
            deadline = asyncio.get_running_loop().time() + expires_in
            while asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(poll_interval)
                token_response = await asyncio.to_thread(
                    self._post_form_json,
                    token_endpoint,
                    {
                        "grant_type": grant_type,
                        "client_id": client_id,
                        "device_code": device_code,
                        "code_verifier": code_verifier,
                    },
                )
                if token_response.get("access_token"):
                    credentials = {
                        "access_token": token_response["access_token"],
                        "refresh_token": token_response.get("refresh_token") or "",
                        "token_type": token_response.get("token_type") or "Bearer",
                        "resource_url": token_response.get("resource_url"),
                        "expiry_date": int(datetime.utcnow().timestamp() * 1000)
                        + int(token_response.get("expires_in") or 3600) * 1000,
                    }
                    await self._cache_qwen_credentials(credentials)
                    return True, "ok"
                if token_response.get("error") == "authorization_pending":
                    continue
                if token_response.get("error") == "slow_down":
                    poll_interval = min(poll_interval * 1.5, 10.0)
                    continue
                return (
                    False,
                    f"{token_response.get('error', 'unknown_error')} - {token_response.get('error_description', '')}".strip(),
                )
            return False, "timeout"
        except Exception as e:
            return False, str(e)

    async def _cache_qwen_credentials(self, credentials: dict):
        qwen_dir = self._get_user_qwen_dir()
        os.makedirs(qwen_dir, exist_ok=True)
        path = os.path.join(qwen_dir, "oauth_creds.json")
        temp_path = f"{path}.tmp.{uuid.uuid4().hex[:8]}"
        with open(temp_path, "w", encoding="utf-8") as file_obj:
            json.dump(credentials, file_obj, ensure_ascii=False, indent=2)
        os.replace(temp_path, path)

    async def _answer_html(
        self, entity, text: str, reply_markup=None, link_preview: bool = False
    ):
        if isinstance(entity, InlineCall):
            with contextlib.suppress(TypeError):
                return await entity.edit(
                    text, reply_markup=reply_markup, parse_mode="html"
                )
            return await entity.edit(text, reply_markup=reply_markup)
        try:
            return await utils.answer(
                entity,
                text,
                reply_markup=reply_markup,
                parse_mode="html",
                link_preview=link_preview,
            )
        except TypeError:
            pass
        except Exception:
            pass
        if hasattr(entity, "edit"):
            with contextlib.suppress(Exception):
                return await entity.edit(
                    text,
                    parse_mode="html",
                    link_preview=link_preview,
                    reply_markup=reply_markup,
                )
        if isinstance(entity, Message):
            return await self.client.send_message(
                entity.chat_id,
                text,
                parse_mode="html",
                link_preview=link_preview,
                reply_to=getattr(entity, "id", None),
            )
        return await utils.answer(entity, text, reply_markup=reply_markup)

    async def _edit_html(
        self, entity, text: str, reply_markup=None, link_preview: bool = False
    ):
        if isinstance(entity, InlineCall):
            with contextlib.suppress(TypeError):
                return await entity.edit(
                    text=text, reply_markup=reply_markup, parse_mode="html"
                )
            return await entity.edit(text=text, reply_markup=reply_markup)
        if hasattr(entity, "edit"):
            with contextlib.suppress(TypeError):
                return await entity.edit(
                    text,
                    parse_mode="html",
                    link_preview=link_preview,
                    reply_markup=reply_markup,
                )
            with contextlib.suppress(Exception):
                return await entity.edit(text=text, reply_markup=reply_markup)
        return await self._answer_html(
            entity, text, reply_markup=reply_markup, link_preview=link_preview
        )

    def _format_qwen_status(self, state: dict) -> str:
        elapsed = max(0, int(asyncio.get_running_loop().time() - state["started_at"]))
        phase = state["phase"]
        phase_emoji = self._PHASE_EMOJI.get(
            phase, "<tg-emoji emoji-id=5415941463764667665>⏳</tg-emoji>"
        )
        session_suffix = (
            f" · <code>{utils.escape_html(state['session_id'][:8])}</code>"
            if state.get("session_id")
            else ""
        )
        cached_suffix = (
            f" (<code>{self._fmt_num(state['cached_tokens'])}</code>↩)"
            if state["cached_tokens"] > 0
            else ""
        )
        tool_line = ""
        if state["active_tool"]:
            exit_suffix = ""
            if state["last_exit_code"] is not None:
                exit_suffix = (
                    " <tg-emoji emoji-id=5330561907671727296>✅</tg-emoji>"
                    if state["last_exit_code"] == 0
                    else f" ❌ exit {state['last_exit_code']}"
                )
            tool_line = f"\n{self.strings['qwen_status_tool'].format(utils.escape_html(state['active_tool']), exit_suffix)}"
        error_line = (
            f"\n{self.strings['qwen_status_final_error'].format(utils.escape_html(state['final_error'][:160]))}"
            if state["final_error"]
            else ""
        )
        return (
            f"<blockquote>"
            f"{self.strings['qwen_status_title'].format(session_suffix, '<code>' + utils.escape_html(state.get('model', '')) + '</code>' if state.get('model') else '')}\n"
            f"{self.strings['qwen_status_phase'].format(phase_emoji, utils.escape_html(phase))} · "
            f"{self.strings['qwen_status_step'].format(state['step'], elapsed)}\n"
            f"{self.strings['qwen_status_tokens'].format(self._fmt_num(state['input_tokens']), cached_suffix, self._fmt_num(state['output_tokens']), self._fmt_num(state['total_tokens']))}"
            f"{tool_line}{error_line}"
            f"</blockquote>"
        )

    async def _update_qwen_status_message(
        self, entity, state: dict, force: bool = False
    ):
        now = asyncio.get_running_loop().time()
        text = self._format_qwen_status(state)
        if not force and now - state["last_status_at"] < 1.2:
            return
        if not force and text == state["last_status_text"]:
            return
        state["last_status_at"] = now
        state["last_status_text"] = text
        try:
            await self._edit_html(entity, text, reply_markup=None, link_preview=False)
        except Exception:
            pass

    async def _read_qwen_stdout_stream(
        self, stream, stdout_lines: list, state: dict, status_entity=None
    ):
        while True:
            line = await stream.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="ignore").strip()
            if not text:
                continue
            stdout_lines.append(text)
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue
            self._update_qwen_progress_state(state, payload)
            if status_entity:
                await self._update_qwen_status_message(status_entity, state)

    async def _read_qwen_stderr_stream(self, stream, stderr_lines: list, state: dict):
        while True:
            line = await stream.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="ignore").strip()
            if not text:
                continue
            stderr_lines.append(text)
            if "error" in text.lower() and not state["final_error"]:
                state["final_error"] = text[:300]

    def _collect_qwen_generated_files(
        self, tempdir: str, ignored_names=None, ignored_paths=None
    ) -> list:
        files = []
        ignored_names = ignored_names or set()
        ignored_paths = {os.path.abspath(path) for path in (ignored_paths or set())}
        for root, dirs, filenames in os.walk(tempdir):
            dirs[:] = [
                directory for directory in dirs if directory not in ignored_names
            ]
            for filename in filenames:
                path = os.path.join(root, filename)
                abs_path = os.path.abspath(path)
                rel = os.path.relpath(path, tempdir)
                if any(part in ignored_names for part in rel.split(os.sep)):
                    continue
                if abs_path in ignored_paths:
                    continue
                if not os.path.isfile(path):
                    continue
                with open(path, "rb") as file_obj:
                    files.append({"name": rel, "data": file_obj.read()})
        files.sort(key=lambda item: item["name"])
        return files

    async def _send_qwen_generated_files(
        self, chat_id: int, files: list, reply_to: int = None
    ):
        for file_info in files:
            file_obj = io.BytesIO(file_info["data"])
            file_obj.name = os.path.basename(file_info["name"]) or "qwen_file"
            await self.client.send_file(
                chat_id,
                file=file_obj,
                caption=self.strings["qwen_file_caption"].format(
                    utils.escape_html(file_info["name"])
                ),
                reply_to=reply_to,
                parse_mode="html",
            )

    async def _create_processing_status(self, message: Message, text: str):
        if self.config["interactive_buttons"]:
            with contextlib.suppress(Exception):
                form = await self.inline.form(text=text, message=message, silent=True)
                if form:
                    return form
        return await self._answer_html(message, text)

    async def _edit_processing_status(self, entity, text: str):
        await self._edit_html(entity, text, reply_markup=None, link_preview=False)

    async def _prepare_request_payload(self, message: Message, custom_text: str = None):
        warnings = []
        prompt_chunks = []
        file_specs = []
        user_args = (
            custom_text
            if custom_text is not None
            else utils.get_args_raw(message).strip()
        )
        reply = await message.get_reply_message()

        if reply and getattr(reply, "text", None):
            try:
                reply_sender = await reply.get_sender()
                reply_author_name = (
                    get_display_name(reply_sender) if reply_sender else "Unknown"
                )
                prompt_chunks.append(
                    f"{reply_author_name}: {utils.remove_html(reply.text)}"
                )
            except Exception:
                prompt_chunks.append(f"Ответ на: {utils.remove_html(reply.text)}")

        try:
            current_sender = await message.get_sender()
            current_user_name = (
                get_display_name(current_sender) if current_sender else "User"
            )
        except Exception:
            current_user_name = "User"

        media_source = message if (message.media or message.sticker) else reply
        has_media = bool(media_source and (media_source.media or media_source.sticker))
        if has_media:
            if media_source.sticker:
                alt_text = "?"
                attrs = getattr(media_source.sticker, "attributes", []) or []
                alt_text = next(
                    (
                        attr.alt
                        for attr in attrs
                        if isinstance(attr, DocumentAttributeSticker)
                    ),
                    "?",
                )
                prompt_chunks.append(f"[Стикер: {alt_text}]")
            elif media_source.photo:
                data = await self.client.download_media(media_source, bytes)
                file_specs.append(
                    {"name": "input/photo.jpg", "data": data, "type": "image"}
                )
            elif getattr(media_source, "document", None):
                mime_type = (
                    getattr(
                        media_source.document, "mime_type", "application/octet-stream"
                    )
                    or "application/octet-stream"
                )
                doc_attr = next(
                    (
                        attr
                        for attr in media_source.document.attributes
                        if isinstance(attr, DocumentAttributeFilename)
                    ),
                    None,
                )
                filename = doc_attr.file_name if doc_attr else "file"
                if mime_type.startswith("image/"):
                    data = await self.client.download_media(media_source, bytes)
                    safe_name = (
                        re.sub(r"[^a-zA-Z0-9._-]+", "_", filename) or "image.bin"
                    )
                    file_specs.append(
                        {"name": f"input/{safe_name}", "data": data, "type": "image"}
                    )
                elif mime_type in TEXT_MIME_TYPES or filename.split(".")[
                    -1
                ].lower() in {"txt", "py", "js", "json", "md", "html", "css", "sh"}:
                    try:
                        data = await self.client.download_media(media_source, bytes)
                        file_content = data.decode("utf-8")
                        prompt_chunks.insert(
                            0,
                            f"[Содержимое файла '{filename}']:\n```\n{file_content}\n```",
                        )
                    except Exception as e:
                        warnings.append(
                            f"<tg-emoji emoji-id=5409235172979672859>⚠️</tg-emoji> Ошибка чтения файла '{filename}': {e}"
                        )
                else:
                    warnings.append(
                        self.strings["unsupported_media"].format(
                            utils.escape_html(mime_type)
                        )
                    )

        if user_args:
            prompt_chunks.append(f"{current_user_name}: {user_args}")
        elif file_specs:
            prompt_chunks.append(
                f"{current_user_name}: Изучи приложенные файлы и ответь по ним."
            )
        elif reply and getattr(reply, "text", None):
            prompt_chunks.append(f"{current_user_name}: Ответь на сообщение выше.")

        prompt_text = "\n".join(
            chunk for chunk in prompt_chunks if chunk and chunk.strip()
        ).strip()
        if not prompt_text and not file_specs:
            return None, warnings

        return {
            "text": prompt_text,
            "files": file_specs,
            "display_prompt": user_args
            or (
                reply.text[:200]
                if reply and getattr(reply, "text", None)
                else self.strings["media_reply_placeholder"]
            ),
        }, warnings

    def _build_qwen_prompt(
        self,
        chat_id: int,
        payload: dict,
        system_prompt: str = None,
        auto: bool = False,
        history_override=None,
    ):
        history = (
            self._get_structured_history(chat_id, auto=auto)
            if history_override is None
            else history_override
        )
        lines = [
            "Ты отвечаешь внутри Telegram-модуля.",
            "Если запрос требует действий в рабочей директории, используй инструменты Qwen CLI и реально выполняй нужные шаги.",
            "Если пользователь просит файл, конфиг, архив, скрипт или другой артефакт для отправки, создай нужный файл в рабочей директории.",
            "Верни только финальный ответ для пользователя без служебных пояснений.",
        ]
        if system_prompt:
            lines.append("ДОПОЛНИТЕЛЬНЫЕ ИНСТРУКЦИИ:")
            lines.append(system_prompt.strip())
        if history:
            lines.append("ИСТОРИЯ ДИАЛОГА:")
            for entry in history:
                role = "ASSISTANT" if entry.get("role") == "assistant" else "USER"
                content = entry.get("content", "")
                if content:
                    lines.append(f"{role}: {content}")
        file_specs = payload.get("files") or []
        if file_specs:
            lines.append("ПРИЛОЖЕННЫЕ ФАЙЛЫ:")
            for spec in file_specs:
                lines.append(f"@{spec['name']}")
        lines.append("")
        lines.append("ТЕКУЩИЙ ЗАПРОС:")
        lines.append(
            self._prepend_now_note(
                payload.get("text")
                or "Обработай приложенные файлы и ответь пользователю."
            )
        )
        return "\n".join(lines), file_specs

    def _get_proxy(self):
        proxy = self.config["proxy"].strip()
        return proxy or None

    def _get_bootstrap_base_dir(self):
        home = os.path.expanduser("~")
        xdg_data_home = os.environ.get("XDG_DATA_HOME")
        if os.name == "nt":
            base = os.environ.get("LOCALAPPDATA") or os.environ.get("APPDATA") or home
            return os.path.join(base, "QwenCLI")
        if xdg_data_home:
            return os.path.join(xdg_data_home, "qwencli")
        return os.path.join(home, ".local", "share", "qwencli")

    def _get_local_node_dir(self):
        return os.path.join(self._get_bootstrap_base_dir(), "node")

    def _get_local_qwen_prefix(self):
        return os.path.join(self._get_bootstrap_base_dir(), "qwen")

    def _get_local_node_binary(self):
        name = "node.exe" if os.name == "nt" else "node"
        return os.path.join(self._get_local_node_dir(), "bin", name)

    def _get_local_npm_binary(self):
        name = "npm.cmd" if os.name == "nt" else "npm"
        return os.path.join(self._get_local_node_dir(), "bin", name)

    def _get_local_qwen_binary(self):
        name = "qwen.cmd" if os.name == "nt" else "qwen"
        return os.path.join(self._get_local_qwen_prefix(), "bin", name)

    def _get_platform_node_target(self):
        sys_name = platform.system().lower()
        machine = platform.machine().lower()
        if sys_name == "linux":
            plat = "linux"
        elif sys_name == "darwin":
            plat = "darwin"
        elif sys_name == "windows":
            plat = "win"
        else:
            raise RuntimeError(
                f"Неподдерживаемая ОС для bootstrap: {platform.system()}"
            )
        arch_map = {
            "x86_64": "x64",
            "amd64": "x64",
            "aarch64": "arm64",
            "arm64": "arm64",
        }
        arch = arch_map.get(machine)
        if not arch:
            raise RuntimeError(
                f"Неподдерживаемая архитектура для bootstrap: {platform.machine()}"
            )
        ext = "zip" if plat == "win" else "tar.xz"
        return plat, arch, ext

    async def _ensure_qwen_cli_available(self, force: bool = False):
        local_qwen = self._get_local_qwen_binary()
        if not force:
            if os.path.isfile(local_qwen):
                self.config["qwen_path"] = local_qwen
                return
            if self._get_qwen_binary():
                self._pin_detected_qwen_path()
                return
        if not self.config["auto_bootstrap"] and not force:
            return
        async with self._install_lock:
            if not force and os.path.isfile(local_qwen):
                self.config["qwen_path"] = local_qwen
                return
            if not force and self._get_qwen_binary():
                self._pin_detected_qwen_path()
                return
            await self._ensure_local_node()
            await self._ensure_local_qwen_cli()
            self.config["qwen_path"] = local_qwen
            ok, details = await self._verify_qwen_installation()
            if not ok:
                raise RuntimeError(
                    self.strings["bootstrap_verify_fail"].format(
                        utils.escape_html(details)
                    )
                )

    async def _ensure_local_node(self):
        node_bin = self._get_local_node_binary()
        if os.path.isfile(node_bin):
            return
        base_dir = self._get_bootstrap_base_dir()
        os.makedirs(base_dir, exist_ok=True)
        version = await self._resolve_node_version()
        plat, arch, ext = self._get_platform_node_target()
        archive_name = f"node-{version}-{plat}-{arch}.{ext}"
        url = f"https://nodejs.org/dist/{version}/{archive_name}"
        with tempfile.TemporaryDirectory(prefix="qwen_node_") as tempdir:
            archive_path = os.path.join(tempdir, archive_name)
            await self._download_file(url, archive_path)
            extract_dir = os.path.join(tempdir, "extract")
            os.makedirs(extract_dir, exist_ok=True)
            await asyncio.to_thread(self._extract_archive, archive_path, extract_dir)
            inner = self._find_single_directory(extract_dir)
            target_dir = self._get_local_node_dir()
            if os.path.exists(target_dir):
                shutil.rmtree(target_dir, ignore_errors=True)
            shutil.move(inner, target_dir)
            self._chmod_tree(target_dir)

    async def _ensure_local_qwen_cli(self):
        qwen_bin = self._get_local_qwen_binary()
        if os.path.isfile(qwen_bin):
            return
        node_bin = self._get_local_node_binary()
        npm_bin = self._get_local_npm_binary()
        if not os.path.isfile(node_bin) or not os.path.isfile(npm_bin):
            raise RuntimeError("Локальный Node.js не был подготовлен.")
        prefix = self._get_local_qwen_prefix()
        os.makedirs(prefix, exist_ok=True)
        env = self._build_subprocess_env()
        path_parts = [
            os.path.dirname(node_bin),
            os.path.dirname(qwen_bin),
            env.get("PATH", ""),
        ]
        env["PATH"] = os.pathsep.join([part for part in path_parts if part])
        env["npm_config_prefix"] = prefix
        env["NPM_CONFIG_PREFIX"] = prefix
        proc = await asyncio.create_subprocess_exec(
            npm_bin,
            "install",
            "-g",
            "@qwen-code/qwen-code@latest",
            "--prefix",
            prefix,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(
                (stderr or stdout).decode("utf-8", errors="ignore").strip()
                or "Не удалось установить Qwen CLI."
            )
        self._chmod_tree(prefix)
        if not os.path.isfile(qwen_bin):
            raise RuntimeError("Установка завершилась без qwen binary.")

    async def _verify_qwen_installation(self):
        qwen_bin = self._get_qwen_binary()
        if not qwen_bin:
            return False, "binary not found"
        env = self._build_subprocess_env()
        for argv in ([qwen_bin, "--version"], [qwen_bin, "--help"]):
            proc = await asyncio.create_subprocess_exec(
                *argv,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
            stdout, stderr = await proc.communicate()
            text = "\n".join(
                part
                for part in [
                    stdout.decode("utf-8", errors="ignore").strip(),
                    stderr.decode("utf-8", errors="ignore").strip(),
                ]
                if part
            ).strip()
            if proc.returncode != 0:
                return False, text or f"exit={proc.returncode}"
            if argv[-1] == "--help" and "Qwen Code" not in text:
                return False, text[:400]
        return True, qwen_bin

    def _pin_detected_qwen_path(self):
        qwen_bin = self._get_qwen_binary()
        if qwen_bin and self.config["qwen_path"] != qwen_bin:
            self.config["qwen_path"] = qwen_bin

    async def _resolve_node_version(self):
        url = "https://nodejs.org/dist/index.json"
        raw = await asyncio.to_thread(self._read_url_bytes, url)
        data = json.loads(raw.decode("utf-8"))
        for item in data:
            version = item.get("version")
            if isinstance(version, str) and re.match(r"^v20\.", version):
                return version
        raise RuntimeError("Не удалось определить доступную версию Node.js 20.x.")

    async def _download_file(self, url: str, dest_path: str):
        data = await asyncio.to_thread(self._read_url_bytes, url)
        with open(dest_path, "wb") as file_obj:
            file_obj.write(data)

    def _read_url_bytes(self, url: str) -> bytes:
        headers = {"User-Agent": "QwenCLI-Bootstrap/1.0"}
        request_obj = urllib_request.Request(url, headers=headers)
        proxy = self._get_proxy()
        opener = None
        if proxy:
            opener = urllib_request.build_opener(
                urllib_request.ProxyHandler({"http": proxy, "https": proxy})
            )
        try:
            if opener:
                with opener.open(request_obj, timeout=60) as resp:
                    return resp.read()
            with urllib_request.urlopen(request_obj, timeout=60) as resp:
                return resp.read()
        except urllib_error.URLError as e:
            raise RuntimeError(f"Ошибка загрузки {url}: {e}") from e

    def _post_form_json(self, url: str, data: dict) -> dict:
        headers = {
            "User-Agent": "QwenCLI-Auth/1.0",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }
        body = urllib_parse.urlencode(data).encode("utf-8")
        request_obj = urllib_request.Request(
            url, headers=headers, data=body, method="POST"
        )
        proxy = self._get_proxy()
        opener = None
        if proxy:
            opener = urllib_request.build_opener(
                urllib_request.ProxyHandler({"http": proxy, "https": proxy})
            )
        try:
            if opener:
                with opener.open(request_obj, timeout=60) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            with urllib_request.urlopen(request_obj, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib_error.HTTPError as e:
            raw = e.read().decode("utf-8", errors="ignore")
            try:
                parsed = json.loads(raw)
            except Exception:
                raise RuntimeError(f"{e.code} {e.reason}: {raw}") from e
            return parsed
        except urllib_error.URLError as e:
            raise RuntimeError(f"Ошибка сети при запросе {url}: {e}") from e

    @staticmethod
    def _generate_code_verifier():
        return uuid.uuid4().hex + uuid.uuid4().hex

    @staticmethod
    def _generate_code_challenge(code_verifier: str):
        digest = sha256(code_verifier.encode("utf-8")).digest()
        import base64

        return base64.urlsafe_b64encode(digest).decode("utf-8").rstrip("=")

    def _extract_archive(self, archive_path: str, extract_dir: str):
        if archive_path.endswith(".zip"):
            with zipfile.ZipFile(archive_path) as archive:
                for member in archive.infolist():
                    self._validate_extract_path(extract_dir, member.filename)
                    archive.extract(member, extract_dir)
            return
        if archive_path.endswith(".tar.xz") or archive_path.endswith(".tar.gz"):
            with tarfile.open(archive_path) as archive:
                for member in archive.getmembers():
                    self._validate_extract_path(extract_dir, member.name)
                    archive.extract(member, extract_dir)
            return
        raise RuntimeError(f"Неизвестный формат архива: {archive_path}")

    def _validate_extract_path(self, extract_dir: str, member_name: str):
        normalized = os.path.abspath(os.path.join(extract_dir, member_name))
        base = os.path.abspath(extract_dir)
        if normalized != base and not normalized.startswith(base + os.sep):
            raise RuntimeError("Архив содержит небезопасный путь распаковки.")

    def _find_single_directory(self, extract_dir: str):
        entries = [os.path.join(extract_dir, name) for name in os.listdir(extract_dir)]
        dirs = [entry for entry in entries if os.path.isdir(entry)]
        if len(dirs) == 1:
            return dirs[0]
        raise RuntimeError(
            "Не удалось определить корневую директорию распакованного архива."
        )

    def _chmod_tree(self, path: str):
        if os.name == "nt":
            return
        for root, dirs, files in os.walk(path):
            for name in dirs:
                full = os.path.join(root, name)
                mode = os.stat(full).st_mode
                os.chmod(full, mode | stat.S_IXUSR)
            for name in files:
                full = os.path.join(root, name)
                mode = os.stat(full).st_mode
                if (
                    "/bin/" in full
                    or full.endswith("/qwen")
                    or full.endswith("/node")
                    or full.endswith("/npm")
                    or full.endswith("/npx")
                ):
                    os.chmod(full, mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    def _build_subprocess_env(self):
        env = os.environ.copy()
        proxy = self._get_proxy()
        if proxy:
            for key in [
                "HTTP_PROXY",
                "HTTPS_PROXY",
                "ALL_PROXY",
                "http_proxy",
                "https_proxy",
                "all_proxy",
            ]:
                env[key] = proxy
        local_paths = [
            os.path.dirname(self._get_local_node_binary()),
            os.path.dirname(self._get_local_qwen_binary()),
        ]
        
        if os.name != "nt":
            wrapper_dir = os.path.join(self._get_bootstrap_base_dir(), "wrapper")
            os.makedirs(wrapper_dir, exist_ok=True)
            wrapper_path = os.path.join(wrapper_dir, "node")
            if not os.path.isfile(wrapper_path):
                with open(wrapper_path, "w") as f:
                    f.write(f'#!/bin/bash\nexec {self._get_local_node_binary()} --disable-wasm-trap-handler "$@"\n')
                os.chmod(wrapper_path, 0o755)
            local_paths.insert(0, wrapper_dir)

        env["PATH"] = os.pathsep.join(
            [part for part in local_paths + [env.get("PATH", "")] if part]
        )
        node_options = env.get("NODE_OPTIONS", "").strip()
        heap_flag = "--max-old-space-size=128"
        if heap_flag not in node_options:
            node_options = (node_options + " " + heap_flag).strip()
        env["NODE_OPTIONS"] = node_options
        env["CI"] = "1"
        env["NO_COLOR"] = "1"
        env["FORCE_COLOR"] = "0"
        return env

    async def _terminate_process(self, proc):
        if proc.returncode is not None:
            return
        with contextlib.suppress(ProcessLookupError):
            if os.name != "nt":
                os.killpg(proc.pid, signal.SIGTERM)
            else:
                proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
            return
        except Exception:
            pass
        with contextlib.suppress(ProcessLookupError):
            if os.name != "nt":
                os.killpg(proc.pid, signal.SIGKILL)
            else:
                proc.kill()
        with contextlib.suppress(Exception):
            await proc.wait()

    def _prepend_now_note(self, text: str) -> str:
        if not text:
            return text
        tz = self._get_timezone()
        now = datetime.now(tz) if tz else datetime.utcnow()
        stamp = now.strftime("%Y-%m-%d %H:%M:%S %Z").strip()
        return f"[System Info: Current local time is {stamp}]\n\n{text}"

    def _get_timezone(self):
        if not pytz:
            return None
        try:
            return pytz.timezone(self.config["timezone"])
        except Exception:
            return pytz.utc

    def _iter_qwen_binary_candidates(self):
        configured = self.config["qwen_path"].strip()
        if configured:
            yield configured
        yield self._get_local_qwen_binary()
        if os.name == "nt":
            for name in ["qwen.exe", "qwen.cmd", "qwen.bat", "qwen"]:
                yield name
        else:
            yield "qwen"
        home = os.path.expanduser("~")
        user_local_bin = os.path.join(home, ".local", "bin")
        user_bin = os.path.join(home, "bin")
        npm_prefix_bin = os.path.join(home, ".npm-global", "bin")
        candidates = [
            os.path.join(user_local_bin, "qwen"),
            os.path.join(user_bin, "qwen"),
            os.path.join(npm_prefix_bin, "qwen"),
            os.path.join(user_local_bin, "qwen.cmd"),
            os.path.join(user_bin, "qwen.cmd"),
            os.path.join(npm_prefix_bin, "qwen.cmd"),
        ]
        appdata = os.environ.get("APPDATA")
        if appdata:
            candidates.extend(
                [
                    os.path.join(appdata, "npm", "qwen.cmd"),
                    os.path.join(appdata, "npm", "qwen"),
                ]
            )
        for item in candidates:
            yield item

    def _resolve_binary_candidate(self, candidate: str):
        if not candidate:
            return None
        if os.path.isabs(candidate) or any(sep in candidate for sep in ("/", "\\")):
            return (
                candidate
                if os.path.isfile(candidate) and os.access(candidate, os.X_OK)
                else None
            )
        return shutil.which(candidate)

    def _get_qwen_binary(self):
        seen = set()
        for candidate in self._iter_qwen_binary_candidates():
            resolved = self._resolve_binary_candidate(candidate)
            if resolved and resolved not in seen:
                seen.add(resolved)
                return resolved
        return None

    def _prepare_qwen_runtime_home(self, tempdir: str) -> str:
        runtime_home = os.path.join(tempdir, "runtime-home")
        runtime_qwen = os.path.join(runtime_home, ".qwen")
        os.makedirs(runtime_qwen, exist_ok=True)
        source_qwen = self._get_user_qwen_dir()
        for name in [
            "oauth_creds.json",
            "installation_id",
            "google_accounts.json",
            "output-language.md",
        ]:
            src = os.path.join(source_qwen, name)
            dst = os.path.join(runtime_qwen, name)
            if os.path.exists(src):
                with open(src, "rb") as src_f, open(dst, "wb") as dst_f:
                    dst_f.write(src_f.read())
        settings = {}
        settings_path = os.path.join(source_qwen, "settings.json")
        if os.path.exists(settings_path):
            with contextlib.suppress(Exception):
                with open(settings_path, "r", encoding="utf-8") as file_obj:
                    settings = json.load(file_obj) or {}
        if not isinstance(settings, dict):
            settings = {}
        security = settings.setdefault("security", {})
        auth = security.setdefault("auth", {})
        auth["selectedType"] = self.config["auth_type"]
        model = settings.setdefault("model", {})
        model["name"] = (self.config["qwen_model"] or "coder-model").strip()
        settings["$version"] = settings.get("$version", 3)
        with open(
            os.path.join(runtime_qwen, "settings.json"), "w", encoding="utf-8"
        ) as file_obj:
            json.dump(settings, file_obj, ensure_ascii=False, indent=2)
        return runtime_home

    def _get_user_qwen_dir(self):
        home = os.path.expanduser("~")
        xdg_state_home = os.environ.get("XDG_STATE_HOME")
        candidates = [
            os.path.join(home, ".qwen"),
        ]
        if xdg_state_home:
            candidates.append(os.path.join(xdg_state_home, "qwen"))
        appdata = os.environ.get("APPDATA")
        if appdata:
            candidates.append(os.path.join(appdata, "QwenCode"))
            candidates.append(os.path.join(appdata, ".qwen"))
        localappdata = os.environ.get("LOCALAPPDATA")
        if localappdata:
            candidates.append(os.path.join(localappdata, "QwenCode"))
            candidates.append(os.path.join(localappdata, ".qwen"))
        for path in candidates:
            if os.path.isdir(path):
                return path
        return candidates[0]

    async def _get_qwen_status_for_runtime(self):
        await self._ensure_qwen_cli_available()
        if not self._get_qwen_binary():
            return False, self.strings["qwen_not_found"]
        auth_type = self.config["auth_type"]
        if auth_type == "qwen-oauth":
            oauth_path = os.path.join(self._get_user_qwen_dir(), "oauth_creds.json")
            if not os.path.exists(oauth_path):
                return False, self.strings["qwen_oauth_missing"]
            return True, "qwen-oauth"
        return False, self.strings["qwen_auth_missing"]

    async def _format_auth_status(self):
        ready, status = await self._get_qwen_status_for_runtime()
        out = [self.strings["status_title"]]
        out.append(
            self.strings["status_qwen"].format(
                self.strings["status_ready"]
                if self._get_qwen_binary()
                else self.strings["status_not_ready"]
            )
        )
        out.append(
            self.strings["status_auth_type"].format(
                utils.escape_html(self.config["auth_type"])
            )
        )
        out.append(
            self.strings["status_model"].format(
                utils.escape_html(self.config["qwen_model"] or "coder-model")
            )
        )
        out.append(f"• Runtime: {'готов' if ready else 'не готов'}")
        if status:
            out.append(f"<code>{utils.escape_html(str(status)[:400])}</code>")
        return "\n".join(out)

    def _handle_error(self, e: Exception) -> str:
        logger.exception("QwenCLI execution error")
        msg = str(e)
        if msg.startswith(
            "<tg-emoji emoji-id=5332431395266524007>❗️</tg-emoji>"
        ) or msg.startswith("<tg-emoji emoji-id=5409235172979672859>⚠️</tg-emoji>"):
            return msg
        return self.strings["generic_error"].format(utils.escape_html(msg))

    def _save_history_sync(self, auto: bool = False):
        data, key = (
            (self.auto_conversations, DB_GAUTO_HISTORY_KEY)
            if auto
            else (self.conversations, DB_HISTORY_KEY)
        )
        self.db.set(self.strings["name"], key, data)

    def _load_history_from_db(self, key):
        data = self.db.get(self.strings["name"], key, {})
        return data if isinstance(data, dict) else {}

    def _migrate_runtime_lists_to_config(self):
        if not self.config["auto_reply_chats"].strip() and self.impersonation_chats:
            self.config["auto_reply_chats"] = "\n".join(
                str(chat_id) for chat_id in sorted(self.impersonation_chats, key=str)
            )
        if (
            not self.config["memory_disabled_chats"].strip()
            and self.memory_disabled_chats
        ):
            self.config["memory_disabled_chats"] = "\n".join(
                sorted(
                    (str(chat_id) for chat_id in self.memory_disabled_chats), key=str
                )
            )

    def _split_cfg_chat_values(self, raw: str):
        return [
            item.strip() for item in re.split(r"[\n,;]+", raw or "") if item.strip()
        ]

    async def _resolve_cfg_chat_values(self, raw: str):
        resolved = set()
        for item in self._split_cfg_chat_values(raw):
            if item.lstrip("-").isdigit():
                resolved.add(int(item))
                continue
            try:
                entity = await self.client.get_entity(item)
                resolved.add(entity.id)
            except Exception:
                logger.warning(
                    "QwenCLI: не удалось разрешить chat target из cfg: %s", item
                )
        return resolved

    async def _sync_runtime_config(self, force: bool = False):
        auto_raw = self.config["auto_reply_chats"]
        if force or self._cfg_sync_cache.get("auto_reply_chats") != auto_raw:
            self.impersonation_chats = await self._resolve_cfg_chat_values(auto_raw)
            self.db.set(
                self.strings["name"],
                DB_IMPERSONATION_KEY,
                list(sorted(self.impersonation_chats, key=str)),
            )
            self._cfg_sync_cache["auto_reply_chats"] = auto_raw

        memory_raw = self.config["memory_disabled_chats"]
        if force or self._cfg_sync_cache.get("memory_disabled_chats") != memory_raw:
            resolved_memory = await self._resolve_cfg_chat_values(memory_raw)
            self.memory_disabled_chats = {str(chat_id) for chat_id in resolved_memory}
            self.db.set(
                self.strings["name"],
                DB_MEMORY_DISABLED_KEY,
                list(sorted(self.memory_disabled_chats)),
            )
            self._cfg_sync_cache["memory_disabled_chats"] = memory_raw

        max_concurrent = int(self.config["max_concurrent_requests"])
        if (
            force
            or self._runtime_limits_cache.get("max_concurrent_requests")
            != max_concurrent
        ):
            if not self._active_processes:
                self._request_semaphore = asyncio.Semaphore(max_concurrent)
                self._runtime_limits_cache["max_concurrent_requests"] = max_concurrent

    async def _update_chat_list_config(self, key: str, target, enabled: bool):
        values = []
        seen = set()
        for item in self._split_cfg_chat_values(self.config[key]):
            if item == str(target):
                continue
            if item not in seen:
                values.append(item)
                seen.add(item)
        if enabled and str(target) not in seen:
            values.append(str(target))
        self.config[key] = "\n".join(values)
        await self._sync_runtime_config(force=True)

    def _get_structured_history(self, cid, auto: bool = False):
        data = self.auto_conversations if auto else self.conversations
        if str(cid) not in data:
            data[str(cid)] = []
        return data[str(cid)]

    def _update_history(
        self,
        chat_id: int,
        payload: dict,
        model_response: str,
        regeneration: bool = False,
        message: Message = None,
        auto: bool = False,
    ):
        if not self._is_memory_enabled(str(chat_id)):
            return
        history = self._get_structured_history(chat_id, auto)
        now = int(datetime.utcnow().timestamp())
        user_id = self.me.id
        user_name = get_display_name(self.me)
        message_id = getattr(message, "id", None)
        if message:
            try:
                peer_id = get_peer_id(message)
                if peer_id:
                    user_id = peer_id
            except Exception:
                if message.sender_id:
                    user_id = message.sender_id
            if getattr(message, "sender", None):
                user_name = get_display_name(message.sender)
        user_text = payload.get("text") or self.strings["media_reply_placeholder"]
        if regeneration and history:
            for idx in range(len(history) - 1, -1, -1):
                if history[idx].get("role") == "assistant":
                    history[idx].update({"content": model_response, "date": now})
                    break
        else:
            history.extend(
                [
                    {
                        "role": "user",
                        "type": "text",
                        "content": user_text,
                        "date": now,
                        "user_id": user_id,
                        "message_id": message_id,
                        "user_name": user_name,
                    },
                    {
                        "role": "assistant",
                        "type": "text",
                        "content": model_response,
                        "date": now,
                        "user_id": None,
                    },
                ]
            )
        limit = self.config["max_history_length"]
        if limit > 0 and len(history) > limit * 2:
            history = history[-(limit * 2) :]
        target = self.auto_conversations if auto else self.conversations
        target[str(chat_id)] = history
        self._save_history_sync(auto)

    def _clear_history(self, cid, auto: bool = False):
        data = self.auto_conversations if auto else self.conversations
        if str(cid) in data:
            del data[str(cid)]
            self._save_history_sync(auto)

    async def _get_recent_chat_text(self, cid, count=None, skip_last=False):
        limit = (count or self.config["impersonation_history_limit"]) + (
            1 if skip_last else 0
        )
        lines = []
        try:
            messages = await self.client.get_messages(cid, limit=limit)
            if skip_last and messages:
                messages = messages[1:]
            for item in messages:
                if not item:
                    continue
                if not (
                    item.text or item.sticker or item.photo or item.file or item.media
                ):
                    continue
                name = get_display_name(await item.get_sender()) or "Unknown"
                txt = item.text or ""
                if item.sticker:
                    alt = "?"
                    if hasattr(item.sticker, "attributes"):
                        alt = next(
                            (
                                attr.alt
                                for attr in item.sticker.attributes
                                if isinstance(attr, DocumentAttributeSticker)
                            ),
                            "?",
                        )
                    txt += f" [Стикер: {alt}]"
                elif item.photo:
                    txt += " [Фото]"
                elif item.file:
                    txt += " [Файл]"
                elif item.media and not txt:
                    txt += " [Медиа]"
                if txt.strip():
                    lines.append(f"{name}: {txt.strip()}")
        except Exception:
            pass
        return "\n".join(reversed(lines))

    def _find_preset(self, query):
        if not query:
            return None
        if str(query).isdigit():
            idx = int(query) - 1
            if 0 <= idx < len(self.prompt_presets):
                return self.prompt_presets[idx]
        for preset in self.prompt_presets:
            if preset["name"].lower() == str(query).lower():
                return preset
        return None

    def _markdown_to_html(self, text: str) -> str:
        def heading_replacer(match):
            level = len(match.group(1))
            title = match.group(2).strip()
            indent = "   " * (level - 1)
            return f"{indent}<b>{title}</b>"

        text = re.sub(r"^(#+)\s+(.*)", heading_replacer, text, flags=re.MULTILINE)
        text = re.sub(
            r"^([ \t]*)[-*+]\s+", lambda m: f"{m.group(1)}• ", text, flags=re.MULTILINE
        )
        if MarkdownIt:
            md = MarkdownIt("commonmark", {"html": False, "linkify": True})
            md.enable("strikethrough")
            md.disable("hr")
            md.disable("heading")
            md.disable("list")
            html_text = md.render(text)
        else:
            html_text = utils.escape_html(text).replace("\n", "<br>")

        def format_code(match):
            lang = utils.escape_html(match.group(1).strip())
            code = utils.escape_html(match.group(2).strip())
            return (
                f'<pre><code class="language-{lang}">{code}</code></pre>'
                if lang
                else f"<pre><code>{code}</code></pre>"
            )

        html_text = re.sub(r"```(.*?)\n([\s\S]+?)\n```", format_code, html_text)
        html_text = re.sub(
            r"<p>(<pre>[\s\S]*?</pre>)</p>", r"\1", html_text, flags=re.DOTALL
        )
        html_text = re.sub(r"<br\s*/?>", "\n", html_text, flags=re.IGNORECASE)
        html_text = html_text.replace("<p>", "").replace("</p>", "\n").strip()
        return html_text

    def _format_response_with_smart_separation(
        self, text: str, expandable: bool = True
    ) -> str:
        parts = re.split(r"(<pre.*?>[\s\S]*?</pre>)", text, flags=re.DOTALL)
        result_parts = []
        blockquote_open = (
            '<blockquote expandable="true">' if expandable else "<blockquote>"
        )
        for idx, part in enumerate(parts):
            if not part or part.isspace():
                continue
            if idx % 2 == 1:
                result_parts.append(part.strip())
            else:
                stripped = part.strip()
                if stripped:
                    result_parts.append(f"{blockquote_open}{stripped}</blockquote>")
        return "\n".join(result_parts)

    def _get_reply_target_id(self, message: Message, fallback: int = None) -> int:
        if message is None:
            return fallback
        reply_to_id = getattr(message, "reply_to_msg_id", None)
        if reply_to_id:
            return reply_to_id
        reply_to = getattr(message, "reply_to", None)
        if reply_to is not None:
            nested_reply_id = getattr(reply_to, "reply_to_msg_id", None)
            if nested_reply_id:
                return nested_reply_id
        return getattr(message, "id", None) or fallback

    def _get_inline_buttons(self, chat_id, base_message_id):
        return [
            [
                {
                    "text": self.strings["btn_clear"],
                    "callback": self._clear_callback,
                    "args": (chat_id,),
                    "icon_custom_emoji_id": "6007942490076745785",
                },
                {
                    "text": self.strings["btn_regenerate"],
                    "callback": self._regenerate_callback,
                    "args": (base_message_id, chat_id),
                    "icon_custom_emoji_id": "5404857686477015710",
                },
            ]
        ]

    def _get_error_buttons(self, chat_id, base_message_id):
        return [
            [
                {
                    "text": self.strings["btn_retry_request"],
                    "callback": self._regenerate_callback,
                    "args": (base_message_id, chat_id),
                    "icon_custom_emoji_id": "5404857686477015710",
                },
                {
                    "text": self.strings["btn_cancel_request"],
                    "callback": self._cancel_request_callback,
                    "args": (base_message_id, chat_id),
                    "icon_custom_emoji_id": "5350470691701407492",
                },
            ]
        ]

    async def _clear_callback(self, call: InlineCall, chat_id: int):
        self._clear_history(chat_id, auto=False)
        await self._edit_html(call, self.strings["memory_cleared"], reply_markup=None)

    async def _regenerate_callback(self, call: InlineCall, mid, cid):
        key = f"{cid}:{mid}"
        if key not in self.last_requests:
            return await call.answer(self.strings["no_last_request"], show_alert=True)
        payload, _ = self.last_requests[key]
        await self._send_request(
            mid, payload, regeneration=True, call=call, chat_id_override=cid
        )

    async def _cancel_request_callback(self, call: InlineCall, mid, cid):
        self.last_requests.pop(f"{cid}:{mid}", None)
        await self._edit_html(
            call, self.strings["request_cancelled"], reply_markup=None
        )

    async def _close_callback(self, call: InlineCall, uid: str):
        await call.answer()
        self.pager_cache.pop(uid, None)
        try:
            await self.client.delete_messages(call.chat_id, call.message_id)
        except Exception:
            with contextlib.suppress(Exception):
                await self._edit_html(call, "✔️ Сессия закрыта.", reply_markup=None)

    async def _render_page(self, uid, page_num, entity):
        data = self.pager_cache.get(uid)
        if not data:
            if isinstance(entity, InlineCall):
                await self._edit_html(
                    entity,
                    "<tg-emoji emoji-id=5409235172979672859>⚠️</tg-emoji> <b>Сессия истекла.</b>",
                    reply_markup=None,
                )
            return
        chunks = data["chunks"]
        total = data["total"]
        header = data.get("header", "")
        raw_text_chunk = chunks[page_num]
        safe_text = self._markdown_to_html(raw_text_chunk)
        formatted_body = self._format_response_with_smart_separation(
            safe_text, expandable=False
        )
        text_to_show = f"{header}\n{formatted_body}"
        nav_row = []
        if page_num > 0:
            nav_row.append({"text": "◀️", "data": f"qwencli:pg:{uid}:{page_num - 1}"})
        nav_row.append({"text": f"{page_num + 1}/{total}", "data": "qwencli:noop"})
        if page_num < total - 1:
            nav_row.append({"text": "▶️", "data": f"qwencli:pg:{uid}:{page_num + 1}"})
        extra_row = [
            {"text": "❌ Закрыть", "callback": self._close_callback, "args": (uid,)}
        ]
        if data.get("chat_id") and data.get("msg_id"):
            extra_row.append(
                {
                    "text": "<tg-emoji emoji-id=5404857686477015710>🔄</tg-emoji>",
                    "callback": self._regenerate_callback,
                    "args": (data["msg_id"], data["chat_id"]),
                }
            )
        buttons = [nav_row, extra_row]
        if isinstance(entity, Message):
            await self.inline.form(
                text=text_to_show, message=entity, reply_markup=buttons
            )
        elif isinstance(entity, InlineCall):
            await self._edit_html(entity, text_to_show, reply_markup=buttons)
        elif hasattr(entity, "edit"):
            with contextlib.suppress(Exception):
                await self._edit_html(entity, text_to_show, reply_markup=buttons)

    def _paginate_text(self, text: str, limit: int) -> list:
        pages = []
        current_page_lines = []
        current_len = 0
        in_code_block = False
        current_code_lang = ""
        for line in text.split("\n"):
            line_len = len(line) + 1
            stripped = line.strip()
            if stripped.startswith("```"):
                if in_code_block:
                    in_code_block = False
                    current_code_lang = ""
                else:
                    in_code_block = True
                    current_code_lang = stripped.replace("```", "").strip()
            if current_len + line_len > limit:
                if current_page_lines:
                    if in_code_block:
                        current_page_lines.append("```")
                    pages.append("\n".join(current_page_lines))
                    current_page_lines = []
                    current_len = 0
                    if in_code_block:
                        header = f"```{current_code_lang}"
                        current_page_lines.append(header)
                        current_len += len(header) + 1
                if line_len > limit:
                    chunks = [line[i : i + limit] for i in range(0, len(line), limit)]
                    for chunk in chunks:
                        if current_len + len(chunk) > limit:
                            pages.append("\n".join(current_page_lines))
                            current_page_lines = [chunk]
                            current_len = len(chunk)
                        else:
                            current_page_lines.append(chunk)
                            current_len += len(chunk)
                    continue
            current_page_lines.append(line)
            current_len += line_len
        if current_page_lines:
            pages.append("\n".join(current_page_lines))
        return pages

    def _is_memory_enabled(self, chat_id: str) -> bool:
        return chat_id not in self.memory_disabled_chats
