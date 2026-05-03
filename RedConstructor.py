# ====================================================================================================================
#   ██████╗  ██████╗ ██╗   ██╗███╗   ███╗ ██████╗ ██████╗ ██╗   ██╗██╗     ███████╗███████╗
#  ██╔════╝ ██╔═══██╗╚██╗ ██╔╝████╗ ████║██╔═══██╗██╔══██╗██║   ██║██║     ██╔════╝██╔════╝
#  ██║  ███╗██║   ██║ ╚████╔╝ ██╔████╔██║██║   ██║██║  ██║██║   ██║██║     █████╗  ███████╗
#  ██║   ██║██║   ██║  ╚██╔╝  ██║╚██╔╝██║██║   ██║██║  ██║██║   ██║██║     ██╔══╝  ╚════██║
#  ╚██████╔╝╚██████╔╝   ██║   ██║ ╚═╝ ██║╚██████╔╝██████╔╝╚██████╔╝███████╗███████╗███████║
#   ╚═════╝  ╚═════╝    ╚═╝   ╚═╝     ╚═╝ ╚═════╝ ╚═════╝  ╚═════╝ ╚══════╝╚══════╝╚══════╝
#
#   OFFICIAL USERNAMES: @goymodules | @samsepi0l_ovf
#   MODULE: redconstructor
#
#   THIS MODULE IS LICENSED UNDER GNU AGPLv3, PROTECTED AGAINST UNAUTHORIZED COPYING/RESALE,
#   AND ITS ORIGINAL AUTHORSHIP BELONGS TO @samsepi0l_ovf.
#   ALL OFFICIAL UPDATES, RELEASE NOTES, AND PATCHES ARE PUBLISHED IN THE TELEGRAM CHANNEL @goymodules.
# ====================================================================================================================
# meta banner: https://raw.githubusercontent.com/sepiol026-wq/GoyModules/refs/heads/main/assets/RedConstructor.png
# meta developer: @GoyModules
# requires: aiohttp ast hashlin 

__version__ = ("1", "0", "0")
 
import ast
import base64
import asyncio
import builtins
import contextlib
import html
import hashlib
import inspect
import io
import json
import re
import time
import tokenize
import typing
from pathlib import Path
from urllib.parse import urlencode, urlparse

import aiohttp

from herokutl.types import Message
from .. import loader, translations, utils


SUPPORTED_LANGS = ["en", "ru", "de", "uk", "jp", "tiktok", "neofit", "leet", "uwu"]
REQUIRED_LANGS = ["en", "ru"]
LANG_CODE_RE = re.compile(r"^[a-z][a-z0-9_]{0,15}$")

VALIDATORS_LIST = [
    "Boolean",
    "Integer",
    "String",
    "Float",
    "Hidden",
    "Link",
    "Choice",
    "MultiChoice",
    "Series",
    "TelegramID",
    "RegExp",
    "Emoji",
]

VALIDATOR_REQUIRED_META = {
    "Choice": "possible_values",
    "MultiChoice": "possible_values",
    "RegExp": "regex",
}

VERSION_RE = re.compile(r"^\d+(?:\.\d+)+$")
IMAGE_META_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".bmp",
    ".avif",
    ".svg",
}
LICENSE_OPTIONS = [
    "MIT",
    "Apache-2.0",
    "GPL-3.0",
    "GPL-2.0",
    "LGPL-3.0",
    "LGPL-2.1",
    "AGPL-3.0",
    "BSD-2-Clause",
    "BSD-3-Clause",
    "MPL-2.0",
    "ISC",
    "Unlicense",
    "CC0-1.0",
    "BSL-1.0",
    "EPL-2.0",
    "Artistic-2.0",
    "CDDL-1.0",
    "CECILL-2.1",
    "OFL-1.1",
    "Zlib",
    "WTFPL",
    "NCSA",
    "AFL-3.0",
    "MS-PL",
    "UPL-1.0",
    "BlueOak-1.0.0",
    "OSL-3.0",
    "PostgreSQL",
    "Python-2.0",
    "OpenSSL",
    "EUPL-1.2",
    "PolyForm-Noncommercial-1.0.0",
    "Proprietary",
]
PREMIUM_TEXT_EMOJIS = {
    "✅": "<tg-emoji emoji-id=5278411813468269386>✅</tg-emoji>",
    "📦": "<tg-emoji emoji-id=5278540791336165644>📦</tg-emoji>",
    "🛠": "<tg-emoji emoji-id=5445384340752053491>🛠</tg-emoji>",
    "⚙️": "<tg-emoji emoji-id=5309974037772928528>⚙️</tg-emoji>",
    "🕓": "<tg-emoji emoji-id=5276412364458059956>🕓</tg-emoji>",
    "🤖": "<tg-emoji emoji-id=5276127848644503161>🤖</tg-emoji>",
    "❌": "<tg-emoji emoji-id=5388785832956016892>❌</tg-emoji>",
    "🌍": "<tg-emoji emoji-id=5415699506782027275>🌍</tg-emoji>",
    "🗑": "<tg-emoji emoji-id=5276384644739129761>🗑</tg-emoji>",
    "🧪": "<tg-emoji emoji-id=5206211858444354221>🧪</tg-emoji>",
    "🐍": "<tg-emoji emoji-id=6312023460214218497>🐍</tg-emoji>",
    "💬": "<tg-emoji emoji-id=5429259122262422749>💬</tg-emoji>",
    "🔁": "<tg-emoji emoji-id=5361993818373655559>🔁</tg-emoji>",
    "🎲": "<tg-emoji emoji-id=5422543773391408326>🎲</tg-emoji>",
    "📥": "<tg-emoji emoji-id=5276220667182736079>📥</tg-emoji>",
    "📁": "<tg-emoji emoji-id=5278227821364275264>📁</tg-emoji>",
    "🔍": "<tg-emoji emoji-id=5276395476646653290>🔍</tg-emoji>",
    "👤": "<tg-emoji emoji-id=5275979556308674886>👤</tg-emoji>",
    "👥": "<tg-emoji emoji-id=5298668674532538341>👥</tg-emoji>",
    "🧑‍💻": "<tg-emoji emoji-id=6312191990435944463>🧑‍💻</tg-emoji>",
    "📚": "<tg-emoji emoji-id=5206626000665868017>📚</tg-emoji>",
    "🏠": "<tg-emoji emoji-id=5278413853577734640>🏠</tg-emoji>",
}
NO_PREMIUM_TEXT_KEYS = {
    "project_btn_line",
    "command_btn_line",
    "watcher_btn_line",
    "loop_btn_line",
    "config_btn_line",
    "lang_btn_required",
    "lang_btn_ready",
    "lang_btn_empty",
}

AI_PROVIDER_OPTIONS = [
    "anthropic",
    "openai",
    "openrouter",
    "deepseek",
    "groq",
    "together",
    "fireworks",
    "mistral",
    "xai",
    "google",
]

AI_PROVIDER_MODELS = {
    "anthropic": "claude-sonnet-4-20250514",
    "openai": "gpt-5-mini",
    "openrouter": "openai/gpt-4.1-mini",
    "deepseek": "deepseek-chat",
    "groq": "llama-3.3-70b-versatile",
    "together": "meta-llama/Llama-3.3-70B-Instruct-Turbo",
    "fireworks": "accounts/fireworks/models/llama-v3p3-70b-instruct",
    "mistral": "mistral-large-latest",
    "xai": "grok-3-mini",
    "google": "gemini-2.5-flash",
}

AI_PROVIDER_BASE_URLS = {
    "anthropic": "https://api.anthropic.com/v1/messages",
    "openai": "https://api.openai.com/v1/chat/completions",
    "openrouter": "https://openrouter.ai/api/v1/chat/completions",
    "deepseek": "https://api.deepseek.com/chat/completions",
    "groq": "https://api.groq.com/openai/v1/chat/completions",
    "together": "https://api.together.xyz/v1/chat/completions",
    "fireworks": "https://api.fireworks.ai/inference/v1/chat/completions",
    "mistral": "https://api.mistral.ai/v1/chat/completions",
    "xai": "https://api.x.ai/v1/chat/completions",
    "google": "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
}

LANG_LABELS = {
    "en": "English",
    "ru": "Русский",
    "de": "Deutsch",
    "uk": "Українська",
    "jp": "日本語",
    "tiktok": "TikTok",
    "neofit": "Neofit",
    "leet": "1337",
    "uwu": "Uwu",
}

WATCHER_TAG_OPTIONS = [
    "no_commands",
    "only_commands",
    "out",
    "in",
    "only_messages",
    "editable",
    "no_media",
    "only_media",
    "only_photos",
    "only_videos",
    "only_audios",
    "only_docs",
    "only_stickers",
    "only_inline",
    "only_channels",
    "only_groups",
    "only_pm",
    "no_pm",
    "no_channels",
    "no_groups",
    "no_inline",
    "no_stickers",
    "no_docs",
    "no_audios",
    "no_videos",
    "no_photos",
    "no_forwards",
    "no_reply",
    "no_mention",
    "mention",
    "only_reply",
    "only_forwards",
]

RESERVED_METHOD_NAMES = {
    "_ma_lang",
    "_ma_pack",
    "_ma_cmd_raw",
    "_ma_cmd_text",
    "_ma_cmd_list",
    "_ma_cmd_value",
    "__init__",
    "client_ready",
    "on_unload",
}

AI_SYSTEM_PROMPT = (
    "Ты пишешь код только для Heroku Userbot. "
    "Никогда не путай Heroku и Hikka. Heroku это Heroku, Hikka это Hikka. "
    "Нужен только Heroku Userbot API, его паттерны и совместимый стиль модулей. "
    "Верни только тело async-функции без def-строки, без class, без decorators, без docstring, без imports, без markdown, без пояснений, без code fences. "
    "Сигнатура уже существует в пользовательском запросе и ей нужно строго соответствовать. "
    "Код должен быть валидным Python, с корректными отступами, без комментариев любого вида и без мусора. "
    "Если есть объект message и нужен ответ, используй await utils.answer(message, text). "
    "Если у сущности нет message, не выдумывай его. "
    "Любое форматирование текста всегда Telegram HTML, не Markdown. "
    "Если нужен форматированный текст, используй только HTML-теги Telegram вроде <b>, <i>, <u>, <s>, <code>, <pre>, <blockquote>, <a href='...'>. "
    "Никогда не используй markdown-разметку, markdown code fences, parse_mode='Markdown' или markdownv2. "
    "Не используй print, input, requests, subprocess, eval, exec, raw telethon imports, сторонние библиотеки, недоступные helper-функции и несуществующие атрибуты. "
    "Не придумывай API. Не оставляй TODO, NOTE, комментарии или заглушки. "
    "Не возвращай def, class, decorator, import, docstring, объяснение, комментарий, markdown или текст вне кода. "
    "Не ломай совместимость Heroku Userbot и не используй Hikka-специфичные конструкции. "
    "Если поведение можно реализовать надёжно, реализуй полностью, а не частично. "
    "Результат должен быть готов к вставке в Heroku Userbot без ручных правок."
)

COMMAND_TEMPLATES = {
    "ping": {
        "title": "⚡ Ping",
        "prompt_key": None,
        "needs_input": False,
    },
    "fixed_text": {
        "title": "💬 Fixed text",
        "prompt_key": "ask_template_fixed_text",
        "needs_input": True,
    },
    "echo_args": {
        "title": "🗣 Echo args",
        "prompt_key": None,
        "needs_input": False,
    },
    "echo_reply": {
        "title": "↩️ Echo reply",
        "prompt_key": None,
        "needs_input": False,
    },
    "upper_reply": {
        "title": "🔠 Upper reply",
        "prompt_key": None,
        "needs_input": False,
    },
    "lower_reply": {
        "title": "🔡 Lower reply",
        "prompt_key": None,
        "needs_input": False,
    },
    "reverse_text": {
        "title": "🔁 Reverse text",
        "prompt_key": None,
        "needs_input": False,
    },
    "random_choice": {
        "title": "🎲 Random choice",
        "prompt_key": "ask_template_random_values",
        "needs_input": True,
    },
    "me_info": {
        "title": "🪪 My info",
        "prompt_key": None,
        "needs_input": False,
    },
    "config_dump": {
        "title": "⚙️ Config dump",
        "prompt_key": None,
        "needs_input": False,
    },
    "db_save": {
        "title": "💾 DB save",
        "prompt_key": "ask_template_db_save",
        "needs_input": True,
    },
    "db_load": {
        "title": "📥 DB load",
        "prompt_key": "ask_template_db_load",
        "needs_input": True,
    },
}


def _escape_html(value: typing.Any) -> str:
    return html.escape(str(value or ""), quote=False)


def _normalize_lang_code(value: typing.Any) -> typing.Optional[str]:
    lang = str(value or "").strip().lower().replace("-", "_")
    if lang == "ua":
        lang = "uk"
    if not LANG_CODE_RE.match(lang):
        return None
    return lang


def _flatten_lang_dict(value: typing.Any) -> dict:
    if not isinstance(value, dict):
        return {}
    return {str(key): item for key, item in value.items()}


def _parse_dependencies(value: typing.Any) -> list:
    deps = []
    seen = set()
    raw = value
    if isinstance(raw, str):
        raw = re.split(r"[\n,]+", raw)
    if not isinstance(raw, (list, tuple, set)):
        return deps
    for item in raw:
        dep = str(item or "").strip()
        if not dep or dep in seen:
            continue
        seen.add(dep)
        deps.append(dep)
    return deps


def _normalize_version(value: typing.Any) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    if not VERSION_RE.fullmatch(normalized):
        return ""
    return normalized


def _version_tuple_expr(value: typing.Any) -> str:
    normalized = _normalize_version(value)
    if not normalized:
        return ""
    parts = normalized.split(".")
    return "({})".format(", ".join(repr(part) for part in parts))


def _b64decode_text(value: str) -> str:
    return base64.b64decode(value).decode("utf-8")


def _normalize_meta_text(value: typing.Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _is_valid_image_meta(value: typing.Any) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return True
    parsed = urlparse(raw)
    path = parsed.path if parsed.scheme else raw
    suffix = Path(path.split("?", 1)[0].split("#", 1)[0]).suffix.lower()
    return suffix in IMAGE_META_EXTENSIONS


def _apply_premium_text_emojis(value: typing.Any) -> str:
    text = str(value or "")
    for emoji, premium in PREMIUM_TEXT_EMOJIS.items():
        text = text.replace(emoji, premium)
    return text


def _project_languages(project: typing.Optional[dict]) -> list:
    seen = set()
    ordered = []

    def _push(lang: typing.Any):
        normalized = _normalize_lang_code(lang)
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        ordered.append(normalized)

    for lang in REQUIRED_LANGS:
        _push(lang)
    for lang in SUPPORTED_LANGS:
        _push(lang)

    if not isinstance(project, dict):
        return ordered

    for lang in project.get("extra_langs") or []:
        _push(lang)

    for lang in (project.get("strings") or {}).keys():
        _push(lang)

    command_docs = project.get("command_docs") or {}
    if isinstance(command_docs, dict):
        for per_command in command_docs.values():
            if not isinstance(per_command, dict):
                continue
            for lang in per_command.keys():
                _push(lang)

    command_resources = project.get("command_resources") or {}
    if isinstance(command_resources, dict):
        for per_command in command_resources.values():
            if not isinstance(per_command, dict):
                continue
            for lang in per_command.keys():
                _push(lang)

    return ordered


def _command_body_source(cmd: dict) -> str:
    if not isinstance(cmd, dict):
        return ""
    return str(cmd.get("body_template") or cmd.get("body") or "")


def _command_resource_bucket(
    project: dict,
    cmd_name: str,
    lang: str,
    *,
    create: bool = False,
) -> typing.Optional[dict]:
    if not isinstance(project, dict):
        return None

    resources = project.setdefault("command_resources", {}) if create else project.get("command_resources")
    if not isinstance(resources, dict):
        if not create:
            return None
        resources = {}
        project["command_resources"] = resources

    per_command = resources.setdefault(cmd_name, {}) if create else resources.get(cmd_name)
    if not isinstance(per_command, dict):
        if not create:
            return None
        per_command = {}
        resources[cmd_name] = per_command

    per_lang = per_command.setdefault(lang, {}) if create else per_command.get(lang)
    if not isinstance(per_lang, dict):
        if not create:
            return None
        per_lang = {}
        per_command[lang] = per_lang

    for key in ("texts", "lists", "values"):
        current = per_lang.setdefault(key, {}) if create else per_lang.get(key)
        if not isinstance(current, dict):
            if not create:
                return None
            per_lang[key] = {}

    return per_lang


def _command_resource_get(
    project: dict,
    cmd_name: str,
    lang: str,
    kind: str,
    key: str,
    default: typing.Any = None,
):
    lang_bucket = _command_resource_bucket(project, cmd_name, lang, create=False) or {}
    base_bucket = _command_resource_bucket(project, cmd_name, "en", create=False) or {}
    lang_values = _flatten_lang_dict(lang_bucket.get(kind))
    base_values = _flatten_lang_dict(base_bucket.get(kind))
    if key in lang_values:
        return lang_values[key]
    if key in base_values:
        return base_values[key]
    return default


def _serialize_resource_value(kind: str, value: typing.Any) -> str:
    if kind == "texts":
        return str(value or "")
    return json.dumps(value, ensure_ascii=False)


def _command_resource_string_key(cmd_name: str, kind: str, key: str) -> str:
    mapping = {"texts": "text", "lists": "list", "values": "value"}
    return "__cmd__{}__{}__{}".format(cmd_name, mapping[kind], key)

IDE_I18N_PATCHES = {
    "en": {
        "placeholder_module_name": "Weather",
        "placeholder_desc_en": "English description",
        "placeholder_desc_ru": "Russian description",
        "placeholder_cmd_name": "ping",
        "placeholder_cmd_desc_en": "English command description",
        "placeholder_cmd_desc_ru": "Russian command description",
        "placeholder_cfg_key": "api_key",
        "placeholder_cfg_default": "default value",
        "placeholder_lang_desc": "Module description",
        "project_list_line": "📦 <b>{name}</b> | cmd: {cmds} | cfg: {cfgs}\n",
        "project_list_line_with_id": "📦 <b>{name}</b> | ID: <code>{pid}</code> | cmd: {cmds} | cfg: {cfgs}\n",
    },
    "ru": {
        "name": "RedConstructor",
        "placeholder_module_name": "Weather",
        "placeholder_desc_en": "English description",
        "placeholder_desc_ru": "Описание на русском",
        "placeholder_cmd_name": "ping",
        "placeholder_cmd_desc_en": "English command description",
        "placeholder_cmd_desc_ru": "Описание команды",
        "placeholder_cfg_key": "api_key",
        "placeholder_cfg_default": "значение по умолчанию",
        "placeholder_lang_desc": "Описание модуля",
        "project_list_line": "📦 <b>{name}</b> | команд: {cmds} | конфигов: {cfgs}\n",
        "project_list_line_with_id": "📦 <b>{name}</b> | ID: <code>{pid}</code> | команд: {cmds} | конфигов: {cfgs}\n",
    },
    "de": {
        "name": "RedConstructor",
        "project_panel": (
            "🗂 <b>{name}</b>\n\n"
            "🛠 Befehle: <code>{cmds}</code>\n"
            "⚙️ Konfigs: <code>{cfgs}</code>\n"
            "🌍 Sprachen: <code>{langs}/{total_langs}</code>\n\n"
            "Wähle eine Aktion:"
        ),
        "ask_name": "✏️ Gib den <b>Modulnamen</b> ein (z. B. <code>Weather</code>; <code>Mod</code> wird automatisch zur Klasse hinzugefügt):",
        "ask_desc_en": "✏️ Gib die <b>Modulbeschreibung auf Englisch</b> ein:",
        "ask_desc_ru": "✏️ Gib die <b>Modulbeschreibung auf Russisch</b> ein:",
        "ask_cmd_name": "✏️ Gib den <b>Befehlsnamen</b> ein (kleine lateinische Buchstaben, ohne Punkt, z. B. <code>ping</code>):",
        "ask_cmd_doc_en": "✏️ Gib die <b>Befehlsbeschreibung auf Englisch</b> ein:",
        "ask_cmd_doc_ru": "✏️ Gib die <b>Befehlsbeschreibung auf Russisch</b> ein:",
        "ask_cmd_ai_prompt": (
            "🤖 Beschreibe, was der Befehl <code>{cmd}</code> tun soll.\n"
            "Beispiel: <i>Nimm den Text aus der Antwort, wandle ihn in Großbuchstaben um und antworte damit</i>"
        ),
        "ask_cmd_body": (
            "✍️ Gib den <b>Python-Body</b> für den Befehl <code>{cmd}</code> ein.\n"
            "Nur der Body ist nötig, ohne <code>def</code> und ohne Importe."
        ),
        "ask_cfg_key": "✏️ Gib den <b>Konfig-Schlüssel</b> ein (z. B. <code>api_key</code>):",
        "ask_cfg_default": "✏️ Gib den <b>Standardwert</b> ein oder lass das Feld leer:",
        "ask_cfg_validator": "⚙️ Wähle den <b>Validator</b> für die Konfiguration <code>{key}</code>:",
        "ask_lang_cls": "✏️ Gib die <b>Modulbeschreibung</b> für die Sprache <b>{lang}</b> ein:",
        "lang_panel": "🌍 <b>Sprachmenü</b>\n\nWähle die Sprache zum Bearbeiten:",
        "body_mode": "🧩 <b>Befehl <code>.{cmd}</code></b>\n\nWähle, wie der Body hinzugefügt werden soll:",
        "ai_generating": "🤖 <i>KI generiert Code für <code>{cmd}</code>...</i>",
        "ai_done": "✅ KI hat den Body für <code>{cmd}</code> erstellt.",
        "ai_error": "❌ KI-Fehler: <code>{err}</code>",
        "compile_start": "🔨 Modul <code>{name}</code> wird kompiliert...",
        "compile_error": "❌ <b>Kompilierfehler:</b>\n<code>{err}</code>",
        "compiled_caption": (
            "✅ <b>RedConstructor</b>\n\n"
            "📦 Modul: <code>{name}</code>\n"
            "🛠 Befehle: <code>{cmds}</code>\n"
            "⚙️ Konfigs: <code>{cfgs}</code>\n\n"
            "Installiere es mit <code>.lm</code> als Antwort auf diese Datei."
        ),
        "lang_saved": "✅ Übersetzung für <b>{lang}</b> gespeichert.",
        "invalid_module_name": "❌ Ungültiger Modulname. Verwende lateinische Buchstaben, Ziffern, Leerzeichen oder Unterstriche. <code>Mod</code> wird automatisch angehängt.",
        "invalid_command_name": "❌ Ungültiger Befehlsname. Verwende kleine lateinische Buchstaben, Ziffern und Unterstriche.",
        "invalid_config_name": "❌ Ungültiger Konfig-Schlüssel. Verwende kleine lateinische Buchstaben, Ziffern und Unterstriche.",
        "invalid_code": "❌ Ungültiger Python-Body:\n<code>{err}</code>",
        "duplicate_project_name": "❌ Ein Projekt mit dem Modulnamen <code>{name}</code> existiert bereits.",
        "duplicate_command_name": "❌ Der Befehl <code>.{cmd}</code> existiert bereits in diesem Projekt.",
        "duplicate_config_name": "❌ Die Konfiguration <code>{key}</code> existiert bereits in diesem Projekt.",
        "project_not_found": "❌ Projekt nicht gefunden.",
        "list_title": "📋 <b>Projekte</b>",
        "delete_confirm": "🗑 Projekt <b>{name}</b> löschen? Das kann nicht rückgängig gemacht werden.",
        "cancel": "❌ Abgebrochen.",
        "saved_empty": "✅ Leerer Wert gespeichert.",
        "placeholder_module_name": "Weather",
        "placeholder_desc_en": "English description",
        "placeholder_desc_ru": "Russische Beschreibung",
        "placeholder_cmd_name": "ping",
        "placeholder_cmd_desc_en": "English command description",
        "placeholder_cmd_desc_ru": "Befehlsbeschreibung",
        "placeholder_cfg_key": "api_key",
        "placeholder_cfg_default": "Standardwert",
        "placeholder_lang_desc": "Modulbeschreibung",
        "project_list_line": "📦 <b>{name}</b> | Befehle: {cmds} | Konfigs: {cfgs}\n",
        "project_list_line_with_id": "📦 <b>{name}</b> | ID: <code>{pid}</code> | Befehle: {cmds} | Konfigs: {cfgs}\n",
    },
    "uk": {
        "name": "RedConstructor",
        "project_panel": (
            "🗂 <b>{name}</b>\n\n"
            "🛠 Команд: <code>{cmds}</code>\n"
            "⚙️ Конфігів: <code>{cfgs}</code>\n"
            "🌍 Мов: <code>{langs}/{total_langs}</code>\n\n"
            "Обери дію:"
        ),
        "ask_name": "✏️ Введи <b>назву модуля</b> (наприклад <code>Weather</code>; суфікс <code>Mod</code> для класу додасться автоматично):",
        "ask_desc_en": "✏️ Введи <b>опис модуля англійською</b>:",
        "ask_desc_ru": "✏️ Введи <b>опис модуля російською</b>:",
        "ask_cmd_name": "✏️ Введи <b>назву команди</b> (малими латинськими літерами, без крапки, наприклад <code>ping</code>):",
        "ask_cmd_doc_en": "✏️ Введи <b>опис команди англійською</b>:",
        "ask_cmd_doc_ru": "✏️ Введи <b>опис команди російською</b>:",
        "ask_cmd_ai_prompt": (
            "🤖 Опиши, що має робити команда <code>{cmd}</code>.\n"
            "Приклад: <i>Бере текст із reply, переводить у верхній регістр і відповідає ним</i>"
        ),
        "ask_cmd_body": (
            "✍️ Введи <b>Python-тіло</b> для команди <code>{cmd}</code>.\n"
            "Потрібне лише тіло, без <code>def</code> та імпортів."
        ),
        "ask_cfg_key": "✏️ Введи <b>ключ конфіга</b> (наприклад <code>api_key</code>):",
        "ask_cfg_default": "✏️ Введи <b>значення за замовчуванням</b> або залиш порожнім:",
        "ask_cfg_validator": "⚙️ Обери <b>валідатор</b> для конфіга <code>{key}</code>:",
        "ask_lang_cls": "✏️ Введи <b>опис модуля</b> для мови <b>{lang}</b>:",
        "lang_panel": "🌍 <b>Панель мов</b>\n\nОбери мову для редагування:",
        "body_mode": "🧩 <b>Команда <code>.{cmd}</code></b>\n\nОбери спосіб додати тіло:",
        "ai_generating": "🤖 <i>AI генерує код для <code>{cmd}</code>...</i>",
        "ai_done": "✅ AI згенерував тіло для <code>{cmd}</code>.",
        "ai_error": "❌ Помилка AI: <code>{err}</code>",
        "compile_start": "🔨 Збираю модуль <code>{name}</code>...",
        "compile_error": "❌ <b>Помилка збірки:</b>\n<code>{err}</code>",
        "compiled_caption": (
            "✅ <b>RedConstructor</b>\n\n"
            "📦 Модуль: <code>{name}</code>\n"
            "🛠 Команд: <code>{cmds}</code>\n"
            "⚙️ Конфігів: <code>{cfgs}</code>\n\n"
            "Встанови через <code>.lm</code> reply на цей файл."
        ),
        "lang_saved": "✅ Переклад для <b>{lang}</b> збережено.",
        "invalid_module_name": "❌ Неправильна назва модуля. Використовуй латиницю, цифри, пробіли або підкреслення. Суфікс <code>Mod</code> додається автоматично.",
        "invalid_command_name": "❌ Неправильна назва команди. Використовуй малі латинські букви, цифри та підкреслення.",
        "invalid_config_name": "❌ Неправильний ключ конфіга. Використовуй малі латинські букви, цифри та підкреслення.",
        "invalid_code": "❌ Невалідне Python-тіло:\n<code>{err}</code>",
        "duplicate_project_name": "❌ Проєкт із модулем <code>{name}</code> уже існує.",
        "duplicate_command_name": "❌ Команда <code>.{cmd}</code> уже є в цьому проєкті.",
        "duplicate_config_name": "❌ Конфіг <code>{key}</code> уже є в цьому проєкті.",
        "project_not_found": "❌ Проєкт не знайдено.",
        "list_title": "📋 <b>Проєкти</b>",
        "delete_confirm": "🗑 Видалити проєкт <b>{name}</b>? Цю дію не можна скасувати.",
        "cancel": "❌ Скасовано.",
        "saved_empty": "✅ Порожнє значення збережено.",
        "placeholder_module_name": "Weather",
        "placeholder_desc_en": "English description",
        "placeholder_desc_ru": "Опис російською",
        "placeholder_cmd_name": "ping",
        "placeholder_cmd_desc_en": "English command description",
        "placeholder_cmd_desc_ru": "Опис команди",
        "placeholder_cfg_key": "api_key",
        "placeholder_cfg_default": "значення за замовчуванням",
        "placeholder_lang_desc": "Опис модуля",
        "project_list_line": "📦 <b>{name}</b> | команд: {cmds} | конфігів: {cfgs}\n",
        "project_list_line_with_id": "📦 <b>{name}</b> | ID: <code>{pid}</code> | команд: {cmds} | конфігів: {cfgs}\n",
    },
    "jp": {
        "name": "RedConstructor",
        "project_panel": (
            "🗂 <b>{name}</b>\n\n"
            "🛠 コマンド: <code>{cmds}</code>\n"
            "⚙️ 設定: <code>{cfgs}</code>\n"
            "🌍 言語: <code>{langs}/{total_langs}</code>\n\n"
            "操作を選択:"
        ),
        "ask_name": "✏️ <b>モジュール名</b>を入力してください（例: <code>Weather</code>。クラス名には <code>Mod</code> が自動で付きます）:",
        "ask_desc_en": "✏️ <b>英語のモジュール説明</b>を入力してください:",
        "ask_desc_ru": "✏️ <b>ロシア語のモジュール説明</b>を入力してください:",
        "ask_cmd_name": "✏️ <b>コマンド名</b>を入力してください（小文字ラテン文字、ドットなし、例: <code>ping</code>）:",
        "ask_cmd_doc_en": "✏️ <b>英語のコマンド説明</b>を入力してください:",
        "ask_cmd_doc_ru": "✏️ <b>ロシア語のコマンド説明</b>を入力してください:",
        "ask_cmd_ai_prompt": (
            "🤖 コマンド <code>{cmd}</code> が何をするべきか説明してください。\n"
            "例: <i>返信先のテキストを大文字に変換して返答する</i>"
        ),
        "ask_cmd_body": (
            "✍️ コマンド <code>{cmd}</code> の<b>Python本文</b>を入力してください。\n"
            "<code>def</code> や import なしで本文だけを書いてください。"
        ),
        "ask_cfg_key": "✏️ <b>設定キー</b>を入力してください（例: <code>api_key</code>）:",
        "ask_cfg_default": "✏️ <b>デフォルト値</b>を入力するか、空のままにしてください:",
        "ask_cfg_validator": "⚙️ 設定 <code>{key}</code> の<b>バリデータ</b>を選択してください:",
        "ask_lang_cls": "✏️ 言語 <b>{lang}</b> 用の<b>モジュール説明</b>を入力してください:",
        "lang_panel": "🌍 <b>言語パネル</b>\n\n編集する言語を選択してください:",
        "body_mode": "🧩 <b>コマンド <code>.{cmd}</code></b>\n\n本文の追加方法を選択してください:",
        "ai_generating": "🤖 <i><code>{cmd}</code> のコードを AI が生成しています...</i>",
        "ai_done": "✅ <code>{cmd}</code> の本文を AI が生成しました。",
        "ai_error": "❌ AI エラー: <code>{err}</code>",
        "compile_start": "🔨 モジュール <code>{name}</code> をコンパイルしています...",
        "compile_error": "❌ <b>コンパイルエラー:</b>\n<code>{err}</code>",
        "compiled_caption": (
            "✅ <b>RedConstructor</b>\n\n"
            "📦 モジュール: <code>{name}</code>\n"
            "🛠 コマンド: <code>{cmds}</code>\n"
            "⚙️ 設定: <code>{cfgs}</code>\n\n"
            "このファイルに返信して <code>.lm</code> でインストールしてください。"
        ),
        "lang_saved": "✅ <b>{lang}</b> の翻訳を保存しました。",
        "invalid_module_name": "❌ 無効なモジュール名です。ラテン文字、数字、スペース、アンダースコアを使ってください。<code>Mod</code> は自動で追加されます。",
        "invalid_command_name": "❌ 無効なコマンド名です。小文字ラテン文字、数字、アンダースコアを使ってください。",
        "invalid_config_name": "❌ 無効な設定キーです。小文字ラテン文字、数字、アンダースコアを使ってください。",
        "invalid_code": "❌ 無効な Python 本文です:\n<code>{err}</code>",
        "duplicate_project_name": "❌ モジュール名 <code>{name}</code> のプロジェクトはすでに存在します。",
        "duplicate_command_name": "❌ コマンド <code>.{cmd}</code> はこのプロジェクトにすでに存在します。",
        "duplicate_config_name": "❌ 設定 <code>{key}</code> はこのプロジェクトにすでに存在します。",
        "project_not_found": "❌ プロジェクトが見つかりません。",
        "list_title": "📋 <b>プロジェクト</b>",
        "delete_confirm": "🗑 プロジェクト <b>{name}</b> を削除しますか？ この操作は元に戻せません。",
        "cancel": "❌ キャンセルしました。",
        "saved_empty": "✅ 空の値を保存しました。",
        "placeholder_module_name": "Weather",
        "placeholder_desc_en": "English description",
        "placeholder_desc_ru": "ロシア語の説明",
        "placeholder_cmd_name": "ping",
        "placeholder_cmd_desc_en": "English command description",
        "placeholder_cmd_desc_ru": "コマンド説明",
        "placeholder_cfg_key": "api_key",
        "placeholder_cfg_default": "デフォルト値",
        "placeholder_lang_desc": "モジュール説明",
        "project_list_line": "📦 <b>{name}</b> | コマンド: {cmds} | 設定: {cfgs}\n",
        "project_list_line_with_id": "📦 <b>{name}</b> | ID: <code>{pid}</code> | コマンド: {cmds} | 設定: {cfgs}\n",
    },
    "tiktok": {
        "name": "RedConstructor",
        "project_panel": (
            "🗂 <b>{name}</b>\n\n"
            "🛠 Команд: <code>{cmds}</code>\n"
            "⚙️ Конфигов: <code>{cfgs}</code>\n"
            "🌍 Языков: <code>{langs}/{total_langs}</code>\n\n"
            "Че ковыряем:"
        ),
        "ask_name": "✏️ Вбей <b>название модуля</b> (типа <code>Weather</code>; класс сам докинет <code>Mod</code>):",
        "ask_desc_en": "✏️ Вкинь <b>описание модуля на английском</b>:",
        "ask_desc_ru": "✏️ Вкинь <b>описание модуля на русском</b>:",
        "ask_cmd_name": "✏️ Вбей <b>имя команды</b> (lowercase latin, без точки, типа <code>ping</code>):",
        "ask_cmd_doc_en": "✏️ Вкинь <b>описание команды на английском</b>:",
        "ask_cmd_doc_ru": "✏️ Вкинь <b>описание команды на русском</b>:",
        "ask_cmd_ai_prompt": (
            "🤖 Распиши, че должна мутить команда <code>{cmd}</code>.\n"
            "Пример: <i>берет текст из реплая, апает в uppercase и отвечает им</i>"
        ),
        "ask_cmd_body": (
            "✍️ Вставь <b>Python body</b> для <code>{cmd}</code>.\n"
            "Только тело, без <code>def</code> и без импортов."
        ),
        "ask_cfg_key": "✏️ Вбей <b>ключ конфига</b> (типа <code>api_key</code>):",
        "ask_cfg_default": "✏️ Вбей <b>дефолтное значение</b> или оставь пустым:",
        "ask_cfg_validator": "⚙️ Выбери <b>валидатор</b> для <code>{key}</code>:",
        "ask_lang_cls": "✏️ Вкинь <b>описание модуля</b> для языка <b>{lang}</b>:",
        "lang_panel": "🌍 <b>Панель языков</b>\n\nЧе редачим:",
        "body_mode": "🧩 <b>Команда <code>.{cmd}</code></b>\n\nКак тело мутим?",
        "ai_generating": "🤖 <i>AI сейчас накидает код для <code>{cmd}</code>...</i>",
        "ai_done": "✅ AI накидал тело для <code>{cmd}</code>.",
        "ai_error": "❌ AI отлетел: <code>{err}</code>",
        "compile_start": "🔨 Пакуем модуль <code>{name}</code>...",
        "compile_error": "❌ <b>Сборка отвалилась:</b>\n<code>{err}</code>",
        "compiled_caption": (
            "✅ <b>RedConstructor</b>\n\n"
            "📦 Модуль: <code>{name}</code>\n"
            "🛠 Команд: <code>{cmds}</code>\n"
            "⚙️ Конфигов: <code>{cfgs}</code>\n\n"
            "Ставь через <code>.lm</code> реплаем на этот файл."
        ),
        "lang_saved": "✅ Текст для <b>{lang}</b> схоронился.",
        "invalid_module_name": "❌ Кривое имя модуля. Юзай латиницу, цифры, пробелы или underscore. <code>Mod</code> докинется сам.",
        "invalid_command_name": "❌ Кривое имя команды. Нужны lowercase latin, цифры и underscore.",
        "invalid_config_name": "❌ Кривой ключ конфига. Нужны lowercase latin, цифры и underscore.",
        "invalid_code": "❌ Кривой Python body:\n<code>{err}</code>",
        "duplicate_project_name": "❌ Модуль <code>{name}</code> уже есть, второй такой не прокатит.",
        "duplicate_command_name": "❌ Команда <code>.{cmd}</code> уже есть в этом проекте.",
        "duplicate_config_name": "❌ Конфиг <code>{key}</code> уже есть в этом проекте.",
        "project_not_found": "❌ Проект куда-то скис.",
        "list_title": "📋 <b>Проекты</b>",
        "delete_confirm": "🗑 Снести проект <b>{name}</b>? Назад не откатишь.",
        "cancel": "❌ Отмена.",
        "saved_empty": "✅ Пустое значение сохранил.",
        "placeholder_module_name": "Weather",
        "placeholder_desc_en": "English description",
        "placeholder_desc_ru": "Описание на русском",
        "placeholder_cmd_name": "ping",
        "placeholder_cmd_desc_en": "English command description",
        "placeholder_cmd_desc_ru": "Описание команды",
        "placeholder_cfg_key": "api_key",
        "placeholder_cfg_default": "дефолт",
        "placeholder_lang_desc": "Описание модуля",
        "project_list_line": "📦 <b>{name}</b> | команд: {cmds} | конфигов: {cfgs}\n",
        "project_list_line_with_id": "📦 <b>{name}</b> | ID: <code>{pid}</code> | команд: {cmds} | конфигов: {cfgs}\n",
    },
    "neofit": {
        "name": "RedConstructor",
        "project_panel": (
            "🗂 <b>{name}</b>\n\n"
            "🛠 Команд: <code>{cmds}</code>\n"
            "⚙️ Конфигов: <code>{cfgs}</code>\n"
            "🌍 Языков: <code>{langs}/{total_langs}</code>\n\n"
            "Что делаем:"
        ),
        "ask_name": "✏️ Введи <b>название модуля</b> (например <code>Weather</code>; суффикс <code>Mod</code> для класса добавится автоматически):",
        "ask_desc_en": "✏️ Введи <b>описание модуля на английском</b>:",
        "ask_desc_ru": "✏️ Введи <b>описание модуля на русском</b>:",
        "ask_cmd_name": "✏️ Введи <b>название команды</b> (маленькими латинскими, без точки, например <code>ping</code>):",
        "ask_cmd_doc_en": "✏️ Введи <b>описание команды на английском</b>:",
        "ask_cmd_doc_ru": "✏️ Введи <b>описание команды на русском</b>:",
        "ask_cmd_ai_prompt": (
            "🤖 Опиши, что должна делать команда <code>{cmd}</code>.\n"
            "Пример: <i>Берёт текст из reply, делает его капсом и отвечает</i>"
        ),
        "ask_cmd_body": (
            "✍️ Введи <b>Python-тело</b> для команды <code>{cmd}</code>.\n"
            "Нужно только тело, без <code>def</code> и импортов."
        ),
        "ask_cfg_key": "✏️ Введи <b>ключ конфига</b> (например <code>api_key</code>):",
        "ask_cfg_default": "✏️ Введи <b>значение по умолчанию</b> или оставь пусто:",
        "ask_cfg_validator": "⚙️ Выбери <b>валидатор</b> для конфига <code>{key}</code>:",
        "ask_lang_cls": "✏️ Введи <b>описание модуля</b> для языка <b>{lang}</b>:",
        "lang_panel": "🌍 <b>Панель языков</b>\n\nВыбери язык для редактирования:",
        "body_mode": "🧩 <b>Команда <code>.{cmd}</code></b>\n\nКак добавить тело:",
        "ai_generating": "🤖 <i>AI пишет код для <code>{cmd}</code>...</i>",
        "ai_done": "✅ AI сгенерировал тело для <code>{cmd}</code>.",
        "ai_error": "❌ Ошибка AI: <code>{err}</code>",
        "compile_start": "🔨 Собираю модуль <code>{name}</code>...",
        "compile_error": "❌ <b>Ошибка сборки:</b>\n<code>{err}</code>",
        "compiled_caption": (
            "✅ <b>RedConstructor</b>\n\n"
            "📦 Модуль: <code>{name}</code>\n"
            "🛠 Команд: <code>{cmds}</code>\n"
            "⚙️ Конфигов: <code>{cfgs}</code>\n\n"
            "Ставь через <code>.lm</code> реплаем на этот файл."
        ),
        "lang_saved": "✅ Перевод для <b>{lang}</b> сохранён.",
        "invalid_module_name": "❌ Неверное имя модуля. Используй латиницу, цифры, пробелы или подчёркивания. Суффикс <code>Mod</code> добавляется автоматически.",
        "invalid_command_name": "❌ Неверное имя команды. Нужны строчные латинские буквы, цифры и подчёркивание.",
        "invalid_config_name": "❌ Неверный ключ конфига. Нужны строчные латинские буквы, цифры и подчёркивание.",
        "invalid_code": "❌ Невалидное Python-тело:\n<code>{err}</code>",
        "duplicate_project_name": "❌ Проект с модулем <code>{name}</code> уже есть.",
        "duplicate_command_name": "❌ Команда <code>.{cmd}</code> уже есть в этом проекте.",
        "duplicate_config_name": "❌ Конфиг <code>{key}</code> уже есть в этом проекте.",
        "project_not_found": "❌ Проект не найден.",
        "list_title": "📋 <b>Проекты</b>",
        "delete_confirm": "🗑 Удалить проект <b>{name}</b>? Назад уже не вернуть.",
        "cancel": "❌ Отменено.",
        "saved_empty": "✅ Пустое значение сохранено.",
        "placeholder_module_name": "Weather",
        "placeholder_desc_en": "English description",
        "placeholder_desc_ru": "Описание на русском",
        "placeholder_cmd_name": "ping",
        "placeholder_cmd_desc_en": "English command description",
        "placeholder_cmd_desc_ru": "Описание команды",
        "placeholder_cfg_key": "api_key",
        "placeholder_cfg_default": "значение по умолчанию",
        "placeholder_lang_desc": "Описание модуля",
        "project_list_line": "📦 <b>{name}</b> | команд: {cmds} | конфигов: {cfgs}\n",
        "project_list_line_with_id": "📦 <b>{name}</b> | ID: <code>{pid}</code> | команд: {cmds} | конфигов: {cfgs}\n",
    },
    "leet": {
        "name": "RedConstructor",
        "project_panel": (
            "🗂 <b>{name}</b>\n\n"
            "🛠 cm d5: <code>{cmds}</code>\n"
            "⚙️ cfg5: <code>{cfgs}</code>\n"
            "🌍 l4ng5: <code>{langs}/{total_langs}</code>\n\n"
            "ch0053:"
        ),
        "ask_name": "✏️ 3n73r th3 <b>m0dul3 n4m3</b> (3.g. <code>Weather</code>; <code>Mod</code> g375 4dd3d 4u70):",
        "ask_desc_en": "✏️ 3n73r th3 <b>3ngl15h m0dul3 d35c</b>:",
        "ask_desc_ru": "✏️ 3n73r th3 <b>ru5514n m0dul3 d35c</b>:",
        "ask_cmd_name": "✏️ 3n73r th3 <b>c0mm4nd n4m3</b> (l0w3rc453 l471n, n0 d07, 3.g. <code>ping</code>):",
        "ask_cmd_doc_en": "✏️ 3n73r th3 <b>3ngl15h c0mm4nd d35c</b>:",
        "ask_cmd_doc_ru": "✏️ 3n73r th3 <b>ru5514n c0mm4nd d35c</b>:",
        "ask_cmd_ai_prompt": (
            "🤖 d35cr1b3 wh47 <code>{cmd}</code> 5h0uld d0.\n"
            "3x4mpl3: <i>74k3 r3ply 73x7, m4k3 17 upp3rc453, 4n5w3r w17h 17</i>"
        ),
        "ask_cmd_body": (
            "✍️ 3n73r th3 <b>Py7h0n b0dy</b> f0r <code>{cmd}</code>.\n"
            "b0dy 0nly, n0 <code>def</code>, n0 1mp0r75."
        ),
        "ask_cfg_key": "✏️ 3n73r th3 <b>cfg k3y</b> (3.g. <code>api_key</code>):",
        "ask_cfg_default": "✏️ 3n73r th3 <b>d3f4ul7 v4lu3</b> 0r l34v3 17 bl4nk:",
        "ask_cfg_validator": "⚙️ ch0053 th3 <b>v4l1d470r</b> f0r <code>{key}</code>:",
        "ask_lang_cls": "✏️ 3n73r th3 <b>m0dul3 d35c</b> f0r <b>{lang}</b>:",
        "lang_panel": "🌍 <b>l4ng p4n31</b>\n\nch0053 wh47 70 3d17:",
        "body_mode": "🧩 <b>cmd <code>.{cmd}</code></b>\n\nh0w 70 4dd 7h3 b0dy?",
        "ai_generating": "🤖 <i>41 15 g3n3r471n9 c0d3 f0r <code>{cmd}</code>...</i>",
        "ai_done": "✅ 41 g3n3r473d 7h3 b0dy f0r <code>{cmd}</code>.",
        "ai_error": "❌ 41 3rr0r: <code>{err}</code>",
        "compile_start": "🔨 c0mp1l1n9 m0dul3 <code>{name}</code>...",
        "compile_error": "❌ <b>c0mp1l3 3rr0r:</b>\n<code>{err}</code>",
        "compiled_caption": (
            "✅ <b>RedConstructor</b>\n\n"
            "📦 m0dul3: <code>{name}</code>\n"
            "🛠 cmd5: <code>{cmds}</code>\n"
            "⚙️ cfg5: <code>{cfgs}</code>\n\n"
            "1n574ll w17h <code>.lm</code> by r3ply1n9 70 7h15 f1l3."
        ),
        "lang_saved": "✅ 74n5l4710n f0r <b>{lang}</b> 54v3d.",
        "invalid_module_name": "❌ 1nv4l1d m0dul3 n4m3. u53 l471n l3773r5, d1g175, 5p4c35 0r und3r5c0r3. <code>Mod</code> 157 4u70.",
        "invalid_command_name": "❌ 1nv4l1d cmd n4m3. u53 l0w3rc453 l471n, d1g175 4nd und3r5c0r3.",
        "invalid_config_name": "❌ 1nv4l1d cfg k3y. u53 l0w3rc453 l471n, d1g175 4nd und3r5c0r3.",
        "invalid_code": "❌ 1nv4l1d Py7h0n b0dy:\n<code>{err}</code>",
        "duplicate_project_name": "❌ m0dul3 <code>{name}</code> 4lr34dy 3x1575.",
        "duplicate_command_name": "❌ cmd <code>.{cmd}</code> 4lr34dy 3x1575 1n 7h15 pr0j3c7.",
        "duplicate_config_name": "❌ cfg <code>{key}</code> 4lr34dy 3x1575 1n 7h15 pr0j3c7.",
        "project_not_found": "❌ pr0j3c7 n07 f0und.",
        "list_title": "📋 <b>pr0j3c75</b>",
        "delete_confirm": "🗑 d3l373 pr0j3c7 <b>{name}</b>? n0 und0.",
        "cancel": "❌ c4nc3l3d.",
        "saved_empty": "✅ 3mp7y v4lu3 54v3d.",
        "placeholder_module_name": "Weather",
        "placeholder_desc_en": "English description",
        "placeholder_desc_ru": "ru5514n d35cr1p710n",
        "placeholder_cmd_name": "ping",
        "placeholder_cmd_desc_en": "English command description",
        "placeholder_cmd_desc_ru": "c0mm4nd d35cr1p710n",
        "placeholder_cfg_key": "api_key",
        "placeholder_cfg_default": "d3f4ul7 v4lu3",
        "placeholder_lang_desc": "m0dul3 d35cr1p710n",
        "project_list_line": "📦 <b>{name}</b> | cmd5: {cmds} | cfg5: {cfgs}\n",
        "project_list_line_with_id": "📦 <b>{name}</b> | ID: <code>{pid}</code> | cmd5: {cmds} | cfg5: {cfgs}\n",
    },
    "uwu": {
        "name": "RedConstructor",
        "project_panel": (
            "🗂 <b>{name}</b>\n\n"
            "🛠 commands: <code>{cmds}</code>\n"
            "⚙️ configs: <code>{cfgs}</code>\n"
            "🌍 wanguages: <code>{langs}/{total_langs}</code>\n\n"
            "choose nya:"
        ),
        "ask_name": "✏️ entew the <b>moduwe name</b> (wike <code>Weather</code>; the cwass gets <code>Mod</code> automaticawwy):",
        "ask_desc_en": "✏️ entew the <b>Engwish moduwe desc</b>:",
        "ask_desc_ru": "✏️ entew the <b>Wussian moduwe desc</b>:",
        "ask_cmd_name": "✏️ entew the <b>command name</b> (wowewcase watin, no dot, wike <code>ping</code>):",
        "ask_cmd_doc_en": "✏️ entew the <b>Engwish command desc</b>:",
        "ask_cmd_doc_ru": "✏️ entew the <b>Wussian command desc</b>:",
        "ask_cmd_ai_prompt": (
            "🤖 descwibe what <code>{cmd}</code> shouwd do nya.\n"
            "exampwe: <i>take wepwied text, make it uppewcase, then answew with it</i>"
        ),
        "ask_cmd_body": (
            "✍️ entew the <b>Python body</b> fow <code>{cmd}</code>.\n"
            "just the body, no <code>def</code>, no impowts."
        ),
        "ask_cfg_key": "✏️ entew the <b>config key</b> (wike <code>api_key</code>):",
        "ask_cfg_default": "✏️ entew the <b>defauwt vawue</b> ow weave it empty:",
        "ask_cfg_validator": "⚙️ choose the <b>vawidatow</b> fow <code>{key}</code>:",
        "ask_lang_cls": "✏️ entew the <b>moduwe desc</b> fow <b>{lang}</b>:",
        "lang_panel": "🌍 <b>wanguage panew</b>\n\nchoose what to edit nya:",
        "body_mode": "🧩 <b>command <code>.{cmd}</code></b>\n\nhow shouwd we add the body?",
        "ai_generating": "🤖 <i>ai is cooking code fow <code>{cmd}</code>...</i>",
        "ai_done": "✅ ai made the body fow <code>{cmd}</code>.",
        "ai_error": "❌ ai ewwow: <code>{err}</code>",
        "compile_start": "🔨 compiwing moduwe <code>{name}</code>...",
        "compile_error": "❌ <b>compiwe ewwow:</b>\n<code>{err}</code>",
        "compiled_caption": (
            "✅ <b>RedConstructor</b>\n\n"
            "📦 moduwe: <code>{name}</code>\n"
            "🛠 commands: <code>{cmds}</code>\n"
            "⚙️ configs: <code>{cfgs}</code>\n\n"
            "instaww it with <code>.lm</code> by wepwying to this fiwe nya."
        ),
        "lang_saved": "✅ saved the <b>{lang}</b> twanswation.",
        "invalid_module_name": "❌ invawid moduwe name. use watin wettews, digits, spaces, ow undewscowes. <code>Mod</code> is added automaticawwy.",
        "invalid_command_name": "❌ invawid command name. use wowewcase watin, digits, and undewscowes.",
        "invalid_config_name": "❌ invawid config key. use wowewcase watin, digits, and undewscowes.",
        "invalid_code": "❌ invawid Python body:\n<code>{err}</code>",
        "duplicate_project_name": "❌ moduwe <code>{name}</code> awweady exists.",
        "duplicate_command_name": "❌ command <code>.{cmd}</code> awweady exists in this pwoject.",
        "duplicate_config_name": "❌ config <code>{key}</code> awweady exists in this pwoject.",
        "project_not_found": "❌ pwoject not found.",
        "list_title": "📋 <b>pwojects</b>",
        "delete_confirm": "🗑 dewete pwoject <b>{name}</b>? this can't be undone nya.",
        "cancel": "❌ cancewed.",
        "saved_empty": "✅ empty vawue saved.",
        "placeholder_module_name": "Weather",
        "placeholder_desc_en": "English description",
        "placeholder_desc_ru": "wussian descwiption",
        "placeholder_cmd_name": "ping",
        "placeholder_cmd_desc_en": "English command description",
        "placeholder_cmd_desc_ru": "command descwiption",
        "placeholder_cfg_key": "api_key",
        "placeholder_cfg_default": "defauwt vawue",
        "placeholder_lang_desc": "moduwe descwiption",
        "project_list_line": "📦 <b>{name}</b> | commands: {cmds} | configs: {cfgs}\n",
        "project_list_line_with_id": "📦 <b>{name}</b> | ID: <code>{pid}</code> | commands: {cmds} | configs: {cfgs}\n",
    },
}


def _make_project_id() -> str:
    return hashlib.md5(str(time.time()).encode()).hexdigest()[:8]


def _indent(code: str, spaces: int = 8) -> str:
    pad = " " * spaces
    return "\n".join(pad + line if line.strip() else line for line in code.splitlines())


def _detect_imports(code: str) -> list:
    imports = []
    checks = {
        "import re": r"\bre\.",
        "import json": r"\bjson\.",
        "import aiohttp": r"\baiohttp\b",
        "import asyncio": r"\basyncio\.",
        "import os": r"\bos\.",
        "import time": r"\btime\.",
        "import random": r"\brandom\.",
        "import datetime": r"\bdatetime\b",
        "import io": r"\bio\.",
        "import hashlib": r"\bhashlib\.",
    }
    for imp, pattern in checks.items():
        if re.search(pattern, code):
            imports.append(imp)
    return imports


AUTO_IMPORT_HINTS = {
    "aiohttp": "import aiohttp",
    "asyncio": "import asyncio",
    "base64": "import base64",
    "collections": "import collections",
    "datetime": "import datetime",
    "decimal": "import decimal",
    "functools": "import functools",
    "hashlib": "import hashlib",
    "html": "import html",
    "io": "import io",
    "itertools": "import itertools",
    "json": "import json",
    "math": "import math",
    "os": "import os",
    "random": "import random",
    "re": "import re",
    "statistics": "import statistics",
    "string": "import string",
    "time": "import time",
    "traceback": "import traceback",
    "typing": "import typing",
    "uuid": "import uuid",
    "Path": "from pathlib import Path",
    "quote": "from urllib.parse import quote",
    "unquote": "from urllib.parse import unquote",
    "urlencode": "from urllib.parse import urlencode",
    "date": "from datetime import date",
    "timedelta": "from datetime import timedelta",
    "timezone": "from datetime import timezone",
}

AUTO_IMPORT_RESERVED_NAMES = {
    "Message",
    "loader",
    "self",
    "utils",
}


def _extract_root_name(node: ast.AST) -> typing.Optional[str]:
    current = node
    while isinstance(current, ast.Attribute):
        current = current.value
    if isinstance(current, ast.Name):
        return current.id
    return None


def _ast_detect_imports(code: str) -> set:
    try:
        tree = ast.parse(code or "", filename="<module>")
    except SyntaxError:
        return set()

    imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            root_name = _extract_root_name(node)
            hint = AUTO_IMPORT_HINTS.get(root_name)
            if hint:
                imports.add(hint)
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            if node.id in AUTO_IMPORT_RESERVED_NAMES:
                continue
            hint = AUTO_IMPORT_HINTS.get(node.id)
            if hint:
                imports.add(hint)
    return imports


def _missing_name_to_imports(err: typing.Optional[str]) -> set:
    if not err:
        return set()

    imports = set()
    for missing_name in re.findall(r"name '([^']+)' is not defined", err):
        if missing_name in AUTO_IMPORT_RESERVED_NAMES:
            continue
        hint = AUTO_IMPORT_HINTS.get(missing_name)
        if hint:
            imports.add(hint)
    return imports


def _validate_syntax(code: str) -> typing.Optional[str]:
    try:
        compile(code, "<module>", "exec")
        return None
    except SyntaxError as e:
        return "SyntaxError at line {}: {}".format(e.lineno, e.msg)


def _validate_body_syntax(code: str) -> typing.Optional[str]:
    wrapped = "async def _tmp(self, message):\n"
    wrapped += _indent(code or "pass", 4)
    syntax_error = _validate_syntax(wrapped)
    if syntax_error:
        return syntax_error

    return _validate_runtime_contracts(wrapped, line_offset=1)


def _literal(node: ast.AST) -> typing.Any:
    try:
        return ast.literal_eval(node)
    except Exception:
        return None


def _call_name(node: ast.AST) -> typing.Optional[str]:
    parts = []
    current = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value

    if isinstance(current, ast.Name):
        parts.append(current.id)
        return ".".join(reversed(parts))

    return None


def _call_label(node: ast.Call) -> str:
    return _call_name(node.func) or getattr(node.func, "attr", None) or getattr(
        node.func, "id", None
    ) or "<call>"


def _build_parent_map(tree: ast.AST) -> dict:
    parents = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[child] = parent
    return parents


def _collect_target_names(target: ast.AST, names: set) -> None:
    if isinstance(target, ast.Name):
        names.add(target.id)
        return

    if isinstance(target, ast.Starred):
        _collect_target_names(target.value, names)
        return

    if isinstance(target, (ast.Tuple, ast.List)):
        for item in target.elts:
            _collect_target_names(item, names)


class _ScopeDefCollector(ast.NodeVisitor):
    def __init__(self):
        self.names = set()

    def visit_FunctionDef(self, node: ast.FunctionDef):
        self.names.add(node.name)

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_ClassDef(self, node: ast.ClassDef):
        self.names.add(node.name)

    def visit_Import(self, node: ast.Import):
        for alias in node.names:
            self.names.add(alias.asname or alias.name.split(".", 1)[0])

    def visit_ImportFrom(self, node: ast.ImportFrom):
        for alias in node.names:
            self.names.add(alias.asname or alias.name)

    def visit_Assign(self, node: ast.Assign):
        for target in node.targets:
            _collect_target_names(target, self.names)
        self.generic_visit(node.value)

    def visit_AnnAssign(self, node: ast.AnnAssign):
        _collect_target_names(node.target, self.names)
        if node.value is not None:
            self.generic_visit(node.value)

    def visit_AugAssign(self, node: ast.AugAssign):
        _collect_target_names(node.target, self.names)
        self.generic_visit(node.value)

    def visit_NamedExpr(self, node):
        _collect_target_names(node.target, self.names)
        self.generic_visit(node.value)

    def visit_For(self, node: ast.For):
        _collect_target_names(node.target, self.names)
        self.generic_visit(node.iter)
        for stmt in node.body:
            self.visit(stmt)
        for stmt in node.orelse:
            self.visit(stmt)

    visit_AsyncFor = visit_For

    def visit_With(self, node: ast.With):
        for item in node.items:
            if item.optional_vars is not None:
                _collect_target_names(item.optional_vars, self.names)
            self.generic_visit(item.context_expr)
        for stmt in node.body:
            self.visit(stmt)

    visit_AsyncWith = visit_With

    def visit_ExceptHandler(self, node: ast.ExceptHandler):
        if isinstance(node.name, str):
            self.names.add(node.name)
        for stmt in node.body:
            self.visit(stmt)


class _UndefinedNameCollector(ast.NodeVisitor):
    def __init__(self, known_names: set, *, line_offset: int = 0):
        self.known_names = set(known_names)
        self.line_offset = line_offset
        self.issues = []
        self._seen = set()

    def _error(self, node: ast.Name):
        line = max(1, getattr(node, "lineno", 1) - self.line_offset)
        key = (line, node.id)
        if key in self._seen:
            return
        self._seen.add(key)
        self.issues.append(f"line {line}: name '{node.id}' is not defined")

    def visit_Name(self, node: ast.Name):
        if isinstance(node.ctx, ast.Load) and node.id not in self.known_names:
            self._error(node)

    def visit_FunctionDef(self, node: ast.FunctionDef):
        return

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_ClassDef(self, node: ast.ClassDef):
        return

    def visit_Lambda(self, node: ast.Lambda):
        return

    def visit_ListComp(self, node: ast.ListComp):
        return

    visit_SetComp = visit_ListComp
    visit_DictComp = visit_ListComp
    visit_GeneratorExp = visit_ListComp


def _function_arg_names(node: typing.Union[ast.FunctionDef, ast.AsyncFunctionDef]) -> set:
    args = set()
    for arg in getattr(node.args, "posonlyargs", []):
        args.add(arg.arg)
    for arg in node.args.args:
        args.add(arg.arg)
    for arg in node.args.kwonlyargs:
        args.add(arg.arg)
    if node.args.vararg:
        args.add(node.args.vararg.arg)
    if node.args.kwarg:
        args.add(node.args.kwarg.arg)
    return args


def _collect_scope_defs(body: typing.Iterable[ast.stmt]) -> set:
    collector = _ScopeDefCollector()
    for stmt in body:
        collector.visit(stmt)
    return collector.names


def _validate_name_resolution(
    tree: ast.AST,
    *,
    line_offset: int = 0,
    max_issues: int = 8,
) -> typing.List[str]:
    builtin_names = set(dir(builtins))
    implicit_globals = {
        "Message",
        "aiohttp",
        "asyncio",
        "datetime",
        "hashlib",
        "io",
        "json",
        "loader",
        "os",
        "random",
        "re",
        "time",
        "utils",
    }
    module_body = getattr(tree, "body", [])
    module_defs = _collect_scope_defs(module_body)
    module_known = builtin_names | implicit_globals | module_defs
    issues = []

    def _collect_known_for_class(node: ast.ClassDef, outer_known: set) -> set:
        return outer_known | _collect_scope_defs(node.body)

    def _walk_scope(body: typing.Iterable[ast.stmt], known_names: set):
        if len(issues) >= max_issues:
            return

        collector = _UndefinedNameCollector(known_names, line_offset=line_offset)
        for stmt in body:
            collector.visit(stmt)
            if len(collector.issues) + len(issues) >= max_issues:
                break

        for issue in collector.issues:
            if issue not in issues:
                issues.append(issue)
                if len(issues) >= max_issues:
                    return

        for stmt in body:
            if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
                fn_known = set(known_names) | _function_arg_names(stmt) | _collect_scope_defs(stmt.body)
                _walk_scope(stmt.body, fn_known)
            elif isinstance(stmt, ast.ClassDef):
                class_known = _collect_known_for_class(stmt, known_names | {stmt.name})
                _walk_scope(stmt.body, class_known)

            if len(issues) >= max_issues:
                return

    _walk_scope(module_body, module_known)
    return issues


def _signature_map() -> dict:
    targets = {
        "utils.answer": getattr(utils, "answer", None),
        "utils.answer_file": getattr(utils, "answer_file", None),
        "asyncio.sleep": asyncio.sleep,
    }
    signatures = {}
    for name, obj in targets.items():
        if obj is None:
            continue
        with contextlib.suppress(TypeError, ValueError):
            signatures[name] = inspect.signature(obj)
    return signatures


def _bind_signature_error(node: ast.Call, signature: inspect.Signature) -> typing.Optional[str]:
    if any(isinstance(arg, ast.Starred) for arg in node.args):
        return None

    kwargs = {}
    for kw in node.keywords:
        if kw.arg is None:
            return None
        kwargs[kw.arg] = object()

    try:
        signature.bind(*([object()] * len(node.args)), **kwargs)
        return None
    except TypeError as e:
        return str(e)


def _is_awaited_call(node: ast.Call, parents: dict) -> bool:
    parent = parents.get(node)
    if isinstance(parent, ast.Await):
        return True

    if isinstance(parent, ast.Call):
        scheduler = _call_name(parent.func) or getattr(parent.func, "attr", None)
        if scheduler in {"asyncio.create_task", "create_task"}:
            return True

    return False


def _validate_runtime_contracts(
    code: str,
    *,
    line_offset: int = 0,
    max_issues: int = 8,
) -> typing.Optional[str]:
    try:
        tree = ast.parse(code, filename="<module>")
    except SyntaxError:
        return None

    parents = _build_parent_map(tree)
    signatures = _signature_map()
    awaited_paths = {
        "utils.answer",
        "utils.answer_file",
        "asyncio.sleep",
        "self.inline.form",
        "self.inline.gallery",
        "self.inline.list",
        "message.delete",
        "message.edit",
        "message.get_reply_message",
        "message.reply",
        "message.respond",
    }
    awaited_attrs = {
        "delete",
        "download_media",
        "edit",
        "get_reply_message",
        "invoke",
        "reply",
        "respond",
        "send_file",
        "send_message",
        "unload",
    }

    issues = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncFunctionDef):
            continue
        arg_names = [arg.arg for arg in node.args.args]
        line = max(1, getattr(node, "lineno", 1) - line_offset)
        if node.name.endswith("cmd") or node.name.endswith("watcher"):
            if arg_names[:2] != ["self", "message"] or len(arg_names) != 2:
                issues.append(
                    f"line {line}: async def {node.name} must have signature (self, message)"
                )
        elif node.name.endswith("loop"):
            if arg_names[:1] != ["self"] or len(arg_names) != 1:
                issues.append(
                    f"line {line}: async def {node.name} must have signature (self)"
                )
        if len(issues) >= max_issues:
            break

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        call_name = _call_name(node.func)
        call_attr = getattr(node.func, "attr", None)
        line = max(1, getattr(node, "lineno", 1) - line_offset)
        label = _call_label(node)

        signature = signatures.get(call_name)
        if signature is not None:
            bind_error = _bind_signature_error(node, signature)
            if bind_error:
                issues.append(f"line {line}: {label}() {bind_error}")

        if (
            call_name in awaited_paths or call_attr in awaited_attrs
        ) and not _is_awaited_call(node, parents):
            issues.append(f"line {line}: {label}() looks async and must be awaited")

        if len(issues) >= max_issues:
            break

    if len(issues) < max_issues:
        for issue in _validate_name_resolution(
            tree,
            line_offset=line_offset,
            max_issues=max_issues - len(issues),
        ):
            if issue not in issues:
                issues.append(issue)
            if len(issues) >= max_issues:
                break

    if not issues:
        return None

    return "Runtime validation failed:\n" + "\n".join(issues[:max_issues])


def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text or "")


def _normalize_module_name(raw_name: str) -> typing.Tuple[typing.Optional[dict], typing.Optional[str]]:
    value = re.sub(r"\s+", " ", (raw_name or "").strip())
    if not value:
        return None, "empty"

    display_name = value
    if display_name.lower().endswith("mod") and len(display_name) > 3:
        display_name = display_name[:-3].rstrip(" _-") or display_name

    class_seed = re.sub(r"[^A-Za-z0-9_ ]+", " ", display_name)
    class_seed = re.sub(r"[_ ]+", " ", class_seed).strip()
    if not class_seed:
        return None, "empty"

    parts = []
    for chunk in class_seed.split():
        if not re.search(r"[A-Za-z]", chunk):
            continue
        head = chunk[0].upper()
        tail = re.sub(r"[^A-Za-z0-9_]", "", chunk[1:])
        parts.append(head + tail)

    if not parts:
        return None, "empty"

    class_base = "".join(parts)
    if not re.match(r"^[A-Za-z][A-Za-z0-9_]{0,62}$", class_base):
        return None, "bad"

    class_name = class_base if class_base.endswith("Mod") else f"{class_base}Mod"
    if len(class_name) > 64:
        return None, "bad"

    return {"display_name": display_name, "class_name": class_name}, None


def _coerce_validator_value(raw: str) -> typing.Any:
    raw = (raw or "").strip()
    if not raw:
        return ""
    with contextlib.suppress(Exception):
        return ast.literal_eval(raw)
    return raw


def _parse_validator_meta(validator: str, raw: str) -> typing.Tuple[typing.Optional[dict], typing.Optional[str]]:
    text = (raw or "").strip()
    if validator in {"Choice", "MultiChoice"}:
        items = [
            _coerce_validator_value(part)
            for part in re.split(r"[\n,]+", text)
            if part.strip()
        ]
        if not items:
            return None, "possible_values"
        return {"possible_values": items}, None

    if validator == "RegExp":
        if not text:
            return None, "regex"
        try:
            re.compile(text)
        except re.error:
            return None, "regex"
        return {"regex": text}, None

    return {}, None


def _validator_expr(cfg: dict) -> str:
    validator = cfg.get("validator", "String")
    meta = cfg.get("validator_args") or {}

    if validator in {"Choice", "MultiChoice"}:
        possible_values = meta.get("possible_values") or []
        return "loader.validators.{}({})".format(validator, repr(list(possible_values)))

    if validator == "RegExp":
        regex = meta.get("regex", "")
        return "loader.validators.RegExp({})".format(repr(regex))

    return "loader.validators.{}()".format(validator)


def _validate_config_validator(cfg: dict) -> typing.Optional[str]:
    key = cfg.get("key", "?")
    validator = cfg.get("validator", "String")
    meta = cfg.get("validator_args") or {}
    default = cfg.get("default", "")

    if validator in {"Choice", "MultiChoice"}:
        possible_values = meta.get("possible_values")
        if not isinstance(possible_values, list) or not possible_values:
            return "Config {!r}: validator {} requires a non-empty possible_values list".format(
                key,
                validator,
            )
        if default != "" and validator == "Choice" and default not in possible_values:
            return "Config {!r}: default {!r} is not in Choice possible_values".format(
                key,
                default,
            )
        if default != "" and validator == "MultiChoice":
            values = default if isinstance(default, (list, tuple, set)) else [default]
            if any(value not in possible_values for value in values):
                return "Config {!r}: default {!r} is not valid for MultiChoice".format(
                    key,
                    default,
                )
        return None

    if validator == "RegExp":
        regex = meta.get("regex")
        if not isinstance(regex, str) or not regex:
            return "Config {!r}: validator RegExp requires a regex pattern".format(key)
        try:
            re.compile(regex)
        except re.error as e:
            return "Config {!r}: invalid regex {!r}: {}".format(key, regex, e)

    parsed_default, error = _parse_default_for_validator(validator, default, meta)
    if error:
        return "Config {!r}: default {!r} is invalid for validator {}".format(
            key,
            default,
            validator,
        )

    return None


def _parse_default_for_validator(
    validator: str,
    raw: typing.Any,
    validator_args: typing.Optional[dict] = None,
) -> typing.Tuple[typing.Any, typing.Optional[str]]:
    meta = validator_args or {}

    if raw == "":
        if validator in {"String", "Hidden"}:
            return "", None
        if validator == "MultiChoice":
            return [], None
        if validator == "Series":
            return [], None
        return None, "required"

    if validator == "Boolean":
        if isinstance(raw, bool):
            return raw, None
        text = str(raw).strip().lower()
        if text in {"1", "true", "yes", "on"}:
            return True, None
        if text in {"0", "false", "no", "off"}:
            return False, None
        return None, "boolean"

    if validator == "Integer":
        try:
            return int(str(raw).strip()), None
        except Exception:
            return None, "integer"

    if validator == "Float":
        try:
            return float(str(raw).strip()), None
        except Exception:
            return None, "float"

    if validator == "TelegramID":
        try:
            return int(str(raw).strip()), None
        except Exception:
            return None, "telegram_id"

    if validator == "Series":
        if isinstance(raw, (list, tuple, set)):
            values = list(raw)
        else:
            values = [item.strip() for item in re.split(r"[\n,]+", str(raw)) if item.strip()]
        return values, None

    if validator == "Choice":
        allowed = meta.get("possible_values") or []
        if raw not in allowed:
            return None, "choice"
        return raw, None

    if validator == "MultiChoice":
        allowed = meta.get("possible_values") or []
        values = list(raw) if isinstance(raw, (list, tuple, set)) else [raw]
        if any(value not in allowed for value in values):
            return None, "multichoice"
        return values, None

    return raw, None


def _sanitize_ai_output(text: str) -> str:
    cleaned = (text or "").strip()
    cleaned = re.sub(r"^```(?:python)?\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    cleaned = cleaned.strip()
    lines = []
    for line in cleaned.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def _validate_ai_body_contracts(code: str) -> typing.Optional[str]:
    text = (code or "").strip()
    if not text:
        return "AI returned empty body"

    checks = [
        (r"```", "AI returned markdown fence"),
        (r"^\s*(?:from\s+\S+\s+import|import\s+\S+)", "AI returned import statement"),
        (r"^\s*(?:async\s+def|def)\b", "AI returned function definition instead of body"),
        (r"^\s*class\b", "AI returned class definition instead of body"),
        (r"^\s*@", "AI returned decorator instead of body"),
        (r'"""|\'\'\'', "AI returned docstring or triple-quoted block"),
        (
            r'parse_mode\s*=\s*[\'"](?:markdown|markdownv2|md)[\'"]',
            "AI used Markdown parse mode; formatting must be HTML",
        ),
    ]
    for pattern, error in checks:
        if re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE):
            return error

    try:
        for token in tokenize.generate_tokens(io.StringIO(text).readline):
            if token.type == tokenize.COMMENT:
                return "AI returned comment; comments are not allowed"
    except tokenize.TokenError as e:
        return "AI output tokenization failed: {}".format(e)

    return None


def _template_body(
    template_key: str,
    payload: str = "",
    *,
    cmd_name: str = "command",
) -> typing.Optional[typing.Tuple[str, dict]]:
    payload = (payload or "").strip()

    if template_key == "ping":
        return (
            'await utils.answer(message, self._ma_cmd_text("{}", "pong", "🏓 Pong"))'.format(cmd_name),
            {"texts": {"pong": "🏓 Pong"}},
        )
    if template_key == "fixed_text":
        text = payload or "Hello"
        return (
            'await utils.answer(message, self._ma_cmd_text("{}", "fixed_text", {}))'.format(
                cmd_name,
                repr(text),
            ),
            {"texts": {"fixed_text": text}},
        )
    if template_key == "echo_args":
        return (
            'args = utils.get_args_raw(message)\n'
            'if not args:\n'
            '    return await utils.answer(message, self._ma_cmd_text("{}", "no_args", "No args provided."))\n'
            'await utils.answer(message, args)'.format(cmd_name)
            ,
            {"texts": {"no_args": "No args provided."}},
        )
    if template_key == "echo_reply":
        return (
            'reply = await message.get_reply_message()\n'
            'if not reply:\n'
            '    return await utils.answer(message, self._ma_cmd_text("{}", "need_reply", "Reply to a message."))\n'
            'await utils.answer(message, reply.raw_text or "")'.format(cmd_name)
            ,
            {"texts": {"need_reply": "Reply to a message."}},
        )
    if template_key == "upper_reply":
        return (
            'reply = await message.get_reply_message()\n'
            'if not reply or not reply.raw_text:\n'
            '    return await utils.answer(message, self._ma_cmd_text("{}", "need_text_reply", "Reply to a text message."))\n'
            'await utils.answer(message, reply.raw_text.upper())'.format(cmd_name)
            ,
            {"texts": {"need_text_reply": "Reply to a text message."}},
        )
    if template_key == "lower_reply":
        return (
            'reply = await message.get_reply_message()\n'
            'if not reply or not reply.raw_text:\n'
            '    return await utils.answer(message, self._ma_cmd_text("{}", "need_text_reply", "Reply to a text message."))\n'
            'await utils.answer(message, reply.raw_text.lower())'.format(cmd_name)
            ,
            {"texts": {"need_text_reply": "Reply to a text message."}},
        )
    if template_key == "reverse_text":
        return (
            'reply = await message.get_reply_message()\n'
            'source = (reply.raw_text if reply and reply.raw_text else utils.get_args_raw(message))\n'
            'if not source:\n'
            '    return await utils.answer(message, self._ma_cmd_text("{}", "need_source", "Reply or pass text."))\n'
            'await utils.answer(message, source[::-1])'.format(cmd_name)
            ,
            {"texts": {"need_source": "Reply or pass text."}},
        )
    if template_key == "random_choice":
        values = [item.strip() for item in re.split(r"[\n,]+", payload) if item.strip()]
        if not values:
            return None
        return (
            'values = self._ma_cmd_list("{}", "choices", {})\n'
            "await utils.answer(message, random.choice(values))"
        ).format(cmd_name, repr(values)), {"lists": {"choices": values}}
    if template_key == "me_info":
        return (
            "me = await message.client.get_me()\n"
            'template = self._ma_cmd_text("{}", "profile", "ID: <code>{{}}</code>\\\\nName: {{}}")\n'
            'text = template.format(me.id, getattr(me, "first_name", "") or "")\n'
            'await utils.answer(message, text)'.format(cmd_name)
            ,
            {"texts": {"profile": "ID: <code>{}</code>\nName: {}"}},
        )
    if template_key == "config_dump":
        return (
            "lines = []\n"
            "for key in self.config:\n"
            '    lines.append("{} = <code>{}</code>".format(key, repr(self.config[key])))\n'
            'empty = self._ma_cmd_text("{}", "empty_configs", "No config values.")\n'
            'await utils.answer(message, "\\n".join(lines) if lines else empty)'.format(cmd_name)
            ,
            {"texts": {"empty_configs": "No config values."}},
        )
    if template_key == "db_save":
        parts = payload.splitlines()
        key = (parts[0] if parts else "").strip()
        value = "\n".join(parts[1:]).strip()
        if not key:
            return None
        if not value:
            value = "value"
        return (
            'self.db.set(self.strings["name"], {}, {})\n'
            'await utils.answer(message, self._ma_cmd_text("{}", "saved", "Saved"))'.format(
                repr(key),
                repr(value),
                cmd_name,
            ),
            {"texts": {"saved": "Saved"}},
        )
    if template_key == "db_load":
        key = payload.strip()
        if not key:
            return None
        return (
            'val = self.db.get(self.strings["name"], {}, self._ma_cmd_text("{}", "not_found", "Not found"))\n'
            "await utils.answer(message, str(val))".format(
                repr(key),
                cmd_name,
            ),
            {"texts": {"not_found": "Not found"}},
        )

    return None


def _collect_project_imports(project: dict) -> set:
    imports = set()
    for cmd in project.get("commands", {}).values():
        body = _command_body_source(cmd)
        imports.update(_detect_imports(body))
        imports.update(_ast_detect_imports(body))
    for watcher in project.get("watchers", {}).values():
        body = _command_body_source(watcher)
        imports.update(_detect_imports(body))
        imports.update(_ast_detect_imports(body))
    for loop in project.get("loops", {}).values():
        body = _command_body_source(loop)
        imports.update(_detect_imports(body))
        imports.update(_ast_detect_imports(body))
    return imports


def _command_doc(project: dict, cmd_name: str, lang: str) -> str:
    command_docs = project.get("command_docs") or {}
    if isinstance(command_docs, dict):
        per_command = command_docs.get(cmd_name) or {}
        if isinstance(per_command, dict):
            value = per_command.get(lang)
            if value:
                return str(value)

    return str(project.get("strings", {}).get(lang, {}).get("{}_doc".format(cmd_name), "") or "")


def _runtime_strings(strings: dict, command_names: typing.Iterable[str]) -> dict:
    command_doc_keys = {"{}_doc".format(cmd_name) for cmd_name in command_names}
    cleaned = {}
    for lang, lang_strings in (strings or {}).items():
        if not isinstance(lang_strings, dict):
            continue
        cleaned[lang] = {
            key: value
            for key, value in lang_strings.items()
            if key not in command_doc_keys
        }
    return cleaned


def _project_runtime_strings(project: dict) -> dict:
    strings = _runtime_strings(project.get("strings", {}), project.get("commands", {}).keys())
    resources = project.get("command_resources") or {}
    if not isinstance(resources, dict):
        return strings

    for cmd_name, per_command in resources.items():
        if not isinstance(per_command, dict):
            continue
        for lang, lang_bucket in per_command.items():
            normalized_lang = _normalize_lang_code(lang)
            if not normalized_lang or not isinstance(lang_bucket, dict):
                continue
            lang_strings = strings.setdefault(normalized_lang, {})
            for kind in ("texts", "lists", "values"):
                values = _flatten_lang_dict(lang_bucket.get(kind))
                for key, value in values.items():
                    lang_strings[_command_resource_string_key(cmd_name, kind, key)] = _serialize_resource_value(
                        kind,
                        value,
                    )
    return strings


def _build_module_code(project: dict, extra_imports: typing.Optional[typing.Iterable[str]] = None) -> str:
    meta = project["meta"]
    commands = project["commands"]
    watchers = project.get("watchers", {})
    loops = project.get("loops", {})
    dependencies = _parse_dependencies(project.get("dependencies") or [])
    configs = project["configs"]
    strings = _project_runtime_strings(project)
    project_langs = _project_languages(project)

    normalized_name, _ = _normalize_module_name(meta.get("name", "MyModule"))
    module_name = meta.get("class_name") or (
        normalized_name["class_name"] if normalized_name else "MyModuleMod"
    )
    visible_name = meta.get("name") or (
        normalized_name["display_name"] if normalized_name else module_name.removesuffix("Mod")
    )
    prefix = meta.get("prefix", "")
    version_expr = _version_tuple_expr(meta.get("version"))

    import_lines = _collect_project_imports(project)
    if extra_imports:
        import_lines.update(extra_imports)

    lines = []
    for meta_comment in (
        ("license", "IyBMaWNlbnNlOiB7fQ=="),
        ("developer", "IyBtZXRhIGRldmVsb3Blcjoge30="),
        ("banner", "IyBtZXRhIGJhbm5lcjoge30="),
        ("pic", "IyBtZXRhIHBpYzoge30="),
        ("heroku_min", "IyBzY29wZSBoZXJva3VfbWluOiB7fQ=="),
    ):
        raw_value = _normalize_meta_text(meta.get(meta_comment[0]))
        if raw_value:
            lines.append(_b64decode_text(meta_comment[1]).format(raw_value))
    if lines:
        lines.append("")

    lines.extend(
        [
            "import json",
            "from herokutl.types import Message",
            "from .. import loader, translations, utils",
        ]
    )
    for imp in sorted(import_lines):
        lines.append(imp)

    lines += ["", "", "@loader.tds", "class {}(loader.Module):".format(module_name)]
    lines.append("")

    def _fmt_dict(lang_strings: dict, varname: str) -> list:
        out = ["    {} = {{".format(varname)]
        for k, v in lang_strings.items():
            out.append("        {}: {},".format(repr(str(k)), repr(str(v))))
        out.append("    }")
        return out

    base = {"name": visible_name}
    base.update(strings.get("en", {}))
    lines += _fmt_dict(base, "strings")
    lines.append("")

    if version_expr:
        lines.append("    __version__ = {}".format(version_expr))
        lines.append("")

    if dependencies:
        lines.append("    __dependencies__ = {}".format(repr(dependencies)))
        lines.append("")

    for lang in project_langs:
        if lang == "en":
            continue
        lang_dict = strings.get(lang, {})
        if lang_dict:
            lines += _fmt_dict(lang_dict, "strings_{}".format(lang))
            lines.append("")

    lines += [
        "    def _ma_lang(self):",
        '        raw_langs = ""',
        "        try:",
        '            raw_langs = self._db.get(translations.__name__, "lang", "en")',
        "        except Exception:",
        '            raw_langs = "en"',
        "        if not isinstance(raw_langs, str):",
        '            return "en"',
        "        for lang in raw_langs.split()[::-1]:",
        '            lang = str(lang).lower().replace("-", "_")',
        '            if lang == "ua":',
        '                lang = "uk"',
        '            if hasattr(type(self), "strings_{}".format(lang)):',
        "                return lang",
        '        return "en"',
        "",
        "    def _ma_pack(self):",
        "        base = dict(type(self).strings)",
        "        lang = self._ma_lang()",
        '        override = dict(getattr(type(self), "strings_{}".format(lang), {})) if lang != "en" else {}',
        "        return {**base, **override}",
        "",
        "    def _ma_cmd_raw(self, cmd: str, kind: str, key: str, default=None):",
        '        pack = self._ma_pack()',
        '        value = pack.get("__cmd__{}__{}__{}".format(cmd, kind, key))',
        "        if value in (None, ''):",
        "            return default",
        "        return value",
        "",
        "    def _ma_cmd_text(self, cmd: str, key: str, default: str = '') -> str:",
        "        value = self._ma_cmd_raw(cmd, 'text', key, default)",
        "        return str(default if value is None else value)",
        "",
        "    def _ma_cmd_list(self, cmd: str, key: str, default=None):",
        "        default = [] if default is None else default",
        "        raw = self._ma_cmd_raw(cmd, 'list', key, None)",
        "        if raw is None:",
        "            return list(default)",
        "        try:",
        "            value = json.loads(raw)",
        "        except Exception:",
        "            return list(default)",
        "        return value if isinstance(value, list) else list(default)",
        "",
        "    def _ma_cmd_value(self, cmd: str, key: str, default=None):",
        "        raw = self._ma_cmd_raw(cmd, 'value', key, None)",
        "        if raw is None:",
        "            return default",
        "        try:",
        "            return json.loads(raw)",
        "        except Exception:",
        "            return default",
        "",
    ]

    if configs:
        lines.append("    def __init__(self):")
        lines.append("        self.config = loader.ModuleConfig(")
        for cfg in configs:
            key = cfg["key"]
            default = repr(cfg.get("default", ""))
            validator = cfg.get("validator", "String")
            lines.append("            loader.ConfigValue(")
            lines.append('                "{}",'.format(key))
            lines.append("                {},".format(default))
            lines.append('                lambda: self.strings("{}_doc"),'.format(key))
            lines.append("                validator={},".format(_validator_expr(cfg)))
            lines.append("            ),")
        lines.append("        )")
        lines.append("")

    for cmd_name, cmd in commands.items():
        dec_args = []
        for lang in project_langs:
            doc = _command_doc(project, cmd_name, lang)
            if doc:
                dec_args.append(
                    '{}_doc="{}"'.format(lang, str(doc).replace('"', '\\"'))
                )

        tags = cmd.get("tags", {})
        for tag_key, tag_value in tags.items():
            if isinstance(tag_value, str):
                dec_args.append('{}="{}"'.format(tag_key, tag_value.replace('"', '\\"')))
            else:
                dec_args.append("{}={}".format(tag_key, repr(tag_value)))

        if dec_args:
            lines.append("    @loader.command(")
            for arg in dec_args:
                lines.append("        {},".format(arg))
            lines.append("    )")
        else:
            lines.append("    @loader.command()")

        full_cmd = "{}{}".format(prefix, cmd_name) if prefix else cmd_name
        lines.append("    async def {}cmd(self, message: Message):".format(full_cmd))
        body = _command_body_source(cmd).strip()
        if body:
            for body_line in body.splitlines():
                lines.append("        " + body_line)
        else:
            lines.append("        pass")
        lines.append("")

    for watcher_name, watcher in watchers.items():
        tags = dict((watcher or {}).get("tags") or {})
        if tags:
            lines.append("    @loader.watcher(")
            for tag_key, tag_value in sorted(tags.items()):
                if tag_value:
                    lines.append("        {}=True,".format(tag_key))
            lines.append("    )")
        else:
            lines.append("    @loader.watcher()")
        lines.append("    async def {}watcher(self, message: Message):".format(watcher_name))
        body = _command_body_source(watcher).strip()
        if body:
            for body_line in body.splitlines():
                lines.append("        " + body_line)
        else:
            lines.append("        pass")
        lines.append("")

    for loop_name, loop_meta in loops.items():
        interval = int((loop_meta or {}).get("interval") or 60)
        lines.append("    @loader.loop(interval={})".format(interval))
        lines.append("    async def {}loop(self):".format(loop_name))
        body = _command_body_source(loop_meta).strip()
        if body:
            for body_line in body.splitlines():
                lines.append("        " + body_line)
        else:
            lines.append("        pass")
        lines.append("")

    return "\n".join(lines)


def _build_module_code_with_import_repair(
    project: dict,
    *,
    max_passes: int = 4,
) -> typing.Tuple[str, typing.Optional[str], set]:
    resolved_imports = _collect_project_imports(project)
    code = _build_module_code(project, resolved_imports)
    err = None

    for _ in range(max_passes):
        err = _validate_syntax(code)
        if not err:
            err = _validate_runtime_contracts(code)

        inferred_imports = set(resolved_imports)
        inferred_imports.update(_ast_detect_imports(code))
        inferred_imports.update(_missing_name_to_imports(err))

        if inferred_imports == resolved_imports:
            break

        resolved_imports = inferred_imports
        code = _build_module_code(project, resolved_imports)

    if err is None:
        err = _validate_syntax(code)
        if not err:
            err = _validate_runtime_contracts(code)

    return code, err, resolved_imports


def _provider_base_url(provider: str, model: str, custom_base_url: str = "") -> str:
    if custom_base_url.strip():
        return custom_base_url.strip()
    return AI_PROVIDER_BASE_URLS.get(provider, "").format(model=model)


def _provider_request(
    provider: str,
    token: str,
    model: str,
    prompt: str,
    base_url: str,
) -> typing.Tuple[str, dict, typing.Optional[dict], typing.Optional[dict]]:
    headers = {"Content-Type": "application/json"}

    if provider == "anthropic":
        headers["x-api-key"] = token
        headers["anthropic-version"] = "2023-06-01"
        payload = {
            "model": model,
            "max_tokens": 1024,
            "system": AI_SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": prompt}],
        }
        return base_url, headers, payload, None

    if provider == "google":
        url = base_url
        if token:
            sep = "&" if "?" in url else "?"
            url = "{}{}{}".format(url, sep, urlencode({"key": token}))
        payload = {
            "system_instruction": {"parts": [{"text": AI_SYSTEM_PROMPT}]},
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.2},
        }
        return url, headers, payload, None

    headers["Authorization"] = "Bearer {}".format(token)
    payload = {
        "model": model,
        "temperature": 0.2,
        "messages": [
            {"role": "system", "content": AI_SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
    }
    return base_url, headers, payload, None


def _extract_ai_text(provider: str, data: dict) -> str:
    if provider == "anthropic":
        content = data.get("content") or []
        if content and isinstance(content[0], dict):
            return str(content[0].get("text", "")).strip()
        return ""

    if provider == "google":
        candidates = data.get("candidates") or []
        if not candidates:
            return ""
        parts = (((candidates[0] or {}).get("content") or {}).get("parts") or [])
        if parts and isinstance(parts[0], dict):
            return str(parts[0].get("text", "")).strip()
        return ""

    choices = data.get("choices") or []
    if choices and isinstance(choices[0], dict):
        message = choices[0].get("message") or {}
        return str(message.get("content", "")).strip()
    return ""


async def _ai_generate(prompt: str, settings: dict) -> str:
    provider = settings.get("provider", "anthropic")
    model = settings.get("model") or AI_PROVIDER_MODELS.get(provider, "")
    token = settings.get("token", "")
    base_url = _provider_base_url(provider, model, settings.get("base_url", ""))
    url, headers, payload, params = _provider_request(provider, token, model, prompt, base_url)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json=payload,
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=60),
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    return "# AI Error: HTTP {} {}".format(resp.status, body[:200])
                data = await resp.json()
                text = _extract_ai_text(provider, data)
                if not text:
                    return "# AI Error: empty provider response"
                return _sanitize_ai_output(text)
    except Exception as e:
        return "# AI Error: {}".format(e)


@loader.tds
class RedConstructor(loader.Module):
    """Interactive IDE for Heroku Userbot modules directly in Telegram."""

    strings = {
        "name": "RedConstructor",
        "welcome": (
            "🏗 <b>RedConstructor</b>\n\n"
            "Build modules for Heroku Userbot directly in Telegram.\n\n"
            "📦 <b>Projects:</b> {count}\n"
            "Choose an action:"
        ),
        "no_projects": "📭 No projects yet. Create the first one.",
        "project_panel": (
            "🗂 <b>{name}</b>\n\n"
            "🆔 ID: <code>{pid}</code>\n"
            "🏷 Class: <code>{class_name}</code>\n"
            "🛠 Commands: <code>{cmds}</code>\n"
            "👁 Watchers: <code>{watchers}</code>\n"
            "🔄 Loops: <code>{loops}</code>\n"
            "⚙️ Configs: <code>{cfgs}</code>\n"
            "🌍 Languages: <code>{langs}/{total_langs}</code>\n"
            "🕓 Updated: <code>{updated}</code>\n"
            "🕓 Created: <code>{created}</code>\n\n"
            "Open the section you need:"
        ),
        "module_panel": (
            "🧱 <b>Module</b>\n\n"
            "🗂 Name: <code>{name}</code>\n"
            "🏷 Class: <code>{class_name}</code>\n"
            "🆔 ID: <code>{pid}</code>\n"
            "🕓 Updated: <code>{updated}</code>\n"
            "👤 Developer: {developer}\n"
            "📚 License: {license}\n"
            "💬 Version: {version}\n"
            "🖼 Banner: {banner}\n"
            "🖼 Pic: {pic}\n"
            "📥 Min Heroku: {heroku_min}\n"
            "📦 Dependencies: {dependencies}\n\n"
            "{descs}"
        ),
        "module_section_hint": "Edit name, descriptions, metadata and dependencies here.",
        "commands_panel": "🛠 <b>Commands</b> · total: <code>{count}</code>",
        "command_section_hint": "Each command has docs, tags, body mode and quick templates.",
        "watchers_panel": "👁 <b>Watchers</b> · total: <code>{count}</code>",
        "watcher_section_hint": "Watchers handle events. Set body code and toggle Heroku watcher tags here.",
        "loops_panel": "🔄 <b>Loops</b> · total: <code>{count}</code>",
        "loop_section_hint": "Loops run in background with a fixed interval in seconds.",
        "configs_panel": "⚙️ <b>Configs</b> · total: <code>{count}</code>",
        "config_section_hint": "Create validator-aware configs. Choice and MultiChoice are built step by step.",
        "command_panel": (
            "🧩 <b>Command <code>.{cmd}</code></b>\n\n"
            "{docs}\n\n"
            "🐍 Body:\n<code>{body}</code>"
        ),
        "watcher_panel": (
            "👁 <b>Watcher <code>{name}</code></b>\n\n"
            "🏷 Tags: {tags}\n\n"
            "🐍 Body:\n<code>{body}</code>"
        ),
        "loop_panel": (
            "🔄 <b>Loop <code>{name}</code></b>\n\n"
            "⏱ Interval: <code>{interval}</code> sec\n\n"
            "🐍 Body:\n<code>{body}</code>"
        ),
        "config_panel": (
            "⚙️ <b>Config <code>{key}</code></b>\n\n"
            "🧪 Validator: <code>{validator}</code>\n"
            "📦 Default: <code>{default}</code>\n"
            "🧷 Validator args: {validator_args}"
        ),
        "command_list_line": "• <code>.{cmd}</code> — {doc}\n  <code>{body}</code>",
        "watcher_list_line": "• <code>{name}</code> — tags: {tags}\n  <code>{body}</code>",
        "loop_list_line": "• <code>{name}</code> — every <code>{interval}</code>s\n  <code>{body}</code>",
        "config_list_line": "• <code>{key}</code> — <code>{validator}</code> — default: <code>{default}</code>",
        "empty_commands": "No commands yet. Add the first one from this tab.",
        "empty_watchers": "No watchers yet. Add the first one from this tab.",
        "empty_loops": "No loops yet. Add the first one from this tab.",
        "empty_configs": "No configs yet. Add the first one from this tab.",
        "empty_dependencies": "none",
        "empty_tags": "none",
        "meta_empty": "<i>not set</i>",
        "unknown_name": "Unknown",
        "unknown_class": "UnknownMod",
        "missing_possible_values": "missing possible_values",
        "missing_regex": "missing regex",
        "template_payload_invalid": "template payload is invalid",
        "empty_code": "empty code",
        "project_btn_line": "📂 {name}",
        "command_btn_line": "🧩 .{cmd}",
        "watcher_btn_line": "👁 {name}",
        "loop_btn_line": "🔄 {name}",
        "config_btn_line": "⚙️ {key}",
        "lang_btn_required": "✅ {lang} *",
        "lang_btn_ready": "✅ {lang}",
        "lang_btn_empty": "➕ {lang}",
        "ask_name": "✏️ Enter the <b>module name</b> (for example <code>Weather</code>; <code>Mod</code> is added to the class automatically):",
        "ask_desc_en": "✏️ Enter the <b>module description in English</b>:",
        "ask_desc_ru": "✏️ Enter the <b>module description in Russian</b>:",
        "ask_module_developer": "✏️ Enter the <b>developer</b> value for <code># meta developer</code>:",
        "ask_module_version": "✏️ Enter the <b>module version</b> in numeric format, for example <code>1.0</code>, <code>1.0.0</code> or <code>67.228.1488.99</code>:",
        "ask_module_banner": "✏️ Enter the <b>banner</b> path or URL for <code># meta banner</code>. Allowed image extensions: <code>.png</code>, <code>.jpg</code>, <code>.jpeg</code>, <code>.gif</code>, <code>.webp</code>, <code>.bmp</code>, <code>.avif</code>, <code>.svg</code>:",
        "ask_module_pic": "✏️ Enter the <b>picture</b> path or URL for <code># meta pic</code>. Allowed image extensions: <code>.png</code>, <code>.jpg</code>, <code>.jpeg</code>, <code>.gif</code>, <code>.webp</code>, <code>.bmp</code>, <code>.avif</code>, <code>.svg</code>:",
        "ask_module_heroku_min": "✏️ Enter the <b>minimum Heroku version</b> for <code># scope heroku_min</code>, for example <code>2.0.0</code>:",
        "ask_cmd_name": "✏️ Enter the <b>command name</b> (lowercase latin, without a dot, for example <code>ping</code>):",
        "ask_watcher_name": "✏️ Enter the <b>watcher name</b> (lowercase latin, for example <code>audit</code>):",
        "ask_loop_name": "✏️ Enter the <b>loop name</b> (lowercase latin, for example <code>cleanup</code>):",
        "ask_cmd_doc_en": "✏️ Enter the <b>command description in English</b>:",
        "ask_cmd_doc_ru": "✏️ Enter the <b>command description in Russian</b>:",
        "ask_cmd_doc_lang": "✏️ Enter the <b>command description</b> for language <b>{lang}</b>:",
        "ask_cmd_ai_prompt": (
            "🤖 Describe in detail what command <code>{cmd}</code> should do.\n"
            "Mention data source, edge cases, exact reply logic and desired formatting.\n"
            "Formatting is always Telegram HTML, never Markdown.\n"
            "Example: <i>Take the replied text, convert it to upper case and answer with a HTML-formatted result</i>"
        ),
        "ask_cmd_body": (
            "✍️ Enter the <b>Python body</b> for command <code>{cmd}</code>.\n"
            "Only the body is needed, without <code>def</code> and imports."
        ),
        "ask_watcher_ai_prompt": (
            "🤖 Describe in detail what watcher <code>{name}</code> should do.\n"
            "Mention which events it reacts to, edge cases and safe reply behavior.\n"
            "Watcher code must work without <code>utils.get_args_raw</code>."
        ),
        "ask_watcher_body": (
            "✍️ Enter the <b>Python body</b> for watcher <code>{name}</code>.\n"
            "Only the body is needed, without <code>def</code> and imports."
        ),
        "ask_loop_ai_prompt": (
            "🤖 Describe in detail what background loop <code>{name}</code> should do.\n"
            "Remember that loops do not receive a message object."
        ),
        "ask_loop_body": (
            "✍️ Enter the <b>Python body</b> for loop <code>{name}</code>.\n"
            "Only the body is needed, without <code>def</code> and imports."
        ),
        "ask_loop_interval": "⏱ Enter the interval in <b>seconds</b> for loop <code>{name}</code>:",
        "ask_cfg_key": "✏️ Enter the <b>config key</b> (for example <code>api_key</code>):",
        "ask_cfg_default": "✏️ Enter the <b>default value</b> or leave it empty:",
        "ask_cfg_validator": "⚙️ Choose the <b>validator</b> for config <code>{key}</code>:",
        "ask_cfg_choice_values": (
            "✏️ Enter allowed values for <b>{validator}</b>.\n"
            "Separate them with commas or new lines.\n"
            "Example: <code>prod, dev, test</code>"
        ),
        "ask_cfg_regex": "✏️ Enter regex for config <code>{key}</code> validator <b>RegExp</b>:",
        "ask_cfg_boolean_default": "⚙️ Choose the default value for boolean config <code>{key}</code>:",
        "ask_cfg_choice_default": "⚙️ Choose the default value for config <code>{key}</code>:",
        "ask_cfg_multichoice_default": (
            "⚙️ Choose the default values for config <code>{key}</code>.\n"
            "Tap items to toggle them, then save."
        ),
        "selected_values_line": "Selected: {values}",
        "none_selected": "none",
        "validator_default_invalid": "❌ Default value does not match validator <code>{validator}</code>.",
        "invalid_validator_meta": "❌ Validator <code>{validator}</code> is configured incorrectly. Fill its required parameters.",
        "ask_lang_cls": "✏️ Enter the <b>module description</b> for language <b>{lang}</b>:",
        "lang_panel": (
            "🌍 <b>Languages</b>\n\n"
            "Class/module translations live here only.\n"
            "Commands and configs are edited in their own sections."
        ),
        "dependencies_panel": (
            "📦 <b>Dependencies</b>\n\n"
            "Current packages: {deps}\n\n"
            "Use one package per line or separate them with commas."
        ),
        "license_panel": (
            "📚 <b>License</b>\n\n"
            "Current value: {current}\n\n"
            "Choose a license:"
        ),
        "module_lang_line": "{status} <b>{lang}</b>: <i>{value}</i>",
        "empty_lang_value": "not filled",
        "body_mode": (
            "🧩 <b>Command <code>.{cmd}</code></b>\n\n"
            "Choose how to add the body:"
        ),
        "watcher_body_mode": (
            "👁 <b>Watcher <code>{name}</code></b>\n\n"
            "Choose how to add the body:"
        ),
        "loop_body_mode": (
            "🔄 <b>Loop <code>{name}</code></b>\n\n"
            "Interval: <code>{interval}</code> sec\n"
            "Choose how to add the body:"
        ),
        "watcher_tags_panel": (
            "🏷 <b>Watcher tags for <code>{name}</code></b>\n\n"
            "Enabled tags: {tags}\n\n"
            "Tap a tag to toggle it."
        ),
        "template_panel": (
            "🧰 <b>No-code command builder</b>\n\n"
            "Choose a template for <code>.{cmd}</code>."
        ),
        "template_saved": "✅ Template applied to <code>.{cmd}</code>.",
        "ask_template_fixed_text": "✏️ Enter the text that command <code>.{cmd}</code> should send:",
        "ask_template_random_values": (
            "✏️ Enter random choice values for <code>.{cmd}</code>.\n"
            "Separate them with commas or new lines."
        ),
        "ask_template_db_save": (
            "✏️ Enter the DB key on the first line and the value on the remaining lines for <code>.{cmd}</code>."
        ),
        "ask_template_db_load": "✏️ Enter the DB key that command <code>.{cmd}</code> should load:",
        "ai_generating": "🤖 <i>AI is generating code for <code>{cmd}</code>...</i>",
        "ai_done": "✅ AI generated the body for <code>{cmd}</code>.",
        "ai_error": "❌ AI error: <code>{err}</code>",
        "ai_not_configured": (
            "❌ AI provider is not ready.\n"
            "Open <code>.cfg RedConstructor</code> and set provider, token and model first."
        ),
        "ai_invalid_provider": "❌ Unsupported AI provider <code>{provider}</code>.",
        "ai_token_missing": "❌ AI token is empty. Set it in <code>.cfg RedConstructor</code>.",
        "ai_model_missing": "❌ AI model is empty. Set it in <code>.cfg RedConstructor</code>.",
        "compile_start": "🔨 Compiling module <code>{name}</code>...",
        "compile_error": "❌ <b>Compilation error:</b>\n<code>{err}</code>",
        "compiled_caption": (
            "✅ <b>RedConstructor</b>\n\n"
            "📦 Module: <code>{name}</code>\n"
            "🛠 Commands: <code>{cmds}</code>\n"
            "👁 Watchers: <code>{watchers}</code>\n"
            "🔄 Loops: <code>{loops}</code>\n"
            "⚙️ Configs: <code>{cfgs}</code>\n\n"
            "Install with: <code>.lm</code> replying to this file."
        ),
        "project_created": "✅ Project <code>{name}</code> created.",
        "project_deleted": "🗑 Project <code>{name}</code> deleted.",
        "cmd_added": "✅ Command <code>.{cmd}</code> added.",
        "cmd_updated": "✅ Command <code>.{cmd}</code> updated.",
        "cmd_deleted": "🗑 Command <code>.{cmd}</code> deleted.",
        "watcher_added": "✅ Watcher <code>{name}</code> added.",
        "watcher_updated": "✅ Watcher <code>{name}</code> updated.",
        "watcher_deleted": "🗑 Watcher <code>{name}</code> deleted.",
        "loop_added": "✅ Loop <code>{name}</code> added.",
        "loop_updated": "✅ Loop <code>{name}</code> updated.",
        "loop_deleted": "🗑 Loop <code>{name}</code> deleted.",
        "cfg_added": "✅ Config <code>{key}</code> added.",
        "cfg_updated": "✅ Config <code>{key}</code> updated.",
        "cfg_deleted": "🗑 Config <code>{key}</code> deleted.",
        "dependencies_saved": "✅ Dependencies updated.",
        "lang_saved": "✅ Translation for <b>{lang}</b> saved.",
        "meta_saved": "✅ Module data updated.",
        "invalid_module_name": "❌ Invalid module name. Use latin letters, digits, spaces or underscore. <code>Mod</code> is added automatically.",
        "invalid_module_version": "❌ Invalid version. Use digits separated by dots, for example <code>1.0</code> or <code>2.0.0</code>.",
        "invalid_module_image": "❌ Invalid image reference. Use a path or URL ending with <code>.png</code>, <code>.jpg</code>, <code>.jpeg</code>, <code>.gif</code>, <code>.webp</code>, <code>.bmp</code>, <code>.avif</code> or <code>.svg</code>.",
        "invalid_module_heroku_min": "❌ Invalid minimum Heroku version. Use digits separated by dots, for example <code>2.0.0</code>.",
        "invalid_command_name": "❌ Invalid command name. Use lowercase latin letters, digits and underscore.",
        "invalid_watcher_name": "❌ Invalid watcher name. Use lowercase latin letters, digits and underscore.",
        "invalid_loop_name": "❌ Invalid loop name. Use lowercase latin letters, digits and underscore.",
        "invalid_config_name": "❌ Invalid config key. Use lowercase latin letters, digits and underscore.",
        "invalid_code": "❌ Invalid Python body:\n<code>{err}</code>",
        "invalid_loop_interval": "❌ Interval must be a positive integer.",
        "invalid_lang_code": "❌ Invalid language code. Use lowercase latin letters, digits or underscore, for example <code>es</code> or <code>pt_br</code>.",
        "duplicate_project_name": "❌ A project with module name <code>{name}</code> already exists.",
        "duplicate_command_name": "❌ Command <code>.{cmd}</code> already exists in this project.",
        "duplicate_watcher_name": "❌ Watcher <code>{name}</code> already exists in this project.",
        "duplicate_loop_name": "❌ Loop <code>{name}</code> already exists in this project.",
        "duplicate_handler_name": "❌ Name <code>{name}</code> is already used by another command, watcher or loop.",
        "duplicate_config_name": "❌ Config <code>{key}</code> already exists in this project.",
        "core_module_conflict": "❌ Module name <code>{name}</code> conflicts with a core Heroku module.",
        "core_command_conflict": "❌ Command <code>.{cmd}</code> conflicts with a core Heroku command or alias.",
        "core_watcher_conflict": "❌ Watcher name <code>{name}</code> conflicts with a reserved or core Heroku method.",
        "core_loop_conflict": "❌ Loop name <code>{name}</code> conflicts with a reserved or core Heroku method.",
        "project_not_found": "❌ Project not found.",
        "list_title": "📋 <b>Projects</b>",
        "delete_confirm": "🗑 Delete project <b>{name}</b>? This cannot be undone.",
        "cancel": "❌ Cancelled.",
        "saved_empty": "✅ Saved empty value.",
        "enter_btn": "✏️ Enter text",
        "skip_btn": "⏭ Skip",
        "cancel_btn": "❌ Cancel",
        "back_btn": "◀️ Back",
        "create_btn": "➕ Create project",
        "list_btn": "📂 Project list",
        "overview_btn": "🧾 Overview",
        "module_btn": "🧱 Module",
        "commands_btn": "🛠 Commands",
        "watchers_btn": "👁 Watchers",
        "loops_btn": "🔄 Loops",
        "configs_btn": "⚙️ Configs",
        "add_cmd_btn": "➕ Add command",
        "add_watcher_btn": "➕ Add watcher",
        "add_loop_btn": "➕ Add loop",
        "add_cfg_btn": "⚙️ Add config",
        "languages_btn": "🌍 Languages",
        "add_lang_btn": "➕ Add lang",
        "compile_btn": "🚀 Compile",
        "delete_btn": "🗑 Delete",
        "edit_name_btn": "🏷 Edit name",
        "edit_developer_btn": "👤 Developer",
        "edit_license_btn": "📚 License",
        "edit_version_btn": "💬 Version",
        "edit_banner_btn": "🖼 Banner",
        "edit_pic_btn": "🖼 Pic",
        "edit_scope_btn": "📥 Heroku min",
        "dependencies_btn": "📦 Dependencies",
        "edit_dependencies_btn": "✏️ Edit dependencies",
        "edit_desc_en_btn": "🇬🇧 Edit EN",
        "edit_desc_ru_btn": "🇷🇺 Edit RU",
        "edit_langs_btn": "🌍 Other langs",
        "edit_doc_en_btn": "🇬🇧 Edit doc",
        "edit_doc_ru_btn": "🇷🇺 Edit doc",
        "edit_docs_btn": "🌍 Docs",
        "tags_btn": "🏷 Tags",
        "interval_btn": "⏱ Interval",
        "edit_body_btn": "✍️ Edit body",
        "edit_default_btn": "📦 Edit default",
        "edit_validator_btn": "🧪 Edit validator",
        "manual_body_btn": "✍️ Manual body",
        "ai_body_btn": "🤖 AI body",
        "template_body_btn": "🧰 Template",
        "stub_body_btn": "📄 Use pass",
        "true_btn": "✅ True",
        "false_btn": "❌ False",
        "empty_btn": "🫙 Empty",
        "save_btn": "💾 Save",
        "clear_btn": "🧹 Clear",
        "clear_meta_btn": "🧹 Clear value",
        "done_btn": "✅ Done",
        "ask_lang_code": "✏️ Enter the <b>language code</b> to add to this project, for example <code>es</code> or <code>pt_br</code>:",
        "ask_dependencies": (
            "✏️ Enter package names for <b>__dependencies__</b>.\n"
            "Use commas or new lines, for example <code>aiohttp, bs4</code>."
        ),
        "placeholder_watcher_name": "audit",
        "placeholder_loop_name": "cleanup",
        "placeholder_module_developer": "coddrago",
        "placeholder_module_version": "1.0.0",
        "placeholder_module_banner": "https://example.com/banner.png",
        "placeholder_module_pic": "https://example.com/pic.jpg",
        "placeholder_module_heroku_min": "2.0.0",
        "purge_done": (
            "🧹 <b>Purge complete.</b>\n"
            "Removed: <code>{removed}</code> corrupted project(s).\n"
            "Remaining: <code>{remaining}</code> valid project(s)."
        ),
        "help_text": (
            "🏠 <b>RedConstructor</b>\n\n"
            "<b>Commands</b>\n"
            "• <code>.rcbm</code> — open the panel\n"
            "• <code>.rchp</code> — show help\n"
            "• <code>.rcls</code> — list projects\n"
            "• <code>.rcpg</code> — purge broken projects\n\n"
            "<b>Features</b>\n"
            "• multi-project storage in DB\n"
            "• module, commands, watchers, loops, configs and languages tabs\n"
            "• module metadata: license, developer, version, banner, pic and heroku_min\n"
            "• command creation plus editing of docs and bodies\n"
            "• no-code command templates for common actions\n"
            "• config creation plus editing of default values and validators\n"
            "• validator-aware config flow for Choice and MultiChoice\n"
            "• translations for all supported IDE languages\n"
            "• protection from overwriting core Heroku modules and commands\n"
            "• AI provider, token and model via <code>.cfg RedConstructor</code>\n"
            "• export of ready <code>.py</code> module files"
        ),
    }

    strings_ru = {
        "welcome": (
            "🏗 <b>RedConstructor</b>\n\n"
            "Создавай модули для Heroku Userbot прямо в Telegram.\n\n"
            "📦 <b>Проектов:</b> {count}\n"
            "Выбери действие:"
        ),
        "no_projects": "📭 Проектов пока нет. Создай первый.",
        "project_panel": (
            "🗂 <b>{name}</b>\n\n"
            "🆔 ID: <code>{pid}</code>\n"
            "🏷 Класс: <code>{class_name}</code>\n"
            "🛠 Команд: <code>{cmds}</code>\n"
            "👁 Watchers: <code>{watchers}</code>\n"
            "🔄 Loops: <code>{loops}</code>\n"
            "⚙️ Конфигов: <code>{cfgs}</code>\n"
            "🌍 Языков: <code>{langs}/{total_langs}</code>\n"
            "🕓 Обновлён: <code>{updated}</code>\n"
            "🕓 Создан: <code>{created}</code>\n\n"
            "Открой нужный раздел:"
        ),
        "module_panel": (
            "🧱 <b>Модуль</b>\n\n"
            "🗂 Имя: <code>{name}</code>\n"
            "🏷 Класс: <code>{class_name}</code>\n"
            "🆔 ID: <code>{pid}</code>\n"
            "🕓 Обновлён: <code>{updated}</code>\n"
            "👤 Разработчик: {developer}\n"
            "📚 Лицензия: {license}\n"
            "💬 Версия: {version}\n"
            "🖼 Баннер: {banner}\n"
            "🖼 Картинка: {pic}\n"
            "📥 Min Heroku: {heroku_min}\n"
            "📦 Зависимости: {dependencies}\n\n"
            "{descs}"
        ),
        "module_section_hint": "Здесь редактируются имя, описания, метаданные и зависимости модуля.",
        "commands_panel": "🛠 <b>Команды</b> · всего: <code>{count}</code>",
        "command_section_hint": "У каждой команды есть описания, режимы тела, теги и быстрые шаблоны.",
        "watchers_panel": "👁 <b>Watchers</b> · всего: <code>{count}</code>",
        "watcher_section_hint": "Watcher'ы обрабатывают события. Здесь редактируются тело и теги watcher'а.",
        "loops_panel": "🔄 <b>Loops</b> · всего: <code>{count}</code>",
        "loop_section_hint": "Loop'ы выполняются в фоне с фиксированным интервалом в секундах.",
        "configs_panel": "⚙️ <b>Конфиги</b> · всего: <code>{count}</code>",
        "config_section_hint": "Создавай конфиги пошагово. Для Choice и MultiChoice сначала задаются варианты, потом дефолт.",
        "command_panel": (
            "🧩 <b>Команда <code>.{cmd}</code></b>\n\n"
            "{docs}\n\n"
            "🐍 Тело:\n<code>{body}</code>"
        ),
        "watcher_panel": (
            "👁 <b>Watcher <code>{name}</code></b>\n\n"
            "🏷 Теги: {tags}\n\n"
            "🐍 Тело:\n<code>{body}</code>"
        ),
        "loop_panel": (
            "🔄 <b>Loop <code>{name}</code></b>\n\n"
            "⏱ Интервал: <code>{interval}</code> сек\n\n"
            "🐍 Тело:\n<code>{body}</code>"
        ),
        "config_panel": (
            "⚙️ <b>Конфиг <code>{key}</code></b>\n\n"
            "🧪 Валидатор: <code>{validator}</code>\n"
            "📦 Дефолт: <code>{default}</code>\n"
            "🧷 Параметры валидатора: {validator_args}"
        ),
        "command_list_line": "• <code>.{cmd}</code> — {doc}\n  <code>{body}</code>",
        "watcher_list_line": "• <code>{name}</code> — теги: {tags}\n  <code>{body}</code>",
        "loop_list_line": "• <code>{name}</code> — каждые <code>{interval}</code>с\n  <code>{body}</code>",
        "config_list_line": "• <code>{key}</code> — <code>{validator}</code> — дефолт: <code>{default}</code>",
        "empty_commands": "Команд пока нет. Добавь первую из этой вкладки.",
        "empty_watchers": "Watcher'ов пока нет. Добавь первый из этой вкладки.",
        "empty_loops": "Loop'ов пока нет. Добавь первый из этой вкладки.",
        "empty_configs": "Конфигов пока нет. Добавь первый из этой вкладки.",
        "empty_dependencies": "нет",
        "empty_tags": "нет",
        "meta_empty": "<i>не задано</i>",
        "unknown_name": "Неизвестно",
        "unknown_class": "UnknownMod",
        "missing_possible_values": "не заданы possible_values",
        "missing_regex": "не задан regex",
        "template_payload_invalid": "невалидные данные шаблона",
        "empty_code": "пустой код",
        "project_btn_line": "📂 {name}",
        "command_btn_line": "🧩 .{cmd}",
        "watcher_btn_line": "👁 {name}",
        "loop_btn_line": "🔄 {name}",
        "config_btn_line": "⚙️ {key}",
        "lang_btn_required": "✅ {lang} *",
        "lang_btn_ready": "✅ {lang}",
        "lang_btn_empty": "➕ {lang}",
        "ask_name": "✏️ Введи <b>название модуля</b> (например <code>Weather</code>; суффикс <code>Mod</code> для класса добавится автоматически):",
        "ask_desc_en": "✏️ Введи <b>описание модуля на английском</b>:",
        "ask_desc_ru": "✏️ Введи <b>описание модуля на русском</b>:",
        "ask_module_developer": "✏️ Введи <b>разработчика</b> для <code># meta developer</code>:",
        "ask_module_version": "✏️ Введи <b>версию модуля</b> в формате чисел через точку, например <code>1.0</code>, <code>1.0.0</code> или <code>67.228.1488.99</code>:",
        "ask_module_banner": "✏️ Введи <b>путь или ссылку на баннер</b> для <code># meta banner</code>. Разрешены только изображения: <code>.png</code>, <code>.jpg</code>, <code>.jpeg</code>, <code>.gif</code>, <code>.webp</code>, <code>.bmp</code>, <code>.avif</code>, <code>.svg</code>:",
        "ask_module_pic": "✏️ Введи <b>путь или ссылку на картинку</b> для <code># meta pic</code>. Разрешены только изображения: <code>.png</code>, <code>.jpg</code>, <code>.jpeg</code>, <code>.gif</code>, <code>.webp</code>, <code>.bmp</code>, <code>.avif</code>, <code>.svg</code>:",
        "ask_module_heroku_min": "✏️ Введи <b>минимальную версию Heroku</b> для <code># scope heroku_min</code>, например <code>2.0.0</code>:",
        "ask_cmd_name": "✏️ Введи <b>название команды</b> (строчными, без точки, например <code>ping</code>):",
        "ask_watcher_name": "✏️ Введи <b>имя watcher'а</b> (строчными, например <code>audit</code>):",
        "ask_loop_name": "✏️ Введи <b>имя loop'а</b> (строчными, например <code>cleanup</code>):",
        "ask_cmd_doc_en": "✏️ Введи <b>описание команды на английском</b>:",
        "ask_cmd_doc_ru": "✏️ Введи <b>описание команды на русском</b>:",
        "ask_cmd_doc_lang": "✏️ Введи <b>описание команды</b> для языка <b>{lang}</b>:",
        "ask_cmd_ai_prompt": (
            "🤖 Максимально подробно опиши, что должна делать команда <code>{cmd}</code>.\n"
            "Укажи источник данных, крайние случаи, точную логику ответа и желаемое форматирование.\n"
            "Форматирование всегда Telegram HTML, не Markdown.\n"
            "Пример: <i>Берёт текст из реплая, переводит в верхний регистр и отвечает HTML-оформленным результатом</i>"
        ),
        "ask_cmd_body": (
            "✍️ Введи <b>Python-тело</b> команды <code>{cmd}</code>.\n"
            "Нужно только тело, без <code>def</code> и импортов."
        ),
        "ask_watcher_ai_prompt": (
            "🤖 Подробно опиши, что должен делать watcher <code>{name}</code>.\n"
            "Укажи, на какие события он реагирует, крайние случаи и безопасное поведение.\n"
            "В watcher'е нельзя использовать <code>utils.get_args_raw</code>."
        ),
        "ask_watcher_body": (
            "✍️ Введи <b>Python-тело</b> watcher'а <code>{name}</code>.\n"
            "Нужно только тело, без <code>def</code> и импортов."
        ),
        "ask_loop_ai_prompt": (
            "🤖 Подробно опиши, что должен делать фоновый loop <code>{name}</code>.\n"
            "Помни, что у loop'а нет объекта message."
        ),
        "ask_loop_body": (
            "✍️ Введи <b>Python-тело</b> loop'а <code>{name}</code>.\n"
            "Нужно только тело, без <code>def</code> и импортов."
        ),
        "ask_loop_interval": "⏱ Введи интервал в <b>секундах</b> для loop'а <code>{name}</code>:",
        "ask_cfg_key": "✏️ Введи <b>ключ конфига</b> (например <code>api_key</code>):",
        "ask_cfg_default": "✏️ Введи <b>значение по умолчанию</b> или оставь пустым:",
        "ask_cfg_validator": "⚙️ Выбери <b>валидатор</b> для конфига <code>{key}</code>:",
        "ask_cfg_choice_values": (
            "✏️ Введи допустимые значения для <b>{validator}</b>.\n"
            "Разделяй запятыми или новыми строками.\n"
            "Пример: <code>prod, dev, test</code>"
        ),
        "ask_cfg_regex": "✏️ Введи regex для валидатора <b>RegExp</b> конфига <code>{key}</code>:",
        "ask_cfg_boolean_default": "⚙️ Выбери дефолтное значение для boolean-конфига <code>{key}</code>:",
        "ask_cfg_choice_default": "⚙️ Выбери дефолтное значение для конфига <code>{key}</code>:",
        "ask_cfg_multichoice_default": (
            "⚙️ Выбери дефолтные значения для конфига <code>{key}</code>.\n"
            "Нажимай по вариантам, чтобы переключать их, затем сохрани."
        ),
        "selected_values_line": "Выбрано: {values}",
        "none_selected": "ничего",
        "validator_default_invalid": "❌ Значение по умолчанию не подходит под валидатор <code>{validator}</code>.",
        "invalid_validator_meta": "❌ Валидатор <code>{validator}</code> настроен криво. Заполни обязательные параметры.",
        "ask_lang_cls": "✏️ Введи <b>описание модуля</b> для языка <b>{lang}</b>:",
        "lang_panel": (
            "🌍 <b>Языки</b>\n\n"
            "Здесь редактируются только переводы описания модуля.\n"
            "Команды и конфиги живут в своих отдельных разделах."
        ),
        "dependencies_panel": (
            "📦 <b>Зависимости</b>\n\n"
            "Текущие пакеты: {deps}\n\n"
            "Используй по одному пакету на строку или разделяй их запятыми."
        ),
        "license_panel": (
            "📚 <b>Лицензия</b>\n\n"
            "Текущее значение: {current}\n\n"
            "Выбери лицензию:"
        ),
        "module_lang_line": "{status} <b>{lang}</b>: <i>{value}</i>",
        "empty_lang_value": "не заполнено",
        "body_mode": "🧩 <b>Команда <code>.{cmd}</code></b>\n\nВыбери способ добавления тела:",
        "watcher_body_mode": "👁 <b>Watcher <code>{name}</code></b>\n\nВыбери способ добавления тела:",
        "loop_body_mode": (
            "🔄 <b>Loop <code>{name}</code></b>\n\n"
            "Интервал: <code>{interval}</code> сек\n"
            "Выбери способ добавления тела:"
        ),
        "watcher_tags_panel": (
            "🏷 <b>Теги watcher'а <code>{name}</code></b>\n\n"
            "Включено: {tags}\n\n"
            "Нажми на тег, чтобы переключить его."
        ),
        "template_panel": (
            "🧰 <b>Командный конструктор без кода</b>\n\n"
            "Выбери шаблон для <code>.{cmd}</code>."
        ),
        "template_saved": "✅ Шаблон применён к <code>.{cmd}</code>.",
        "ask_template_fixed_text": "✏️ Введи текст, который должна отправлять команда <code>.{cmd}</code>:",
        "ask_template_random_values": (
            "✏️ Введи варианты для случайного выбора у <code>.{cmd}</code>.\n"
            "Разделяй их запятыми или новыми строками."
        ),
        "ask_template_db_save": (
            "✏️ Введи ключ БД в первой строке и значение в остальных строках для <code>.{cmd}</code>."
        ),
        "ask_template_db_load": "✏️ Введи ключ БД, который команда <code>.{cmd}</code> должна загрузить:",
        "ai_generating": "🤖 <i>AI генерирует код для <code>{cmd}</code>...</i>",
        "ai_done": "✅ AI сгенерировал тело для <code>{cmd}</code>.",
        "ai_error": "❌ Ошибка AI: <code>{err}</code>",
        "ai_not_configured": (
            "❌ AI не настроен.\n"
            "Открой <code>.cfg RedConstructor</code> и укажи провайдера, токен и модель."
        ),
        "ai_invalid_provider": "❌ Провайдер AI <code>{provider}</code> не поддерживается.",
        "ai_token_missing": "❌ Токен AI пустой. Заполни его в <code>.cfg RedConstructor</code>.",
        "ai_model_missing": "❌ Модель AI пустая. Заполни её в <code>.cfg RedConstructor</code>.",
        "compile_start": "🔨 Собираю модуль <code>{name}</code>...",
        "compile_error": "❌ <b>Ошибка сборки:</b>\n<code>{err}</code>",
        "compiled_caption": (
            "✅ <b>RedConstructor</b>\n\n"
            "📦 Модуль: <code>{name}</code>\n"
            "🛠 Команд: <code>{cmds}</code>\n"
            "👁 Watchers: <code>{watchers}</code>\n"
            "🔄 Loops: <code>{loops}</code>\n"
            "⚙️ Конфигов: <code>{cfgs}</code>\n\n"
            "Установи через <code>.lm</code> реплаем на этот файл."
        ),
        "project_created": "✅ Проект <code>{name}</code> создан.",
        "project_deleted": "🗑 Проект <code>{name}</code> удалён.",
        "cmd_added": "✅ Команда <code>.{cmd}</code> добавлена.",
        "cmd_updated": "✅ Команда <code>.{cmd}</code> обновлена.",
        "cmd_deleted": "🗑 Команда <code>.{cmd}</code> удалена.",
        "watcher_added": "✅ Watcher <code>{name}</code> добавлен.",
        "watcher_updated": "✅ Watcher <code>{name}</code> обновлён.",
        "watcher_deleted": "🗑 Watcher <code>{name}</code> удалён.",
        "loop_added": "✅ Loop <code>{name}</code> добавлен.",
        "loop_updated": "✅ Loop <code>{name}</code> обновлён.",
        "loop_deleted": "🗑 Loop <code>{name}</code> удалён.",
        "cfg_added": "✅ Конфиг <code>{key}</code> добавлен.",
        "cfg_updated": "✅ Конфиг <code>{key}</code> обновлён.",
        "cfg_deleted": "🗑 Конфиг <code>{key}</code> удалён.",
        "dependencies_saved": "✅ Зависимости обновлены.",
        "lang_saved": "✅ Перевод для <b>{lang}</b> сохранён.",
        "meta_saved": "✅ Данные модуля обновлены.",
        "invalid_module_name": "❌ Неверное имя модуля. Используй латиницу, цифры, пробелы или подчёркивания. Суффикс <code>Mod</code> добавляется автоматически.",
        "invalid_module_version": "❌ Неверная версия. Используй числа, разделённые точками, например <code>1.0</code> или <code>2.0.0</code>.",
        "invalid_module_image": "❌ Неверная ссылка или путь к картинке. Нужен файл с расширением <code>.png</code>, <code>.jpg</code>, <code>.jpeg</code>, <code>.gif</code>, <code>.webp</code>, <code>.bmp</code>, <code>.avif</code> или <code>.svg</code>.",
        "invalid_module_heroku_min": "❌ Неверная минимальная версия Heroku. Используй числа, разделённые точками, например <code>2.0.0</code>.",
        "invalid_command_name": "❌ Неверное имя команды. Используй строчные латинские буквы, цифры и подчёркивание.",
        "invalid_watcher_name": "❌ Неверное имя watcher'а. Используй строчные латинские буквы, цифры и подчёркивание.",
        "invalid_loop_name": "❌ Неверное имя loop'а. Используй строчные латинские буквы, цифры и подчёркивание.",
        "invalid_config_name": "❌ Неверный ключ конфига. Используй строчные латинские буквы, цифры и подчёркивание.",
        "invalid_code": "❌ Невалидное Python-тело:\n<code>{err}</code>",
        "invalid_loop_interval": "❌ Интервал должен быть положительным целым числом.",
        "invalid_lang_code": "❌ Невалидный код языка. Используй строчные латинские буквы, цифры или подчёркивание, например <code>es</code> или <code>pt_br</code>.",
        "duplicate_project_name": "❌ Проект с модулем <code>{name}</code> уже существует.",
        "duplicate_command_name": "❌ Команда <code>.{cmd}</code> уже есть в проекте.",
        "duplicate_watcher_name": "❌ Watcher <code>{name}</code> уже есть в проекте.",
        "duplicate_loop_name": "❌ Loop <code>{name}</code> уже есть в проекте.",
        "duplicate_handler_name": "❌ Имя <code>{name}</code> уже занято другой командой, watcher'ом или loop'ом.",
        "duplicate_config_name": "❌ Конфиг <code>{key}</code> уже есть в проекте.",
        "core_module_conflict": "❌ Имя <code>{name}</code> конфликтует с core-модулем Heroku.",
        "core_command_conflict": "❌ Команда <code>.{cmd}</code> конфликтует с core-командой или alias Heroku.",
        "core_watcher_conflict": "❌ Имя watcher'а <code>{name}</code> конфликтует с зарезервированным или core-методом Heroku.",
        "core_loop_conflict": "❌ Имя loop'а <code>{name}</code> конфликтует с зарезервированным или core-методом Heroku.",
        "project_not_found": "❌ Проект не найден.",
        "list_title": "📋 <b>Проекты</b>",
        "delete_confirm": "🗑 Удалить проект <b>{name}</b>? Это действие необратимо.",
        "cancel": "❌ Отменено.",
        "saved_empty": "✅ Пустое значение сохранено.",
        "enter_btn": "✏️ Ввести текст",
        "skip_btn": "⏭ Пропустить",
        "cancel_btn": "❌ Отмена",
        "back_btn": "◀️ Назад",
        "create_btn": "➕ Создать проект",
        "list_btn": "📂 Список проектов",
        "overview_btn": "🧾 Обзор",
        "module_btn": "🧱 Модуль",
        "commands_btn": "🛠 Команды",
        "watchers_btn": "👁 Watchers",
        "loops_btn": "🔄 Loops",
        "configs_btn": "⚙️ Конфиги",
        "add_cmd_btn": "➕ Добавить команду",
        "add_watcher_btn": "➕ Добавить watcher",
        "add_loop_btn": "➕ Добавить loop",
        "add_cfg_btn": "⚙️ Добавить конфиг",
        "languages_btn": "🌍 Языки",
        "add_lang_btn": "➕ Язык",
        "compile_btn": "🚀 Скомпилировать",
        "delete_btn": "🗑 Удалить",
        "edit_name_btn": "🏷 Имя",
        "edit_developer_btn": "👤 Разраб",
        "edit_license_btn": "📚 Лицензия",
        "edit_version_btn": "💬 Версия",
        "edit_banner_btn": "🖼 Баннер",
        "edit_pic_btn": "🖼 Пикча",
        "edit_scope_btn": "📥 Heroku min",
        "dependencies_btn": "📦 Зависимости",
        "edit_dependencies_btn": "✏️ Изменить зависимости",
        "edit_desc_en_btn": "🇬🇧 EN",
        "edit_desc_ru_btn": "🇷🇺 RU",
        "edit_langs_btn": "🌍 Остальные",
        "edit_doc_en_btn": "🇬🇧 EN doc",
        "edit_doc_ru_btn": "🇷🇺 RU doc",
        "edit_docs_btn": "🌍 Описания",
        "tags_btn": "🏷 Теги",
        "interval_btn": "⏱ Интервал",
        "edit_body_btn": "✍️ Тело",
        "edit_default_btn": "📦 Дефолт",
        "edit_validator_btn": "🧪 Валидатор",
        "manual_body_btn": "✍️ Ввести тело",
        "ai_body_btn": "🤖 AI тело",
        "template_body_btn": "🧰 Шаблон",
        "stub_body_btn": "📄 Поставить pass",
        "true_btn": "✅ True",
        "false_btn": "❌ False",
        "empty_btn": "🫙 Пусто",
        "save_btn": "💾 Сохранить",
        "clear_btn": "🧹 Сбросить",
        "clear_meta_btn": "🧹 Очистить",
        "done_btn": "✅ Готово",
        "ask_lang_code": "✏️ Введи <b>код языка</b>, который нужно добавить в проект, например <code>es</code> или <code>pt_br</code>:",
        "ask_dependencies": (
            "✏️ Введи имена пакетов для <b>__dependencies__</b>.\n"
            "Можно через запятые или с новой строки, например <code>aiohttp, bs4</code>."
        ),
        "placeholder_watcher_name": "audit",
        "placeholder_loop_name": "cleanup",
        "placeholder_module_developer": "coddrago",
        "placeholder_module_version": "1.0.0",
        "placeholder_module_banner": "https://example.com/banner.png",
        "placeholder_module_pic": "https://example.com/pic.jpg",
        "placeholder_module_heroku_min": "2.0.0",
        "purge_done": (
            "🧹 <b>Очистка завершена.</b>\n"
            "Удалено: <code>{removed}</code> битых проект(ов).\n"
            "Осталось: <code>{remaining}</code> валидных проект(ов)."
        ),
        "help_text": (
            "🏠 <b>RedConstructor</b>\n\n"
            "<b>Команды</b>\n"
            "• <code>.rcbm</code> — открыть панель\n"
            "• <code>.rchp</code> — показать справку\n"
            "• <code>.rcls</code> — список проектов\n\n"
            "• <code>.rcpg</code> — очистить битые проекты\n\n"
            "<b>Возможности</b>\n"
            "• хранение нескольких проектов в БД\n"
            "• вкладки модуля, команд, watcher'ов, loop'ов, конфигов и языков\n"
            "• метаданные модуля: лицензия, разработчик, версия, banner, pic и heroku_min\n"
            "• создание и редактирование команд, их описаний и тел\n"
            "• шаблоны команд без ручного Python\n"
            "• создание и редактирование конфигов, дефолтов и валидаторов\n"
            "• пошаговый мастер Choice и MultiChoice\n"
            "• переводы IDE на все поддерживаемые языки\n"
            "• защита от перезаписи core-модулей и core-команд Heroku\n"
            "• AI-провайдер, токен и модель через <code>.cfg RedConstructor</code>\n"
            "• экспорт готовых <code>.py</code> модулей"
        ),
    }

    strings_de = {
        "welcome": "🏗 <b>RedConstructor</b>\n\nErstelle Heroku-Userbot-Module direkt in Telegram.\n\n📦 <b>Projekte:</b> {count}\nWähle eine Aktion:",
        "no_projects": "📭 Noch keine Projekte.",
        "project_created": "✅ Projekt <code>{name}</code> erstellt.",
        "project_deleted": "🗑 Projekt <code>{name}</code> gelöscht.",
        "cmd_added": "✅ Befehl <code>.{cmd}</code> hinzugefügt.",
        "cfg_added": "✅ Konfiguration <code>{key}</code> hinzugefügt.",
        "core_module_conflict": "❌ Der Modulname <code>{name}</code> kollidiert mit einem Heroku-Core-Modul.",
        "core_command_conflict": "❌ Der Befehl <code>.{cmd}</code> kollidiert mit einem Heroku-Core-Befehl oder Alias.",
        "enter_btn": "✏️ Text eingeben",
        "skip_btn": "⏭ Überspringen",
        "cancel_btn": "❌ Abbrechen",
        "back_btn": "◀️ Zurück",
        "create_btn": "➕ Projekt erstellen",
        "list_btn": "📂 Projekte",
        "add_cmd_btn": "➕ Befehl",
        "add_cfg_btn": "⚙️ Konfig",
        "languages_btn": "🌍 Sprachen",
        "compile_btn": "🚀 Kompilieren",
        "delete_btn": "🗑 Löschen",
        "manual_body_btn": "✍️ Manuell",
        "ai_body_btn": "🤖 KI-Body",
        "stub_body_btn": "📄 pass",
        "help_text": "🏗 <b>RedConstructor</b>\n\nMehrprojekt-IDE fur Heroku Userbot mit KI, manueller Code-Eingabe, Konfig-Builder, Sprachverwaltung und Schutz vor Kollisionen mit Core-Modulen/Core-Befehlen.",
    }

    strings_uk = {
        "welcome": "🏗 <b>RedConstructor</b>\n\nСтворюй модулі для Heroku Userbot прямо в Telegram.\n\n📦 <b>Проєктів:</b> {count}\nОбери дію:",
        "no_projects": "📭 Проєктів ще немає.",
        "project_created": "✅ Проєкт <code>{name}</code> створено.",
        "project_deleted": "🗑 Проєкт <code>{name}</code> видалено.",
        "cmd_added": "✅ Команду <code>.{cmd}</code> додано.",
        "cfg_added": "✅ Конфіг <code>{key}</code> додано.",
        "core_module_conflict": "❌ Назва <code>{name}</code> конфліктує з core-модулем Heroku.",
        "core_command_conflict": "❌ Команда <code>.{cmd}</code> конфліктує з core-командою або alias Heroku.",
        "enter_btn": "✏️ Ввести текст",
        "skip_btn": "⏭ Пропустити",
        "cancel_btn": "❌ Скасувати",
        "back_btn": "◀️ Назад",
        "create_btn": "➕ Створити проєкт",
        "list_btn": "📂 Проєкти",
        "add_cmd_btn": "➕ Команда",
        "add_cfg_btn": "⚙️ Конфіг",
        "languages_btn": "🌍 Мови",
        "compile_btn": "🚀 Зібрати",
        "delete_btn": "🗑 Видалити",
        "manual_body_btn": "✍️ Вручну",
        "ai_body_btn": "🤖 AI тіло",
        "stub_body_btn": "📄 pass",
        "help_text": "🏗 <b>RedConstructor</b>\n\nIDE для Heroku Userbot з кількома проєктами, AI/ручним введенням коду, конструктором конфігів, керуванням мовами та захистом від конфліктів із core-модулями/core-командами.",
    }

    strings_jp = {
        "welcome": "🏗 <b>RedConstructor</b>\n\nTelegramでHeroku Userbot用モジュールを作成します。\n\n📦 <b>プロジェクト:</b> {count}\n操作を選択:",
        "no_projects": "📭 まだプロジェクトがありません。",
        "project_created": "✅ プロジェクト <code>{name}</code> を作成しました。",
        "project_deleted": "🗑 プロジェクト <code>{name}</code> を削除しました。",
        "cmd_added": "✅ コマンド <code>.{cmd}</code> を追加しました。",
        "cfg_added": "✅ 設定 <code>{key}</code> を追加しました。",
        "core_module_conflict": "❌ モジュール名 <code>{name}</code> はHerokuのcoreモジュールと衝突します。",
        "core_command_conflict": "❌ コマンド <code>.{cmd}</code> はHerokuのcoreコマンドまたはaliasと衝突します。",
        "enter_btn": "✏️ 入力",
        "skip_btn": "⏭ スキップ",
        "cancel_btn": "❌ キャンセル",
        "back_btn": "◀️ 戻る",
        "create_btn": "➕ 作成",
        "list_btn": "📂 一覧",
        "add_cmd_btn": "➕ コマンド",
        "add_cfg_btn": "⚙️ 設定",
        "languages_btn": "🌍 言語",
        "compile_btn": "🚀 コンパイル",
        "delete_btn": "🗑 削除",
        "manual_body_btn": "✍️ 手動",
        "ai_body_btn": "🤖 AI本文",
        "stub_body_btn": "📄 pass",
        "help_text": "🏗 <b>RedConstructor</b>\n\nHeroku Userbot向けのマルチプロジェクトIDE。AI生成、手動コード入力、設定ビルダー、言語管理、coreモジュール/coreコマンド衝突防止を含みます。",
    }

    strings_tiktok = {
        "welcome": "🏗 <b>RedConstructor</b>\n\nТут собирается модуль для Heroku прямо в тг.\n\n📦 <b>Проектов:</b> {count}\nЧе делаем:",
        "no_projects": "📭 Тут пока пусто, бро.",
        "project_created": "✅ Проект <code>{name}</code> залетел.",
        "project_deleted": "🗑 Проект <code>{name}</code> снесён.",
        "cmd_added": "✅ Команда <code>.{cmd}</code> добавлена.",
        "cfg_added": "✅ Конфиг <code>{key}</code> добавлен.",
        "core_module_conflict": "❌ <code>{name}</code> конфликтует с core-модулем Heroku.",
        "core_command_conflict": "❌ <code>.{cmd}</code> конфликтует с core-командой или алиасом Heroku.",
        "enter_btn": "✏️ вбить",
        "skip_btn": "⏭ скип",
        "cancel_btn": "❌ отмена",
        "back_btn": "◀️ назад",
        "create_btn": "➕ новый проект",
        "list_btn": "📂 проекты",
        "add_cmd_btn": "➕ команда",
        "add_cfg_btn": "⚙️ конфиг",
        "languages_btn": "🌍 языки",
        "compile_btn": "🚀 собрать",
        "delete_btn": "🗑 снести",
        "manual_body_btn": "✍️ руками",
        "ai_body_btn": "🤖 AI",
        "stub_body_btn": "📄 pass",
        "help_text": "🏗 <b>RedConstructor</b>\n\nМультипроектный конструктор модулей для Heroku Userbot: ручной или AI-ввод тела команды, билдер конфигов, панель языков и защита от перезаписи core.",
    }

    strings_neofit = {
        "welcome": "🏗 <b>RedConstructor</b>\n\nздесь делаются модули для Heroku Userbot.\n\n📦 <b>проектов:</b> {count}\nчто делаем:",
        "no_projects": "📭 пока пусто.",
        "project_created": "✅ проект <code>{name}</code> готов.",
        "project_deleted": "🗑 проект <code>{name}</code> удалён.",
        "cmd_added": "✅ команда <code>.{cmd}</code> добавлена.",
        "cfg_added": "✅ конфиг <code>{key}</code> добавлен.",
        "core_module_conflict": "❌ имя <code>{name}</code> уже занято core-модулем Heroku.",
        "core_command_conflict": "❌ команда <code>.{cmd}</code> уже занята core-командой Heroku.",
        "enter_btn": "✏️ ввести",
        "skip_btn": "⏭ пропуск",
        "cancel_btn": "❌ отмена",
        "back_btn": "◀️ назад",
        "create_btn": "➕ проект",
        "list_btn": "📂 список",
        "add_cmd_btn": "➕ команда",
        "add_cfg_btn": "⚙️ конфиг",
        "languages_btn": "🌍 языки",
        "compile_btn": "🚀 собрать",
        "delete_btn": "🗑 удалить",
        "manual_body_btn": "✍️ вручную",
        "ai_body_btn": "🤖 через AI",
        "stub_body_btn": "📄 pass",
        "help_text": "🏗 <b>RedConstructor</b>\n\nконструктор для Heroku: проекты, команды, конфиги, переводы, AI и защита от совпадений с core.",
    }

    strings_leet = {
        "welcome": "🏗 <b>RedConstructor</b>\n\nbu1ld H3r0ku m0dul3z 1n T3l3gr4m.\n\n📦 <b>pr0j3ct5:</b> {count}\nch0053:",
        "no_projects": "📭 n0 pr0j3ct5.",
        "project_created": "✅ pr0j3ct <code>{name}</code> cr34t3d.",
        "project_deleted": "🗑 pr0j3ct <code>{name}</code> d3l3t3d.",
        "cmd_added": "✅ c0mm4nd <code>.{cmd}</code> 4dd3d.",
        "cfg_added": "✅ c0nfig <code>{key}</code> 4dd3d.",
        "core_module_conflict": "❌ <code>{name}</code> c0ll1d35 w17h H3r0ku c0r3 m0dul3.",
        "core_command_conflict": "❌ <code>.{cmd}</code> c0ll1d35 w17h H3r0ku c0r3 c0mm4nd/4l145.",
        "enter_btn": "✏️ 3nt3r",
        "skip_btn": "⏭ 5k1p",
        "cancel_btn": "❌ c4nc3l",
        "back_btn": "◀️ b4ck",
        "create_btn": "➕ cr34t3",
        "list_btn": "📂 l157",
        "add_cmd_btn": "➕ cmd",
        "add_cfg_btn": "⚙️ cfg",
        "languages_btn": "🌍 l4ng5",
        "compile_btn": "🚀 c0mp1l3",
        "delete_btn": "🗑 d3l3t3",
        "manual_body_btn": "✍️ m4nu4l",
        "ai_body_btn": "🤖 41",
        "stub_body_btn": "📄 p455",
        "help_text": "🏗 <b>RedConstructor</b>\n\nmu171-pr0j3c7 H3r0ku 1D3 w17h 41/m4nu4l c0d3 1npu7, cfg bu1ld3r, l4ng p4n31 4nd c0r3 c0ll1510n pr073c710n.",
    }

    strings_uwu = {
        "welcome": "🏗 <b>RedConstructor</b>\n\nbuiwd Hewoku moduwes wight in Tewegwam.\n\n📦 <b>pwojects:</b> {count}\nchoose nya:",
        "no_projects": "📭 no pwojects yet uwu.",
        "project_created": "✅ pwoject <code>{name}</code> cweated.",
        "project_deleted": "🗑 pwoject <code>{name}</code> deweted.",
        "cmd_added": "✅ command <code>.{cmd}</code> added nya.",
        "cfg_added": "✅ config <code>{key}</code> added.",
        "core_module_conflict": "❌ <code>{name}</code> bumps into a Hewoku cowe moduwe.",
        "core_command_conflict": "❌ <code>.{cmd}</code> bumps into a Hewoku cowe command ow awias.",
        "enter_btn": "✏️ entew text",
        "skip_btn": "⏭ skip",
        "cancel_btn": "❌ cancew",
        "back_btn": "◀️ back",
        "create_btn": "➕ cweate",
        "list_btn": "📂 wist",
        "add_cmd_btn": "➕ command",
        "add_cfg_btn": "⚙️ config",
        "languages_btn": "🌍 wanguages",
        "compile_btn": "🚀 compiwe",
        "delete_btn": "🗑 dewete",
        "manual_body_btn": "✍️ manuaw",
        "ai_body_btn": "🤖 ai",
        "stub_body_btn": "📄 pass",
        "help_text": "🏗 <b>RedConstructor</b>\n\ncute Hewoku IDE with many pwojects, ai ow manuaw code entwy, config buiwdew, wanguage panel and anti-cowe-ovewwwite pwotection.",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "ai_provider",
                "anthropic",
                lambda: "AI provider for body generation",
                validator=loader.validators.Choice(AI_PROVIDER_OPTIONS),
            ),
            loader.ConfigValue(
                "ai_token",
                "",
                lambda: "API token for selected AI provider",
                validator=loader.validators.Hidden(loader.validators.String()),
            ),
            loader.ConfigValue(
                "ai_model",
                AI_PROVIDER_MODELS["anthropic"],
                lambda: "Model name for selected AI provider",
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "ai_base_url",
                "",
                lambda: "Optional custom base URL for selected provider",
                validator=loader.validators.String(),
            ),
        )
        self._active = {}
        self._core_cache = None

    def _ai_settings(self) -> typing.Tuple[typing.Optional[dict], typing.Optional[str]]:
        provider = str(self.config.get("ai_provider", "anthropic")).strip().lower()
        token = str(self.config.get("ai_token", "")).strip()
        model = str(self.config.get("ai_model", "")).strip()
        base_url = str(self.config.get("ai_base_url", "")).strip()

        if provider not in AI_PROVIDER_OPTIONS:
            return None, self._t("ai_invalid_provider", provider=provider or "?")
        if not token:
            return None, self._t("ai_token_missing")
        if not model:
            return None, self._t("ai_model_missing")

        return {
            "provider": provider,
            "token": token,
            "model": model,
            "base_url": base_url,
        }, None

    def _get_lang(self) -> str:
        try:
            raw_langs = self._db.get(translations.__name__, "lang", "en")
        except Exception:
            return "en"

        if not isinstance(raw_langs, str):
            return "en"

        langs = []
        for lang in raw_langs.split():
            normalized = lang.lower()
            if normalized == "ua":
                normalized = "uk"
            if normalized in SUPPORTED_LANGS:
                langs.append(normalized)

        if not langs:
            return "en"

        meme_langs = set(getattr(translations, "MEME_LANGUAGES", {}).keys())
        for lang in reversed(langs):
            if lang in meme_langs:
                return lang

        return langs[-1]

        return "en"

    def _lang_pack(self, lang: str) -> dict:
        base = dict(type(self).strings)
        override = dict(getattr(type(self), f"strings_{lang}", {})) if lang != "en" else {}
        patch = IDE_I18N_PATCHES.get(lang, {})
        return {**base, **override, **patch}

    def _t(self, text_key: str, **kwargs) -> str:
        lang = self._get_lang()
        value = self._lang_pack(lang).get(text_key)
        if value is None:
            value = self._lang_pack("en").get(text_key, text_key)
        rendered = value.format(**kwargs) if kwargs else value
        if text_key.endswith("_btn") or text_key in NO_PREMIUM_TEXT_KEYS:
            return rendered
        return _apply_premium_text_emojis(rendered)

    def _get_projects(self) -> dict:
        projects = self.db.get("RedConstructor", "projects", None)
        return projects if isinstance(projects, dict) else {}

    def _save_projects(self, projects: dict):
        self.db.set("RedConstructor", "projects", projects)

    def _get_project(self, pid: str) -> typing.Optional[dict]:
        project = self._get_projects().get(pid)
        if not isinstance(project, dict):
            return project
        return self._normalize_project_schema(project)

    def _touch_project(self, project: dict):
        meta = project.setdefault("meta", {})
        now = int(time.time())
        meta.setdefault("created_at", now)
        meta["updated_at"] = now

    def _save_project(self, pid: str, project: dict):
        project = self._normalize_project_schema(project)
        self._touch_project(project)
        projects = self._get_projects()
        projects[pid] = project
        self._save_projects(projects)

    def _delete_project(self, pid: str):
        projects = self._get_projects()
        projects.pop(pid, None)
        self._save_projects(projects)

    def _new_project(self, name: str, class_name: typing.Optional[str] = None) -> str:
        pid = _make_project_id()
        normalized, _ = _normalize_module_name(name)
        display_name = normalized["display_name"] if normalized else name
        final_class_name = class_name or (normalized["class_name"] if normalized else name)
        now = int(time.time())
        project = {
            "meta": {
                "name": display_name,
                "class_name": final_class_name,
                "prefix": "",
                "developer": "",
                "license": "",
                "version": "",
                "banner": "",
                "pic": "",
                "heroku_min": "",
                "created_at": now,
                "updated_at": now,
            },
            "description": "",
            "configs": [],
            "commands": {},
            "watchers": {},
            "loops": {},
            "dependencies": [],
            "command_docs": {},
            "command_resources": {},
            "extra_langs": [],
            "strings": {"en": {"name": display_name}, "ru": {}},
        }
        self._save_project(pid, project)
        return pid

    def _normalize_project_schema(self, project: dict) -> dict:
        if not isinstance(project, dict):
            return project

        meta = project.setdefault("meta", {})
        if not isinstance(meta, dict):
            meta = {}
            project["meta"] = meta
        for key in ("developer", "license", "version", "banner", "pic", "heroku_min"):
            meta[key] = _normalize_meta_text(meta.get(key))
        if meta.get("version"):
            meta["version"] = _normalize_version(meta.get("version")) or ""
        if meta.get("heroku_min"):
            meta["heroku_min"] = _normalize_version(meta.get("heroku_min")) or ""
        for key in ("banner", "pic"):
            if meta.get(key) and not _is_valid_image_meta(meta.get(key)):
                meta[key] = ""

        commands = project.setdefault("commands", {})
        watchers = project.setdefault("watchers", {})
        loops = project.setdefault("loops", {})
        strings = project.setdefault("strings", {})
        project.setdefault("command_docs", {})
        project.setdefault("command_resources", {})
        project.setdefault("extra_langs", [])
        project["dependencies"] = _parse_dependencies(project.get("dependencies") or [])

        if not isinstance(watchers, dict):
            watchers = {}
            project["watchers"] = watchers
        if not isinstance(loops, dict):
            loops = {}
            project["loops"] = loops

        command_docs = project.get("command_docs")
        if not isinstance(command_docs, dict):
            command_docs = {}
            project["command_docs"] = command_docs

        command_resources = project.get("command_resources")
        if not isinstance(command_resources, dict):
            command_resources = {}
            project["command_resources"] = command_resources

        extra_langs = []
        for lang in project.get("extra_langs") or []:
            normalized = _normalize_lang_code(lang)
            if normalized and normalized not in REQUIRED_LANGS and normalized not in extra_langs:
                extra_langs.append(normalized)
        project["extra_langs"] = extra_langs

        for lang in _project_languages(project):
            lang_strings = strings.setdefault(lang, {}) if lang != "en" else strings.setdefault("en", {})
            if not isinstance(lang_strings, dict):
                lang_strings = {}
                strings[lang] = lang_strings

        for cmd_name in list(commands):
            per_command = command_docs.setdefault(cmd_name, {})
            if not isinstance(per_command, dict):
                per_command = {}
                command_docs[cmd_name] = per_command
            legacy_key = "{}_doc".format(cmd_name)
            for lang in _project_languages(project):
                lang_strings = strings.get(lang) or {}
                legacy_value = lang_strings.pop(legacy_key, None)
                if legacy_value and not per_command.get(lang):
                    per_command[lang] = legacy_value

            per_command_resources = command_resources.setdefault(cmd_name, {})
            if not isinstance(per_command_resources, dict):
                per_command_resources = {}
                command_resources[cmd_name] = per_command_resources
            for lang in _project_languages(project):
                lang_bucket = per_command_resources.setdefault(lang, {})
                if not isinstance(lang_bucket, dict):
                    lang_bucket = {}
                    per_command_resources[lang] = lang_bucket
                for key in ("texts", "lists", "values"):
                    value = lang_bucket.setdefault(key, {})
                    if not isinstance(value, dict):
                        lang_bucket[key] = {}

        for watcher_name in list(watchers):
            watcher_meta = watchers.get(watcher_name)
            if not isinstance(watcher_meta, dict):
                watcher_meta = {}
                watchers[watcher_name] = watcher_meta
            watcher_meta["body"] = _command_body_source(watcher_meta)
            tags = watcher_meta.get("tags") or {}
            if not isinstance(tags, dict):
                tags = {}
            watcher_meta["tags"] = {
                str(tag): bool(value)
                for tag, value in tags.items()
                if str(tag) in WATCHER_TAG_OPTIONS and value
            }

        for loop_name in list(loops):
            loop_meta = loops.get(loop_name)
            if not isinstance(loop_meta, dict):
                loop_meta = {}
                loops[loop_name] = loop_meta
            loop_meta["body"] = _command_body_source(loop_meta)
            try:
                loop_meta["interval"] = max(1, int(loop_meta.get("interval") or 60))
            except Exception:
                loop_meta["interval"] = 60

        return project

    def _short(self, value: typing.Any, limit: int = 80) -> str:
        text = str(value or "").strip()
        text = re.sub(r"\s+", " ", text)
        if not text:
            return "—"
        if len(text) <= limit:
            return text
        return text[: max(0, limit - 1)].rstrip() + "…"

    def _fmt_time(self, stamp: typing.Optional[int]) -> str:
        if not stamp:
            return "—"
        try:
            return time.strftime("%Y-%m-%d %H:%M", time.localtime(int(stamp)))
        except Exception:
            return "—"

    def _default_repr(self, value: typing.Any) -> str:
        return self._short(repr(value) if value != "" else "''", 72)

    def _validator_args_text(self, cfg: dict) -> str:
        validator = cfg.get("validator", "String")
        meta = cfg.get("validator_args") or {}
        if validator in {"Choice", "MultiChoice"}:
            values = meta.get("possible_values") or []
            if not values:
                return "<code>{}</code>".format(self._t("missing_possible_values"))
            return ", ".join("<code>{}</code>".format(_escape_html(item)) for item in values)
        if validator == "RegExp":
            regex = meta.get("regex")
            return "<code>{}</code>".format(_escape_html(regex)) if regex else "<code>{}</code>".format(
                self._t("missing_regex")
            )
        return "<code>—</code>"

    def _project_button_text(self, name: str) -> str:
        return self._t("project_btn_line", name=name)

    def _command_button_text(self, cmd_name: str) -> str:
        return self._t("command_btn_line", cmd=cmd_name)

    def _config_button_text(self, key: str) -> str:
        return self._t("config_btn_line", key=key)

    def _watcher_button_text(self, name: str) -> str:
        return self._t("watcher_btn_line", name=name)

    def _loop_button_text(self, name: str) -> str:
        return self._t("loop_btn_line", name=name)

    def _lang_button_text(self, lang: str, project: dict) -> str:
        if lang in REQUIRED_LANGS:
            return self._t("lang_btn_required", lang=lang)
        if project["strings"].get(lang):
            return self._t("lang_btn_ready", lang=lang)
        return self._t("lang_btn_empty", lang=lang)

    def _dependencies_text(self, project: dict) -> str:
        deps = _parse_dependencies(project.get("dependencies") or [])
        if not deps:
            return self._t("empty_dependencies")
        return ", ".join("<code>{}</code>".format(_escape_html(dep)) for dep in deps)

    def _meta_text(self, value: typing.Any) -> str:
        normalized = _normalize_meta_text(value)
        if not normalized:
            return self._t("meta_empty")
        return "<code>{}</code>".format(_escape_html(normalized))

    def _watcher_tags_summary(self, watcher: dict) -> str:
        enabled = [tag for tag in WATCHER_TAG_OPTIONS if (watcher or {}).get("tags", {}).get(tag)]
        if not enabled:
            return self._t("empty_tags")
        return ", ".join("<code>{}</code>".format(_escape_html(tag)) for tag in enabled)

    def _choice_values(self, cfg: dict) -> list:
        return list((cfg.get("validator_args") or {}).get("possible_values") or [])

    def _config_default_label(self, value: typing.Any) -> str:
        if value in ("", None, [], ()):
            return self._t("none_selected")
        if isinstance(value, (list, tuple, set)):
            return ", ".join(str(item) for item in value)
        return str(value)

    def _command_body_preview(self, body: str) -> str:
        text = self._short(body, 96)
        return _escape_html(text)

    def _command_doc_lines(self, project: dict, cmd_name: str) -> str:
        lines = []
        for lang in _project_languages(project):
            value = _command_doc(project, cmd_name, lang)
            if lang == "en" and not value:
                value = cmd_name
            shown = _escape_html(self._short(value, 120)) if value else self._t("empty_lang_value")
            lines.append(
                self._t(
                    "module_lang_line",
                    status="✅" if value else "➕",
                    lang=LANG_LABELS.get(lang, lang),
                    value=shown,
                )
            )
        return "\n".join(lines)

    def _get_config_entry(self, project: dict, key: str) -> typing.Optional[dict]:
        for cfg in project.get("configs", []):
            if cfg.get("key") == key:
                return cfg
        return None

    def _module_desc_lines(self, project: dict) -> str:
        lines = []
        strings = project.get("strings", {})
        fallback_en = project.get("description") or project.get("meta", {}).get("name", "")
        for lang in _project_languages(project):
            value = strings.get(lang, {}).get("_cls_doc", "")
            if lang == "en" and not value:
                value = fallback_en
            shown = _escape_html(self._short(value, 140)) if value else self._t("empty_lang_value")
            lines.append(
                self._t(
                    "module_lang_line",
                    status="✅" if value else "➕",
                    lang=LANG_LABELS.get(lang, lang),
                    value=shown,
                )
            )
        return "\n".join(lines)

    def _module_lang_buttons(self, pid: str) -> list:
        project = self._get_project(pid) or {}
        buttons = []
        row = []
        for lang in _project_languages(project):
            if lang in {"en", "ru"}:
                continue
            row.append(
                {
                    "text": LANG_LABELS.get(lang, lang),
                    "callback": lambda c, target_lang=lang: self._cb_edit_module_desc(c, pid, target_lang),
                }
            )
            if len(row) == 3:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        return buttons

    def _command_lang_buttons(self, project: dict, pid: str, cmd_name: str) -> list:
        buttons = []
        row = []
        for lang in _project_languages(project):
            if lang in {"en", "ru"}:
                continue
            has_doc = bool(_command_doc(project, cmd_name, lang))
            row.append(
                {
                    "text": "{} {}".format("✅" if has_doc else "➕", LANG_LABELS.get(lang, lang)),
                    "callback": lambda c, target_lang=lang: self._cb_edit_command_doc(c, pid, cmd_name, target_lang),
                }
            )
            if len(row) == 3:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        return buttons

    def _validator_followup_prompt(self, validator: str, key: str) -> str:
        if validator in {"Choice", "MultiChoice"}:
            return self._t("ask_cfg_choice_values", validator=validator)
        if validator == "RegExp":
            return self._t("ask_cfg_regex", key=key)
        return ""

    def _project_overview_text(
        self,
        pid: str,
        project: dict,
        notice: typing.Optional[str] = None,
    ) -> str:
        meta = project.get("meta", {})
        desc_en = project.get("description") or project.get("strings", {}).get("en", {}).get("_cls_doc", "")
        desc_ru = project.get("strings", {}).get("ru", {}).get("_cls_doc", "")
        text = self._t(
            "project_panel",
            name=meta.get("name", self._t("unknown_name")),
            class_name=meta.get("class_name", self._t("unknown_class")),
            pid=pid,
            cmds=len(project.get("commands", {})),
            watchers=len(project.get("watchers", {})),
            loops=len(project.get("loops", {})),
            cfgs=len(project.get("configs", [])),
            langs=sum(1 for lang in _project_languages(project) if project["strings"].get(lang)),
            total_langs=len(_project_languages(project)),
            created=self._fmt_time(meta.get("created_at")),
            updated=self._fmt_time(meta.get("updated_at")),
            desc_en=_escape_html(self._short(desc_en, 140)),
            desc_ru=_escape_html(self._short(desc_ru, 140)),
        )
        if notice:
            return "{}\n\n{}".format(notice, text)
        return text

    def _module_panel_text(self, pid: str, project: dict) -> str:
        meta = project.get("meta", {})
        return "{}\n\n{}".format(
            self._t("module_section_hint"),
            self._t(
                "module_panel",
                name=meta.get("name", self._t("unknown_name")),
                class_name=meta.get("class_name", self._t("unknown_class")),
                pid=pid,
                updated=self._fmt_time(meta.get("updated_at")),
                developer=self._meta_text(meta.get("developer")),
                license=self._meta_text(meta.get("license")),
                version=self._meta_text(meta.get("version")),
                banner=self._meta_text(meta.get("banner")),
                pic=self._meta_text(meta.get("pic")),
                heroku_min=self._meta_text(meta.get("heroku_min")),
                descs=self._module_desc_lines(project),
                dependencies=self._dependencies_text(project),
            ),
        )

    def _commands_panel_text(self, project: dict) -> str:
        lines = [
            self._t("commands_panel", count=len(project.get("commands", {}))),
            "",
            self._t("command_section_hint"),
            "",
        ]
        if not project.get("commands"):
            lines.append(self._t("empty_commands"))
            return "\n".join(lines)

        for cmd_name, cmd in sorted(project["commands"].items()):
            lines.append(
                self._t(
                    "command_list_line",
                    cmd=cmd_name,
                    doc=_escape_html(self._short(_command_doc(project, cmd_name, "en") or cmd_name, 70)),
                    body=self._command_body_preview(_command_body_source(cmd)),
                )
            )
        return "\n".join(lines)

    def _configs_panel_text(self, project: dict) -> str:
        lines = [
            self._t("configs_panel", count=len(project.get("configs", []))),
            "",
            self._t("config_section_hint"),
            "",
        ]
        if not project.get("configs"):
            lines.append(self._t("empty_configs"))
            return "\n".join(lines)

        for cfg in project["configs"]:
            lines.append(
                self._t(
                    "config_list_line",
                    key=cfg.get("key", "?"),
                    validator=cfg.get("validator", "String"),
                    default=_escape_html(self._default_repr(cfg.get("default", ""))),
                )
            )
        return "\n".join(lines)

    def _command_detail_text(self, project: dict, cmd_name: str) -> str:
        cmd = project["commands"][cmd_name]
        return self._t(
            "command_panel",
            cmd=cmd_name,
            docs=self._command_doc_lines(project, cmd_name),
            body=_escape_html(_command_body_source(cmd).strip() or "pass"),
        )

    def _config_detail_text(self, cfg: dict) -> str:
        return self._t(
            "config_panel",
            key=cfg.get("key", "?"),
            validator=cfg.get("validator", "String"),
            default=_escape_html(self._default_repr(cfg.get("default", ""))),
            validator_args=self._validator_args_text(cfg),
        )

    def _watchers_panel_text(self, project: dict) -> str:
        lines = [
            self._t("watchers_panel", count=len(project.get("watchers", {}))),
            "",
            self._t("watcher_section_hint"),
            "",
        ]
        if not project.get("watchers"):
            lines.append(self._t("empty_watchers"))
            return "\n".join(lines)

        for watcher_name, watcher in sorted(project.get("watchers", {}).items()):
            lines.append(
                self._t(
                    "watcher_list_line",
                    name=watcher_name,
                    tags=_escape_html(self._short(", ".join(tag for tag in WATCHER_TAG_OPTIONS if watcher.get("tags", {}).get(tag)) or self._t("empty_tags"), 72)),
                    body=self._command_body_preview(_command_body_source(watcher)),
                )
            )
        return "\n".join(lines)

    def _loops_panel_text(self, project: dict) -> str:
        lines = [
            self._t("loops_panel", count=len(project.get("loops", {}))),
            "",
            self._t("loop_section_hint"),
            "",
        ]
        if not project.get("loops"):
            lines.append(self._t("empty_loops"))
            return "\n".join(lines)

        for loop_name, loop_meta in sorted(project.get("loops", {}).items()):
            lines.append(
                self._t(
                    "loop_list_line",
                    name=loop_name,
                    interval=int((loop_meta or {}).get("interval") or 60),
                    body=self._command_body_preview(_command_body_source(loop_meta)),
                )
            )
        return "\n".join(lines)

    def _watcher_detail_text(self, project: dict, watcher_name: str) -> str:
        watcher = project.get("watchers", {}).get(watcher_name, {})
        return self._t(
            "watcher_panel",
            name=watcher_name,
            tags=self._watcher_tags_summary(watcher),
            body=_escape_html(_command_body_source(watcher).strip() or "pass"),
        )

    def _loop_detail_text(self, project: dict, loop_name: str) -> str:
        loop_meta = project.get("loops", {}).get(loop_name, {})
        return self._t(
            "loop_panel",
            name=loop_name,
            interval=int((loop_meta or {}).get("interval") or 60),
            body=_escape_html(_command_body_source(loop_meta).strip() or "pass"),
        )

    def _build_ai_request(self, project: dict, state: dict, user_prompt: str) -> str:
        entity_kind = state.get("entity_kind", "command")
        cmd_name = state.get("cmd_name") or state.get("watcher_name") or state.get("loop_name") or "command"
        meta = project.get("meta", {})
        module_doc_en = project.get("strings", {}).get("en", {}).get("_cls_doc", "")
        module_doc_ru = project.get("strings", {}).get("ru", {}).get("_cls_doc", "")
        commands = project.get("commands") or {}
        loops = project.get("loops") or {}
        watchers = project.get("watchers") or {}
        inline_handlers = project.get("inline_handlers") or {}
        if not isinstance(commands, dict):
            commands = {}
        if not isinstance(loops, dict):
            loops = {}
        if not isinstance(watchers, dict):
            watchers = {}
        if not isinstance(inline_handlers, dict):
            inline_handlers = {}

        def _topology_names(bucket: dict) -> str:
            names = []
            for name in bucket.keys():
                value = str(name).strip()
                if value and value != cmd_name:
                    names.append(value)
            return ", ".join(sorted(set(names))) or "none"

        config_lines = []
        for cfg in project.get("configs", []):
            config_lines.append(
                "- self.config[{key!r}] -> {validator}, default={default}, meta={meta}".format(
                    key=cfg.get("key", ""),
                    validator=cfg.get("validator", "String"),
                    default=repr(cfg.get("default", "")),
                    meta=repr(cfg.get("validator_args") or {}),
                )
            )

        if not config_lines:
            config_lines.append("- no project configs yet")

        topology_context = (
            "Project topology:\n"
            "- other commands: {commands}\n"
            "- other loops: {loops}\n"
            "- other watchers: {watchers}\n"
            "- other inline handlers: {inline_handlers}\n\n"
        ).format(
            commands=_topology_names(commands),
            loops=_topology_names(loops),
            watchers=_topology_names(watchers),
            inline_handlers=_topology_names(inline_handlers),
        )

        header = "command"
        signature = "async def {}cmd(self, message: Message)".format(cmd_name)
        rules = [
            "- return only Python body code",
            "- no imports",
            "- no markdown",
            "- no comments",
            "- no code fences",
            "- no docstrings",
            "- no function definitions",
            "- no class definitions",
            "- no decorators",
            "- use existing Heroku patterns, not Hikka-specific ones",
            "- do not invent helper methods or attributes",
            "- do not use unsupported libraries",
            "- avoid unsupported helpers and avoid dangerous operations",
            "- do not create docstrings or decorator code",
            "- any text formatting must use Telegram HTML only",
            "- never use Markdown syntax, MarkdownV2, or parse_mode='Markdown'",
            "- HTML must be valid and compatible with Telegram message formatting",
            "- do not emit placeholder text, TODOs, notes, explanations or examples",
            "- do not output partial solutions; produce ready body code only",
            "- prefer simple, reliable Heroku-compatible logic over clever but fragile code",
            "- final code must be directly insertable without edits",
            "- project topology is authoritative; if the requested behavior should interact with another command, watcher, loop or inline handler, use the real names from the project topology above",
            "- to share state between commands, loops, and watchers (like a toggle turning a feature on/off), you MUST use Heroku's built-in database wrappers: self.get('some_key', False) and self.set('some_key', True). DO NOT use instance attributes (like self.is_active = True) because they reset on module reload",
            "- if you are writing a toggle command, its job is to update the DB using self.set() and notify the user",
            "- if you are writing a loop or watcher that depends on a toggle, it MUST check the state using self.get() as its first step and return early if the feature is disabled",
        ]
        extra_context = ""
        if entity_kind == "command":
            header = "command"
            rules.extend(
                [
                    "- use await utils.answer(message, ...) for normal replies",
                    "- if reply is needed, use reply = await message.get_reply_message()",
                    "- if args are needed, use args = utils.get_args_raw(message)",
                    "- code must work inside {}".format(signature),
                    "- for user-facing translated text prefer self._ma_cmd_text('{cmd_name}', 'key', 'Default text')".format(cmd_name=cmd_name),
                    "- for translated lists prefer self._ma_cmd_list('{cmd_name}', 'key', ['a', 'b'])".format(cmd_name=cmd_name),
                    "- for translated scalar/json values prefer self._ma_cmd_value('{cmd_name}', 'key', default)".format(cmd_name=cmd_name),
                    "- if the task can fail because of missing args or missing reply, handle it clearly",
                ]
            )
        elif entity_kind == "watcher":
            header = "watcher"
            signature = "async def {}watcher(self, message: Message)".format(cmd_name)
            rules.extend(
                [
                    "- code must work inside {}".format(signature),
                    "- this is an event watcher, so handle incoming or outgoing events/messages directly",
                    "- do not use utils.get_args_raw(message) inside watchers",
                    "- do not assume every event is a plain text message",
                    "- if the watcher replies, use await utils.answer(message, ...) only when the event supports it",
                ]
            )
            extra_context = "Watcher tags: {}\n\n".format(
                ", ".join(tag for tag in WATCHER_TAG_OPTIONS if state.get("watcher_tags", {}).get(tag)) or "none"
            )
        else:
            header = "background loop"
            signature = "async def {}loop(self)".format(cmd_name)
            rules.extend(
                [
                    "- code must work inside {}".format(signature),
                    "- loops do not receive a message argument",
                    "- never reference message, utils.get_args_raw or reply objects",
                    "- use self.config, self.db and client APIs directly if needed",
                ]
            )
            extra_context = "Loop interval: {} seconds\n\n".format(int(state.get("interval") or 60))

        return (
            "Task: write ONLY the body of a Heroku Userbot {}.\n"
            "Target platform: Heroku Userbot, not Hikka.\n"
            "Heroku is Heroku. Hikka is Hikka. Do not mix APIs.\n"
            "Module name: {module_name}\n"
            "Module class: {class_name}\n"
            "English module description: {module_doc_en}\n"
            "Russian module description: {module_doc_ru}\n"
            "Entity: {cmd_name}\n"
            "English command description: {doc_en}\n"
            "Russian command description: {doc_ru}\n\n"
            "{extra_context}"
            "Project configs available:\n"
            "{configs}\n\n"
            "{topology_context}"
            "Strict rules:\n"
            "{rules}\n\n"
            "User request for behavior:\n"
            "{request}"
        ).format(
            header,
            module_name=meta.get("name", "Unknown"),
            class_name=meta.get("class_name", self._t("unknown_class")),
            module_doc_en=module_doc_en,
            module_doc_ru=module_doc_ru,
            cmd_name=cmd_name,
            doc_en=state.get("doc_en", _command_doc(project, cmd_name, "en") or cmd_name),
            doc_ru=state.get("doc_ru", _command_doc(project, cmd_name, "ru")),
            extra_context=extra_context,
            configs="\n".join(config_lines),
            topology_context=topology_context,
            rules="\n".join(rules),
            request=user_prompt.strip(),
        )

    def _state_aliases(self, call, extra_keys: typing.Iterable[typing.Any] = ()) -> list:
        aliases = []
        for key in (
            getattr(call, "_ma_origin_inline_message_id", None),
            getattr(call, "_ma_origin_unit_id", None),
            getattr(call, "unit_id", None),
            getattr(call, "inline_message_id", None),
            *tuple(extra_keys),
        ):
            if key is not None and key not in aliases:
                aliases.append(key)
        return aliases

    def _set_state(self, call, **data):
        current = self._get_state(call)
        payload = {}
        for meta_key in ("_ma_origin_inline_message_id", "_ma_origin_unit_id"):
            if meta_key in data:
                payload[meta_key] = data[meta_key]
                continue

            value = getattr(call, meta_key, None)
            if value is None and meta_key == "_ma_origin_inline_message_id":
                value = getattr(call, "inline_message_id", None)
            if value is None and meta_key == "_ma_origin_unit_id":
                value = getattr(call, "unit_id", None)
            if value is None:
                value = current.get(meta_key)
            if value is not None:
                payload[meta_key] = value

        payload.update(data)
        for key in self._state_aliases(
            call,
            extra_keys=(
                payload.get("_ma_origin_inline_message_id"),
                payload.get("_ma_origin_unit_id"),
            ),
        ):
            self._active[key] = payload

    def _get_state(self, call, extra_keys: typing.Iterable[typing.Any] = ()) -> dict:
        for key in self._state_aliases(call, extra_keys=extra_keys):
            if key in self._active:
                return self._active[key]
        return {}

    def _clear_state(self, call):
        for key in self._state_aliases(call):
            self._active.pop(key, None)

    def _bind_origin(self, call, inline_message_id: typing.Optional[str] = None):
        state = self._get_state(call, extra_keys=(inline_message_id,))
        origin_unit_id = (
            getattr(call, "_ma_origin_unit_id", None)
            or getattr(call, "unit_id", None)
            or state.get("_ma_origin_unit_id")
        )
        if origin_unit_id is not None:
            setattr(call, "_ma_origin_unit_id", origin_unit_id)

        origin_inline_message_id = (
            inline_message_id
            or getattr(call, "_ma_origin_inline_message_id", None)
            or getattr(call, "inline_message_id", None)
            or state.get("_ma_origin_inline_message_id")
        )
        if origin_inline_message_id is not None:
            setattr(call, "_ma_origin_inline_message_id", origin_inline_message_id)

    def _origin_inline_message_id(self, call) -> typing.Optional[str]:
        return getattr(call, "_ma_origin_inline_message_id", None) or getattr(
            call, "inline_message_id", None
        )

    async def _edit_ui(self, call, text: str, reply_markup=None, **kwargs):
        kwargs.pop("inline_message_id", None)
        kwargs.pop("unit_id", None)
        await call.edit(text, reply_markup=reply_markup, **kwargs)

    def _core_modules_path(self) -> typing.Optional[Path]:
        try:
            base = Path(inspect.getfile(loader)).resolve().parent
        except Exception:
            return None

        for candidate in (base / "modules", base.parent / "modules"):
            if candidate.exists() and candidate.is_dir():
                return candidate
        return None

    def _scan_core_signature(self) -> dict:
        if self._core_cache is not None:
            return self._core_cache

        module_names = set()
        command_names = set()
        method_names = set()
        path = self._core_modules_path()
        if not path:
            self._core_cache = {"modules": module_names, "commands": command_names, "methods": method_names}
            return self._core_cache

        for file_path in path.glob("*.py"):
            module_names.add(file_path.stem.lower())
            try:
                source = file_path.read_text("utf-8")
                tree = ast.parse(source, filename=str(file_path))
            except Exception:
                continue

            for node in tree.body:
                if not isinstance(node, ast.ClassDef):
                    continue

                module_names.add(node.name.lower())

                for item in node.body:
                    if isinstance(item, ast.Assign):
                        for target in item.targets:
                            if isinstance(target, ast.Name) and target.id == "strings":
                                strings_obj = _literal(item.value)
                                if isinstance(strings_obj, dict):
                                    mod_name = strings_obj.get("name")
                                    if isinstance(mod_name, str):
                                        module_names.add(mod_name.lower())

                    if isinstance(item, ast.AsyncFunctionDef) and item.name.endswith("cmd"):
                        command_names.add(item.name[:-3].lower())
                        for deco in item.decorator_list:
                            if not isinstance(deco, ast.Call):
                                continue
                            deco_name = getattr(deco.func, "attr", None) or getattr(
                                deco.func, "id", None
                            )
                            if deco_name != "command":
                                continue
                            for kw in deco.keywords:
                                if kw.arg == "alias":
                                    alias = _literal(kw.value)
                                    if isinstance(alias, str):
                                        command_names.add(alias.lower())
                                elif kw.arg == "aliases":
                                    aliases = _literal(kw.value)
                                    if isinstance(aliases, (list, tuple, set)):
                                        for alias in aliases:
                                            if isinstance(alias, str):
                                                command_names.add(alias.lower())
                    if isinstance(item, ast.AsyncFunctionDef):
                        method_names.add(item.name.lower())

        self._core_cache = {"modules": module_names, "commands": command_names, "methods": method_names}
        return self._core_cache

    def _sanitize_projects(self):
        """Remove corrupted projects that are missing required fields."""
        projects = self._get_projects()
        bad = [
            pid for pid, proj in projects.items()
            if not isinstance(proj, dict)
            or not isinstance(proj.get("meta"), dict)
            or not proj["meta"].get("name")
        ]
        if bad:
            for pid in bad:
                projects.pop(pid, None)
            self._save_projects(projects)

    def _project_name_exists(
        self,
        display_name: str,
        class_name: typing.Optional[str] = None,
        skip_pid: typing.Optional[str] = None,
    ) -> bool:
        for pid, project in self._get_projects().items():
            if pid == skip_pid:
                continue
            meta = project.get("meta") if isinstance(project, dict) else None
            if not meta or not meta.get("name"):
                continue
            project_name = meta["name"].lower()
            project_class_name = meta.get("class_name", meta["name"]).lower()
            if project_name == display_name.lower():
                return True
            if class_name and project_class_name == class_name.lower():
                return True
        return False

    def _check_module_name(self, name: str, skip_pid: typing.Optional[str] = None) -> typing.Optional[str]:
        normalized, reason = _normalize_module_name(name)
        if reason:
            return self._t("invalid_module_name")
        display_name = normalized["display_name"]
        class_name = normalized["class_name"]
        if self._project_name_exists(display_name, class_name=class_name, skip_pid=skip_pid):
            return self._t("duplicate_project_name", name=display_name)
        core_modules = self._scan_core_signature()["modules"]
        if display_name.lower() in core_modules or class_name.lower() in core_modules:
            return self._t("core_module_conflict", name=display_name)
        return None

    def _check_command_name(self, project: dict, cmd_name: str) -> typing.Optional[str]:
        if not re.match(r"^[a-z][a-z0-9_]{0,31}$", cmd_name):
            return self._t("invalid_command_name")
        if cmd_name in project["commands"]:
            return self._t("duplicate_command_name", cmd=cmd_name)
        if cmd_name in project.get("watchers", {}) or cmd_name in project.get("loops", {}):
            return self._t("duplicate_handler_name", name=cmd_name)
        if cmd_name.lower() in self._scan_core_signature()["commands"]:
            return self._t("core_command_conflict", cmd=cmd_name)
        return None

    def _check_watcher_name(
        self,
        project: dict,
        watcher_name: str,
        *,
        skip_name: typing.Optional[str] = None,
    ) -> typing.Optional[str]:
        if not re.match(r"^[a-z][a-z0-9_]{0,31}$", watcher_name):
            return self._t("invalid_watcher_name")
        if watcher_name != skip_name and watcher_name in project.get("watchers", {}):
            return self._t("duplicate_watcher_name", name=watcher_name)
        if watcher_name in project.get("commands", {}) or watcher_name in project.get("loops", {}):
            return self._t("duplicate_handler_name", name=watcher_name)
        method_name = "{}watcher".format(watcher_name).lower()
        core = self._scan_core_signature()
        if watcher_name in RESERVED_METHOD_NAMES or method_name in core.get("methods", set()):
            return self._t("core_watcher_conflict", name=watcher_name)
        return None

    def _check_loop_name(
        self,
        project: dict,
        loop_name: str,
        *,
        skip_name: typing.Optional[str] = None,
    ) -> typing.Optional[str]:
        if not re.match(r"^[a-z][a-z0-9_]{0,31}$", loop_name):
            return self._t("invalid_loop_name")
        if loop_name != skip_name and loop_name in project.get("loops", {}):
            return self._t("duplicate_loop_name", name=loop_name)
        if loop_name in project.get("commands", {}) or loop_name in project.get("watchers", {}):
            return self._t("duplicate_handler_name", name=loop_name)
        method_name = "{}loop".format(loop_name).lower()
        core = self._scan_core_signature()
        if loop_name in RESERVED_METHOD_NAMES or method_name in core.get("methods", set()):
            return self._t("core_loop_conflict", name=loop_name)
        return None

    def _check_config_key(self, project: dict, key: str) -> typing.Optional[str]:
        if not re.match(r"^[a-z][a-z0-9_]{0,63}$", key):
            return self._t("invalid_config_name")
        if any(cfg["key"] == key for cfg in project["configs"]):
            return self._t("duplicate_config_name", key=key)
        return None

    def _validate_project_conflicts(self, project: dict) -> typing.Optional[str]:
        name = project["meta"]["name"]
        class_name = project["meta"].get("class_name", name)
        core_modules = self._scan_core_signature()["modules"]
        if name.lower() in core_modules or class_name.lower() in core_modules:
            return self._t("core_module_conflict", name=name)

        for cmd_name in project["commands"]:
            if cmd_name.lower() in self._scan_core_signature()["commands"]:
                return self._t("core_command_conflict", cmd=cmd_name)

        for watcher_name in project.get("watchers", {}):
            conflict = self._check_watcher_name(project, watcher_name, skip_name=watcher_name)
            if conflict:
                return conflict

        for loop_name in project.get("loops", {}):
            conflict = self._check_loop_name(project, loop_name, skip_name=loop_name)
            if conflict:
                return conflict

        return None

    def _input_button(
        self,
        handler,
        args: tuple = (),
        placeholder: typing.Optional[str] = None,
        prompt_text: typing.Optional[str] = None,
        origin_inline_message_id: typing.Optional[str] = None,
    ) -> dict:
        return {
            "text": self._t("enter_btn"),
            "input": _strip_html(placeholder or prompt_text or self._t("enter_btn")),
            "handler": handler,
            "args": args,
        }

    async def _prompt(
        self,
        call,
        text: str,
        handler,
        args: tuple = (),
        placeholder: typing.Optional[str] = None,
        skip_handler=None,
        skip_args: tuple = (),
        cancel_callback=None,
    ):
        buttons = [[
            self._input_button(
                handler,
                args=args,
                placeholder=placeholder,
                prompt_text=text,
            )
        ]]

        row = []
        if skip_handler:
            def _skip_and_clear(c, h=skip_handler, a=skip_args):
                self._clear_state(c)
                return h(c, *a)

            row.append({"text": self._t("skip_btn"), "callback": _skip_and_clear})
        if cancel_callback:
            def _cancel_and_clear(c, cb=cancel_callback):
                self._clear_state(c)
                return cb(c)

            row.append({"text": self._t("cancel_btn"), "callback": _cancel_and_clear})
        if row:
            buttons.append(row)

        await self._edit_ui(call, text, reply_markup=buttons)

    async def _show_main_menu(self, message):
        self._sanitize_projects()
        projects = self._get_projects()
        buttons = [[{"text": self._t("create_btn"), "callback": self._cb_create_project}]]
        if projects:
            buttons.append([{"text": self._t("list_btn"), "callback": self._cb_list_projects}])

        await self.inline.form(
            text=self._t("welcome", count=len(projects)),
            message=message,
            reply_markup=buttons,
        )

    async def _show_project_panel(
        self,
        call,
        pid: str,
        notice: typing.Optional[str] = None,
    ):
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        text = self._project_overview_text(pid, project, notice=notice)
        buttons = [
            [
                {"text": self._t("overview_btn"), "callback": lambda c: self._show_project_panel(c, pid)},
                {"text": self._t("module_btn"), "callback": lambda c: self._cb_module_panel(c, pid)},
                {"text": self._t("languages_btn"), "callback": lambda c: self._cb_lang_panel(c, pid)},
            ],
            [
                {"text": self._t("commands_btn"), "callback": lambda c: self._cb_commands_panel(c, pid)},
                {"text": self._t("watchers_btn"), "callback": lambda c: self._cb_watchers_panel(c, pid)},
                {"text": self._t("loops_btn"), "callback": lambda c: self._cb_loops_panel(c, pid)},
            ],
            [
                {"text": self._t("configs_btn"), "callback": lambda c: self._cb_configs_panel(c, pid)},
                {"text": self._t("compile_btn"), "callback": lambda c: self._cb_compile(c, pid)},
            ],
            [
                {"text": self._t("delete_btn"), "callback": lambda c: self._cb_delete_confirm(c, pid)},
                {"text": self._t("back_btn"), "callback": self._cb_list_projects},
            ],
        ]
        await self._edit_ui(call, text, reply_markup=buttons)

    async def _cb_module_panel(self, call, pid: str):
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        buttons = [
            [{"text": self._t("edit_name_btn"), "callback": lambda c: self._cb_edit_module_name(c, pid)}],
            [
                {"text": self._t("edit_developer_btn"), "callback": lambda c: self._cb_edit_module_meta(c, pid, "developer")},
                {"text": self._t("edit_license_btn"), "callback": lambda c: self._cb_license_panel(c, pid)},
            ],
            [
                {"text": self._t("edit_version_btn"), "callback": lambda c: self._cb_edit_module_meta(c, pid, "version")},
                {"text": self._t("edit_scope_btn"), "callback": lambda c: self._cb_edit_module_meta(c, pid, "heroku_min")},
            ],
            [
                {"text": self._t("edit_banner_btn"), "callback": lambda c: self._cb_edit_module_meta(c, pid, "banner")},
                {"text": self._t("edit_pic_btn"), "callback": lambda c: self._cb_edit_module_meta(c, pid, "pic")},
            ],
            [
                {"text": self._t("edit_desc_en_btn"), "callback": lambda c: self._cb_edit_module_desc(c, pid, "en")},
                {"text": self._t("edit_desc_ru_btn"), "callback": lambda c: self._cb_edit_module_desc(c, pid, "ru")},
            ],
            [{"text": self._t("dependencies_btn"), "callback": lambda c: self._cb_dependencies_panel(c, pid)}],
        ]
        buttons.extend(self._module_lang_buttons(pid))
        buttons.append([{"text": self._t("back_btn"), "callback": lambda c: self._show_project_panel(c, pid)}])
        await self._edit_ui(
            call,
            self._module_panel_text(pid, project),
            reply_markup=buttons,
        )

    async def _cb_edit_module_name(self, call, pid: str):
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        self._clear_state(call)
        self._set_state(call, flow="edit_module_name", pid=pid)
        await self._prompt(
            call,
            self._t("ask_name"),
            self._handle_edit_module_name,
            args=(pid,),
            placeholder=project["meta"].get("name", self._t("placeholder_module_name")),
            cancel_callback=lambda c: self._cb_module_panel(c, pid),
        )

    async def _handle_edit_module_name(self, call, data: str, pid: str):
        self._bind_origin(call)
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        raw_name = (data or "").strip()
        error = self._check_module_name(raw_name, skip_pid=pid)
        if error:
            return await self._prompt(
                call,
                error + "\n\n" + self._t("ask_name"),
                self._handle_edit_module_name,
                args=(pid,),
                placeholder=project["meta"].get("name", self._t("placeholder_module_name")),
                cancel_callback=lambda c: self._cb_module_panel(c, pid),
            )

        normalized, _ = _normalize_module_name(raw_name)
        project["meta"]["name"] = normalized["display_name"]
        project["meta"]["class_name"] = normalized["class_name"]
        project["strings"].setdefault("en", {})
        project["strings"]["en"]["name"] = normalized["display_name"]
        if not project["strings"]["en"].get("_cls_doc"):
            project["strings"]["en"]["_cls_doc"] = normalized["display_name"]
        self._save_project(pid, project)
        self._clear_state(call)

        await self._edit_ui(call, self._t("meta_saved"))
        await asyncio.sleep(1)
        await self._cb_module_panel(call, pid)

    async def _cb_edit_module_desc(self, call, pid: str, lang: str):
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        current = ""
        if lang == "en":
            current = project.get("description", "") or project["strings"].get("en", {}).get("_cls_doc", "")
        else:
            current = project["strings"].get(lang, {}).get("_cls_doc", "")

        self._clear_state(call)
        self._set_state(call, flow="edit_module_desc", pid=pid, lang=lang)
        await self._prompt(
            call,
            self._t("ask_desc_en")
            if lang == "en"
            else self._t("ask_desc_ru")
            if lang == "ru"
            else self._t("ask_lang_cls", lang=LANG_LABELS.get(lang, lang)),
            self._handle_edit_module_desc,
            args=(pid, lang),
            placeholder=current or (
                self._t("placeholder_desc_en")
                if lang == "en"
                else self._t("placeholder_desc_ru")
                if lang == "ru"
                else self._t("placeholder_lang_desc")
            ),
            skip_handler=lambda c, project_id=pid: self._handle_edit_module_desc(c, "", project_id, lang),
            cancel_callback=lambda c: self._cb_module_panel(c, pid),
        )

    async def _handle_edit_module_desc(self, call, data: str, pid: str, lang: str):
        self._bind_origin(call)
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        value = (data or "").strip()
        project["strings"].setdefault(lang, {})
        if lang == "en":
            project["description"] = value
            project["strings"]["en"]["_cls_doc"] = value or project["meta"]["name"]
        else:
            if value:
                project["strings"][lang]["_cls_doc"] = value
            else:
                project["strings"][lang].pop("_cls_doc", None)
        self._save_project(pid, project)
        self._clear_state(call)

        await self._edit_ui(call, self._t("meta_saved"))
        await asyncio.sleep(1)
        await self._cb_module_panel(call, pid)

    async def _cb_edit_module_meta(self, call, pid: str, field: str):
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        prompts = {
            "developer": ("ask_module_developer", "placeholder_module_developer"),
            "version": ("ask_module_version", "placeholder_module_version"),
            "banner": ("ask_module_banner", "placeholder_module_banner"),
            "pic": ("ask_module_pic", "placeholder_module_pic"),
            "heroku_min": ("ask_module_heroku_min", "placeholder_module_heroku_min"),
        }
        if field not in prompts:
            return await self._cb_module_panel(call, pid)

        prompt_key, placeholder_key = prompts[field]
        current = project.get("meta", {}).get(field, "")
        self._clear_state(call)
        self._set_state(call, flow="edit_module_meta", pid=pid, field=field)
        await self._prompt(
            call,
            self._t(prompt_key),
            self._handle_edit_module_meta,
            args=(pid, field),
            placeholder=current or self._t(placeholder_key),
            skip_handler=lambda c, project_id=pid, current_field=field: self._handle_edit_module_meta(
                c, "", project_id, current_field
            ),
            cancel_callback=lambda c: self._cb_module_panel(c, pid),
        )

    async def _handle_edit_module_meta(self, call, data: str, pid: str, field: str):
        self._bind_origin(call)
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        value = _normalize_meta_text(data)
        error = None
        if field == "version":
            normalized = _normalize_version(value)
            if value and not normalized:
                error = self._t("invalid_module_version")
            value = normalized
        elif field == "heroku_min":
            normalized = _normalize_version(value)
            if value and not normalized:
                error = self._t("invalid_module_heroku_min")
            value = normalized
        elif field in {"banner", "pic"} and value and not _is_valid_image_meta(value):
            error = self._t("invalid_module_image")

        if error:
            return await self._prompt(
                call,
                error + "\n\n" + self._t(
                    {
                        "developer": "ask_module_developer",
                        "version": "ask_module_version",
                        "banner": "ask_module_banner",
                        "pic": "ask_module_pic",
                        "heroku_min": "ask_module_heroku_min",
                    }[field]
                ),
                self._handle_edit_module_meta,
                args=(pid, field),
                placeholder=project.get("meta", {}).get(field, "") or self._t(
                    {
                        "developer": "placeholder_module_developer",
                        "version": "placeholder_module_version",
                        "banner": "placeholder_module_banner",
                        "pic": "placeholder_module_pic",
                        "heroku_min": "placeholder_module_heroku_min",
                    }[field]
                ),
                cancel_callback=lambda c: self._cb_module_panel(c, pid),
            )

        project.setdefault("meta", {})[field] = value
        self._save_project(pid, project)
        self._clear_state(call)
        await self._edit_ui(call, self._t("meta_saved"))
        await asyncio.sleep(1)
        await self._cb_module_panel(call, pid)

    async def _cb_license_panel(self, call, pid: str):
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        current = project.get("meta", {}).get("license", "")
        buttons = []
        row = []
        for license_name in LICENSE_OPTIONS:
            row.append(
                {
                    "text": "{} {}".format("✅" if license_name == current else "•", license_name),
                    "callback": lambda c, value=license_name: self._handle_module_license(c, pid, value),
                }
            )
            if len(row) == 3:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append(
            [
                {"text": self._t("clear_meta_btn"), "callback": lambda c: self._handle_module_license(c, pid, "")},
                {"text": self._t("back_btn"), "callback": lambda c: self._cb_module_panel(c, pid)},
            ]
        )
        await self._edit_ui(
            call,
            self._t("license_panel", current=self._meta_text(current)),
            reply_markup=buttons,
        )

    async def _handle_module_license(self, call, pid: str, license_name: str):
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        project.setdefault("meta", {})["license"] = _normalize_meta_text(license_name)
        self._save_project(pid, project)
        await self._edit_ui(call, self._t("meta_saved"))
        await asyncio.sleep(1)
        await self._cb_module_panel(call, pid)

    async def _cb_dependencies_panel(self, call, pid: str):
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        await self._edit_ui(
            call,
            self._t("dependencies_panel", deps=self._dependencies_text(project)),
            reply_markup=[
                [{"text": self._t("edit_dependencies_btn"), "callback": lambda c: self._cb_edit_dependencies(c, pid)}],
                [{"text": self._t("back_btn"), "callback": lambda c: self._cb_module_panel(c, pid)}],
            ],
        )

    async def _cb_edit_dependencies(self, call, pid: str):
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        self._clear_state(call)
        self._set_state(call, flow="edit_dependencies", pid=pid)
        await self._prompt(
            call,
            self._t("ask_dependencies"),
            self._handle_edit_dependencies,
            args=(pid,),
            placeholder=", ".join(_parse_dependencies(project.get("dependencies") or [])),
            skip_handler=lambda c, project_id=pid: self._handle_edit_dependencies(c, "", project_id),
            cancel_callback=lambda c: self._cb_dependencies_panel(c, pid),
        )

    async def _handle_edit_dependencies(self, call, data: str, pid: str):
        self._bind_origin(call)
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        project["dependencies"] = _parse_dependencies(data or "")
        self._save_project(pid, project)
        self._clear_state(call)
        await self._edit_ui(call, self._t("dependencies_saved"))
        await asyncio.sleep(1)
        await self._cb_dependencies_panel(call, pid)

    async def _cb_create_project(self, call):
        self._clear_state(call)
        await self._prompt(
            call,
            self._t("ask_name"),
            self._handle_project_name,
            placeholder=self._t("placeholder_module_name"),
            cancel_callback=self._cb_back_to_main,
        )

    async def _handle_project_name(
        self,
        call,
        data: str,
    ):
        self._bind_origin(call)
        raw_name = (data or "").strip()
        error = self._check_module_name(raw_name)
        if error:
            return await self._prompt(
                call,
                error + "\n\n" + self._t("ask_name"),
                self._handle_project_name,
                placeholder=self._t("placeholder_module_name"),
                cancel_callback=self._cb_back_to_main,
            )

        normalized, _ = _normalize_module_name(raw_name)
        self._set_state(
            call,
            flow="create_project",
            name=normalized["display_name"],
            class_name=normalized["class_name"],
        )
        await self._prompt(
            call,
            self._t("ask_desc_en"),
            self._handle_project_desc_en,
            placeholder=self._t("placeholder_desc_en"),
            skip_handler=self._skip_project_desc_en,
            cancel_callback=self._cb_back_to_main,
        )

    async def _skip_project_desc_en(self, call):
        await self._handle_project_desc_en(call, "")

    async def _handle_project_desc_en(
        self,
        call,
        data: str,
    ):
        self._bind_origin(call)
        state = self._get_state(call)
        state["desc_en"] = (data or "").strip()
        self._set_state(call, **state)
        await self._prompt(
            call,
            self._t("ask_desc_ru"),
            self._handle_project_desc_ru,
            placeholder=self._t("placeholder_desc_ru"),
            skip_handler=self._skip_project_desc_ru,
            cancel_callback=self._cb_back_to_main,
        )

    async def _skip_project_desc_ru(self, call):
        await self._handle_project_desc_ru(call, "")

    async def _handle_project_desc_ru(
        self,
        call,
        data: str,
    ):
        self._bind_origin(call)
        state = self._get_state(call)
        name = state.get("name")
        class_name = state.get("class_name")
        if not name:
            return await self._cb_back_to_main(call)

        pid = self._new_project(name, class_name=class_name)
        project = self._get_project(pid)
        desc_en = state.get("desc_en", "").strip()
        desc_ru = (data or "").strip()
        project["description"] = desc_en
        project["strings"]["en"]["_cls_doc"] = desc_en or name
        if desc_ru:
            project["strings"]["ru"]["_cls_doc"] = desc_ru
        self._save_project(pid, project)
        self._clear_state(call)

        await self._show_project_panel(call, pid, notice=self._t("project_created", name=name))

    async def _cb_list_projects(self, call):
        self._sanitize_projects()
        projects = self._get_projects()
        if not projects:
            return await self._edit_ui(
                call,
                self._t("no_projects"),
                reply_markup=[[{"text": self._t("back_btn"), "callback": self._cb_back_to_main}]],
            )

        text = self._t("list_title") + "\n\n"
        buttons = []
        for pid, proj in projects.items():
            text += self._t(
                "project_list_line",
                name=proj["meta"]["name"],
                cmds=len(proj["commands"]),
                cfgs=len(proj["configs"]),
            )
            buttons.append(
                [{"text": self._project_button_text(proj["meta"]["name"]), "callback": lambda c, p=pid: self._show_project_panel(c, p)}]
            )

        buttons.append([{"text": self._t("back_btn"), "callback": self._cb_back_to_main}])
        await self._edit_ui(call, text, reply_markup=buttons)

    async def _cb_commands_panel(self, call, pid: str):
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        buttons = [[{"text": self._t("add_cmd_btn"), "callback": lambda c: self._cb_add_command(c, pid)}]]
        for cmd_name in sorted(project["commands"]):
            buttons.append(
                [{"text": self._command_button_text(cmd_name), "callback": lambda c, name=cmd_name: self._cb_command_detail(c, pid, name)}]
            )
        buttons.append([{"text": self._t("back_btn"), "callback": lambda c: self._show_project_panel(c, pid)}])
        await self._edit_ui(call, self._commands_panel_text(project), reply_markup=buttons)

    async def _cb_command_detail(self, call, pid: str, cmd_name: str):
        project = self._get_project(pid)
        if not project or cmd_name not in project.get("commands", {}):
            return await self._edit_ui(call, self._t("project_not_found"))

        buttons = [
            [
                {"text": self._t("edit_doc_en_btn"), "callback": lambda c: self._cb_edit_command_doc(c, pid, cmd_name, "en")},
                {"text": self._t("edit_doc_ru_btn"), "callback": lambda c: self._cb_edit_command_doc(c, pid, cmd_name, "ru")},
            ],
        ]
        buttons.extend(self._command_lang_buttons(project, pid, cmd_name))
        buttons.extend(
            [
                [
                    {"text": self._t("edit_docs_btn"), "callback": lambda c: self._cb_command_docs_panel(c, pid, cmd_name)},
                    {"text": self._t("edit_body_btn"), "callback": lambda c: self._cb_edit_command_body(c, pid, cmd_name)},
                ],
                [
                    {"text": self._t("delete_btn"), "callback": lambda c: self._cb_delete_command(c, pid, cmd_name)},
                ],
                [{"text": self._t("back_btn"), "callback": lambda c: self._cb_commands_panel(c, pid)}],
            ]
        )

        await self._edit_ui(
            call,
            self._command_detail_text(project, cmd_name),
            reply_markup=buttons,
        )

    async def _cb_command_docs_panel(self, call, pid: str, cmd_name: str):
        project = self._get_project(pid)
        if not project or cmd_name not in project.get("commands", {}):
            return await self._edit_ui(call, self._t("project_not_found"))

        buttons = []
        row = []
        for lang in _project_languages(project):
            row.append(
                {
                    "text": self._lang_button_text(lang, {"strings": {lang: {"_cls_doc": _command_doc(project, cmd_name, lang)}}}),
                    "callback": lambda c, target_lang=lang: self._cb_edit_command_doc(c, pid, cmd_name, target_lang),
                }
            )
            if len(row) == 3:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([{"text": self._t("back_btn"), "callback": lambda c: self._cb_command_detail(c, pid, cmd_name)}])
        await self._edit_ui(call, self._command_detail_text(project, cmd_name), reply_markup=buttons)

    async def _cb_edit_command_doc(self, call, pid: str, cmd_name: str, lang: str):
        project = self._get_project(pid)
        if not project or cmd_name not in project.get("commands", {}):
            return await self._edit_ui(call, self._t("project_not_found"))

        current = _command_doc(project, cmd_name, lang)
        self._clear_state(call)
        self._set_state(call, flow="edit_command_doc", pid=pid, cmd_name=cmd_name, lang=lang)
        await self._prompt(
            call,
            self._t("ask_cmd_doc_en")
            if lang == "en"
            else self._t("ask_cmd_doc_ru")
            if lang == "ru"
            else self._t("ask_cmd_doc_lang", lang=LANG_LABELS.get(lang, lang)),
            self._handle_edit_command_doc,
            args=(pid, cmd_name, lang),
            placeholder=current or (
                self._t("placeholder_cmd_desc_en")
                if lang == "en"
                else self._t("placeholder_cmd_desc_ru")
                if lang == "ru"
                else self._t("placeholder_lang_desc")
            ),
            skip_handler=lambda c, project_id=pid, command=cmd_name, target_lang=lang: self._handle_edit_command_doc(
                c, "", project_id, command, target_lang
            ),
            cancel_callback=lambda c: self._cb_command_docs_panel(c, pid, cmd_name),
        )

    async def _handle_edit_command_doc(self, call, data: str, pid: str, cmd_name: str, lang: str):
        self._bind_origin(call)
        project = self._get_project(pid)
        if not project or cmd_name not in project.get("commands", {}):
            return await self._edit_ui(call, self._t("project_not_found"))

        value = (data or "").strip()
        project.setdefault("command_docs", {})
        project["command_docs"].setdefault(cmd_name, {})
        if lang == "en":
            project["command_docs"][cmd_name][lang] = value or cmd_name
        else:
            if value:
                project["command_docs"][cmd_name][lang] = value
            else:
                project["command_docs"][cmd_name].pop(lang, None)
        self._save_project(pid, project)
        self._clear_state(call)

        await self._edit_ui(call, self._t("cmd_updated", cmd=cmd_name))
        await asyncio.sleep(1)
        await self._cb_command_detail(call, pid, cmd_name)

    async def _cb_edit_command_body(self, call, pid: str, cmd_name: str):
        project = self._get_project(pid)
        if not project or cmd_name not in project.get("commands", {}):
            return await self._edit_ui(call, self._t("project_not_found"))

        self._clear_state(call)
        self._set_state(
            call,
            flow="edit_command_body",
            pid=pid,
            cmd_name=cmd_name,
            original_cmd_name=cmd_name,
            entity_kind="command",
            editing_command=True,
            doc_en=_command_doc(project, cmd_name, "en") or cmd_name,
            doc_ru=_command_doc(project, cmd_name, "ru"),
        )
        await self._show_body_mode(call, pid)

    async def _cb_delete_command(self, call, pid: str, cmd_name: str):
        project = self._get_project(pid)
        if not project or cmd_name not in project.get("commands", {}):
            return await self._edit_ui(call, self._t("project_not_found"))

        project["commands"].pop(cmd_name, None)
        project.setdefault("command_docs", {}).pop(cmd_name, None)
        project.setdefault("command_resources", {}).pop(cmd_name, None)
        self._save_project(pid, project)
        await self._edit_ui(call, self._t("cmd_deleted", cmd=cmd_name))
        await asyncio.sleep(1)
        await self._cb_commands_panel(call, pid)

    async def _cb_watchers_panel(self, call, pid: str):
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        buttons = [[{"text": self._t("add_watcher_btn"), "callback": lambda c: self._cb_add_watcher(c, pid)}]]
        for watcher_name in sorted(project.get("watchers", {})):
            buttons.append(
                [{"text": self._watcher_button_text(watcher_name), "callback": lambda c, name=watcher_name: self._cb_watcher_detail(c, pid, name)}]
            )
        buttons.append([{"text": self._t("back_btn"), "callback": lambda c: self._show_project_panel(c, pid)}])
        await self._edit_ui(call, self._watchers_panel_text(project), reply_markup=buttons)

    async def _cb_add_watcher(self, call, pid: str):
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        self._clear_state(call)
        self._set_state(call, flow="add_watcher", pid=pid, entity_kind="watcher")
        await self._prompt(
            call,
            self._t("ask_watcher_name"),
            self._handle_watcher_name,
            args=(pid,),
            placeholder=self._t("placeholder_watcher_name"),
            cancel_callback=lambda c: self._cb_watchers_panel(c, pid),
        )

    async def _handle_watcher_name(self, call, data: str, pid: str):
        self._bind_origin(call)
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        watcher_name = (data or "").strip().lower()
        error = self._check_watcher_name(project, watcher_name)
        if error:
            return await self._prompt(
                call,
                error + "\n\n" + self._t("ask_watcher_name"),
                self._handle_watcher_name,
                args=(pid,),
                placeholder=self._t("placeholder_watcher_name"),
                cancel_callback=lambda c: self._cb_watchers_panel(c, pid),
            )

        state = self._get_state(call)
        state.update({"pid": pid, "watcher_name": watcher_name, "entity_kind": "watcher", "watcher_tags": {}})
        self._set_state(call, **state)
        await self._show_watcher_body_mode(call, pid)

    async def _show_watcher_body_mode(self, call, pid: str):
        self._bind_origin(call)
        state = self._get_state(call)
        watcher_name = state.get("watcher_name", "?")
        cancel_callback = (
            (lambda c, project_id=pid, watcher=watcher_name: self._cb_watcher_detail(c, project_id, watcher))
            if state.get("editing_watcher")
            else (lambda c, project_id=pid: self._cb_watchers_panel(c, project_id))
        )
        await self._edit_ui(
            call,
            self._t("watcher_body_mode", name=watcher_name),
            reply_markup=[
                [
                    {
                        "text": self._t("ai_body_btn"),
                        "input": self._t("ask_watcher_ai_prompt", name=watcher_name),
                        "handler": self._handle_watcher_ai_body,
                        "args": (pid,),
                    }
                ],
                [
                    {
                        "text": self._t("manual_body_btn"),
                        "input": self._t("ask_watcher_body", name=watcher_name),
                        "handler": self._handle_watcher_manual_body,
                        "args": (pid,),
                    }
                ],
                [
                    {"text": self._t("stub_body_btn"), "callback": lambda c, project_id=pid: self._save_watcher(c, project_id, "pass")},
                    {"text": self._t("cancel_btn"), "callback": cancel_callback},
                ],
            ],
        )

    async def _handle_watcher_ai_body(self, call, data: str, pid: str):
        self._bind_origin(call)
        prompt = (data or "").strip()
        state = self._get_state(call)
        watcher_name = state.get("watcher_name", "watcher")
        if not prompt:
            return await self._show_watcher_body_mode(call, pid)

        settings, settings_error = self._ai_settings()
        if settings_error:
            await self._edit_ui(call, settings_error)
            await asyncio.sleep(2)
            return await self._show_watcher_body_mode(call, pid)

        await self._edit_ui(call, self._t("ai_generating", cmd=watcher_name))
        project = self._get_project(pid)
        generated = await _ai_generate(self._build_ai_request(project or {}, state, prompt), settings)
        if generated.startswith("# AI Error"):
            await self._edit_ui(call, self._t("ai_error", err=generated))
            await asyncio.sleep(2)
            return await self._show_watcher_body_mode(call, pid)
        body_error = _validate_body_syntax(generated)
        if body_error:
            await self._edit_ui(call, self._t("invalid_code", err=body_error))
            await asyncio.sleep(2)
            return await self._show_watcher_body_mode(call, pid)

        await self._edit_ui(call, self._t("ai_done", cmd=watcher_name))
        await asyncio.sleep(1)
        await self._save_watcher(call, pid, generated)

    async def _handle_watcher_manual_body(self, call, data: str, pid: str):
        self._bind_origin(call)
        body = (data or "").strip() or "pass"
        body_error = _validate_body_syntax(body)
        if body_error:
            await self._edit_ui(call, self._t("invalid_code", err=body_error))
            await asyncio.sleep(2)
            return await self._show_watcher_body_mode(call, pid)

        await self._save_watcher(call, pid, body)

    async def _save_watcher(self, call, pid: str, body: str):
        state = self._get_state(call)
        project = self._get_project(pid)
        if not project:
            self._clear_state(call)
            return await self._edit_ui(call, self._t("project_not_found"))

        watcher_name = state.get("watcher_name")
        original_watcher_name = state.get("original_watcher_name")
        existing = bool(original_watcher_name and original_watcher_name in project.get("watchers", {}))
        error = self._check_watcher_name(project, watcher_name, skip_name=original_watcher_name if existing else None)
        if error:
            self._clear_state(call)
            return await self._edit_ui(call, error)

        existing_watcher = project.get("watchers", {}).get(original_watcher_name or watcher_name, {})
        project.setdefault("watchers", {})
        project["watchers"][watcher_name] = {
            "body": body,
            "tags": dict(state.get("watcher_tags") or existing_watcher.get("tags") or {}),
        }
        if existing and original_watcher_name != watcher_name:
            project["watchers"].pop(original_watcher_name, None)
        self._save_project(pid, project)
        self._clear_state(call)

        await self._edit_ui(call, self._t("watcher_updated" if existing else "watcher_added", name=watcher_name))
        await asyncio.sleep(1)
        if existing:
            await self._cb_watcher_detail(call, pid, watcher_name)
        else:
            await self._cb_watchers_panel(call, pid)

    async def _cb_watcher_detail(self, call, pid: str, watcher_name: str):
        project = self._get_project(pid)
        if not project or watcher_name not in project.get("watchers", {}):
            return await self._edit_ui(call, self._t("project_not_found"))

        await self._edit_ui(
            call,
            self._watcher_detail_text(project, watcher_name),
            reply_markup=[
                [
                    {"text": self._t("edit_body_btn"), "callback": lambda c: self._cb_edit_watcher_body(c, pid, watcher_name)},
                    {"text": self._t("tags_btn"), "callback": lambda c: self._cb_watcher_tags_panel(c, pid, watcher_name)},
                ],
                [{"text": self._t("delete_btn"), "callback": lambda c: self._cb_delete_watcher(c, pid, watcher_name)}],
                [{"text": self._t("back_btn"), "callback": lambda c: self._cb_watchers_panel(c, pid)}],
            ],
        )

    async def _cb_edit_watcher_body(self, call, pid: str, watcher_name: str):
        project = self._get_project(pid)
        if not project or watcher_name not in project.get("watchers", {}):
            return await self._edit_ui(call, self._t("project_not_found"))

        self._clear_state(call)
        self._set_state(
            call,
            flow="edit_watcher_body",
            pid=pid,
            watcher_name=watcher_name,
            original_watcher_name=watcher_name,
            entity_kind="watcher",
            editing_watcher=True,
            watcher_tags=dict(project["watchers"][watcher_name].get("tags") or {}),
        )
        await self._show_watcher_body_mode(call, pid)

    async def _cb_watcher_tags_panel(self, call, pid: str, watcher_name: str):
        project = self._get_project(pid)
        if not project or watcher_name not in project.get("watchers", {}):
            return await self._edit_ui(call, self._t("project_not_found"))

        watcher = project["watchers"][watcher_name]
        buttons = []
        row = []
        for tag in WATCHER_TAG_OPTIONS:
            enabled = bool(watcher.get("tags", {}).get(tag))
            row.append(
                {
                    "text": "{} {}".format("✅" if enabled else "➕", tag),
                    "callback": lambda c, current=tag: self._toggle_watcher_tag(c, pid, watcher_name, current),
                }
            )
            if len(row) == 3:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([{"text": self._t("back_btn"), "callback": lambda c: self._cb_watcher_detail(c, pid, watcher_name)}])
        await self._edit_ui(
            call,
            self._t("watcher_tags_panel", name=watcher_name, tags=self._watcher_tags_summary(watcher)),
            reply_markup=buttons,
        )

    async def _toggle_watcher_tag(self, call, pid: str, watcher_name: str, tag: str):
        project = self._get_project(pid)
        if not project or watcher_name not in project.get("watchers", {}):
            return await self._edit_ui(call, self._t("project_not_found"))

        watcher = project["watchers"][watcher_name]
        tags = dict(watcher.get("tags") or {})
        if tags.get(tag):
            tags.pop(tag, None)
        else:
            tags[tag] = True
        watcher["tags"] = tags
        self._save_project(pid, project)
        await self._cb_watcher_tags_panel(call, pid, watcher_name)

    async def _cb_delete_watcher(self, call, pid: str, watcher_name: str):
        project = self._get_project(pid)
        if not project or watcher_name not in project.get("watchers", {}):
            return await self._edit_ui(call, self._t("project_not_found"))

        project["watchers"].pop(watcher_name, None)
        self._save_project(pid, project)
        await self._edit_ui(call, self._t("watcher_deleted", name=watcher_name))
        await asyncio.sleep(1)
        await self._cb_watchers_panel(call, pid)

    async def _cb_loops_panel(self, call, pid: str):
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        buttons = [[{"text": self._t("add_loop_btn"), "callback": lambda c: self._cb_add_loop(c, pid)}]]
        for loop_name in sorted(project.get("loops", {})):
            buttons.append(
                [{"text": self._loop_button_text(loop_name), "callback": lambda c, name=loop_name: self._cb_loop_detail(c, pid, name)}]
            )
        buttons.append([{"text": self._t("back_btn"), "callback": lambda c: self._show_project_panel(c, pid)}])
        await self._edit_ui(call, self._loops_panel_text(project), reply_markup=buttons)

    async def _cb_add_loop(self, call, pid: str):
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        self._clear_state(call)
        self._set_state(call, flow="add_loop", pid=pid, entity_kind="loop")
        await self._prompt(
            call,
            self._t("ask_loop_name"),
            self._handle_loop_name,
            args=(pid,),
            placeholder=self._t("placeholder_loop_name"),
            cancel_callback=lambda c: self._cb_loops_panel(c, pid),
        )

    async def _handle_loop_name(self, call, data: str, pid: str):
        self._bind_origin(call)
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        loop_name = (data or "").strip().lower()
        error = self._check_loop_name(project, loop_name)
        if error:
            return await self._prompt(
                call,
                error + "\n\n" + self._t("ask_loop_name"),
                self._handle_loop_name,
                args=(pid,),
                placeholder=self._t("placeholder_loop_name"),
                cancel_callback=lambda c: self._cb_loops_panel(c, pid),
            )

        state = self._get_state(call)
        state.update({"pid": pid, "loop_name": loop_name, "entity_kind": "loop"})
        self._set_state(call, **state)
        await self._prompt(
            call,
            self._t("ask_loop_interval", name=loop_name),
            self._handle_loop_interval,
            args=(pid, loop_name, False),
            placeholder="60",
            cancel_callback=lambda c: self._cb_loops_panel(c, pid),
        )

    async def _handle_loop_interval(self, call, data: str, pid: str, loop_name: str, editing: bool):
        self._bind_origin(call)
        raw = (data or "").strip()
        try:
            interval = int(raw)
            if interval <= 0:
                raise ValueError
        except Exception:
            return await self._prompt(
                call,
                self._t("invalid_loop_interval") + "\n\n" + self._t("ask_loop_interval", name=loop_name),
                self._handle_loop_interval,
                args=(pid, loop_name, editing),
                placeholder=raw or "60",
                cancel_callback=(lambda c: self._cb_loop_detail(c, pid, loop_name)) if editing else (lambda c: self._cb_loops_panel(c, pid)),
            )

        project = self._get_project(pid)
        if editing:
            if not project or loop_name not in project.get("loops", {}):
                return await self._edit_ui(call, self._t("project_not_found"))
            project["loops"][loop_name]["interval"] = interval
            self._save_project(pid, project)
            await self._edit_ui(call, self._t("loop_updated", name=loop_name))
            await asyncio.sleep(1)
            return await self._cb_loop_detail(call, pid, loop_name)

        state = self._get_state(call)
        state["interval"] = interval
        self._set_state(call, **state)
        await self._show_loop_body_mode(call, pid)

    async def _show_loop_body_mode(self, call, pid: str):
        self._bind_origin(call)
        state = self._get_state(call)
        loop_name = state.get("loop_name", "?")
        cancel_callback = (
            (lambda c, project_id=pid, loop=loop_name: self._cb_loop_detail(c, project_id, loop))
            if state.get("editing_loop")
            else (lambda c, project_id=pid: self._cb_loops_panel(c, project_id))
        )
        await self._edit_ui(
            call,
            self._t("loop_body_mode", name=loop_name, interval=int(state.get("interval") or 60)),
            reply_markup=[
                [
                    {
                        "text": self._t("ai_body_btn"),
                        "input": self._t("ask_loop_ai_prompt", name=loop_name),
                        "handler": self._handle_loop_ai_body,
                        "args": (pid,),
                    }
                ],
                [
                    {
                        "text": self._t("manual_body_btn"),
                        "input": self._t("ask_loop_body", name=loop_name),
                        "handler": self._handle_loop_manual_body,
                        "args": (pid,),
                    }
                ],
                [
                    {"text": self._t("stub_body_btn"), "callback": lambda c, project_id=pid: self._save_loop(c, project_id, "pass")},
                    {"text": self._t("cancel_btn"), "callback": cancel_callback},
                ],
            ],
        )

    async def _handle_loop_ai_body(self, call, data: str, pid: str):
        self._bind_origin(call)
        prompt = (data or "").strip()
        state = self._get_state(call)
        loop_name = state.get("loop_name", "loop")
        if not prompt:
            return await self._show_loop_body_mode(call, pid)

        settings, settings_error = self._ai_settings()
        if settings_error:
            await self._edit_ui(call, settings_error)
            await asyncio.sleep(2)
            return await self._show_loop_body_mode(call, pid)

        await self._edit_ui(call, self._t("ai_generating", cmd=loop_name))
        project = self._get_project(pid)
        generated = await _ai_generate(self._build_ai_request(project or {}, state, prompt), settings)
        if generated.startswith("# AI Error"):
            await self._edit_ui(call, self._t("ai_error", err=generated))
            await asyncio.sleep(2)
            return await self._show_loop_body_mode(call, pid)
        body_error = _validate_body_syntax(generated)
        if body_error:
            await self._edit_ui(call, self._t("invalid_code", err=body_error))
            await asyncio.sleep(2)
            return await self._show_loop_body_mode(call, pid)

        await self._edit_ui(call, self._t("ai_done", cmd=loop_name))
        await asyncio.sleep(1)
        await self._save_loop(call, pid, generated)

    async def _handle_loop_manual_body(self, call, data: str, pid: str):
        self._bind_origin(call)
        body = (data or "").strip() or "pass"
        body_error = _validate_body_syntax(body)
        if body_error:
            await self._edit_ui(call, self._t("invalid_code", err=body_error))
            await asyncio.sleep(2)
            return await self._show_loop_body_mode(call, pid)

        await self._save_loop(call, pid, body)

    async def _save_loop(self, call, pid: str, body: str):
        state = self._get_state(call)
        project = self._get_project(pid)
        if not project:
            self._clear_state(call)
            return await self._edit_ui(call, self._t("project_not_found"))

        loop_name = state.get("loop_name")
        original_loop_name = state.get("original_loop_name")
        existing = bool(original_loop_name and original_loop_name in project.get("loops", {}))
        error = self._check_loop_name(project, loop_name, skip_name=original_loop_name if existing else None)
        if error:
            self._clear_state(call)
            return await self._edit_ui(call, error)

        existing_loop = project.get("loops", {}).get(original_loop_name or loop_name, {})
        project.setdefault("loops", {})
        project["loops"][loop_name] = {
            "body": body,
            "interval": int(state.get("interval") or existing_loop.get("interval") or 60),
        }
        if existing and original_loop_name != loop_name:
            project["loops"].pop(original_loop_name, None)
        self._save_project(pid, project)
        self._clear_state(call)

        await self._edit_ui(call, self._t("loop_updated" if existing else "loop_added", name=loop_name))
        await asyncio.sleep(1)
        if existing:
            await self._cb_loop_detail(call, pid, loop_name)
        else:
            await self._cb_loops_panel(call, pid)

    async def _cb_loop_detail(self, call, pid: str, loop_name: str):
        project = self._get_project(pid)
        if not project or loop_name not in project.get("loops", {}):
            return await self._edit_ui(call, self._t("project_not_found"))

        await self._edit_ui(
            call,
            self._loop_detail_text(project, loop_name),
            reply_markup=[
                [
                    {"text": self._t("interval_btn"), "callback": lambda c: self._cb_edit_loop_interval(c, pid, loop_name)},
                    {"text": self._t("edit_body_btn"), "callback": lambda c: self._cb_edit_loop_body(c, pid, loop_name)},
                ],
                [{"text": self._t("delete_btn"), "callback": lambda c: self._cb_delete_loop(c, pid, loop_name)}],
                [{"text": self._t("back_btn"), "callback": lambda c: self._cb_loops_panel(c, pid)}],
            ],
        )

    async def _cb_edit_loop_interval(self, call, pid: str, loop_name: str):
        project = self._get_project(pid)
        if not project or loop_name not in project.get("loops", {}):
            return await self._edit_ui(call, self._t("project_not_found"))

        await self._prompt(
            call,
            self._t("ask_loop_interval", name=loop_name),
            self._handle_loop_interval,
            args=(pid, loop_name, True),
            placeholder=str(project["loops"][loop_name].get("interval") or 60),
            cancel_callback=lambda c: self._cb_loop_detail(c, pid, loop_name),
        )

    async def _cb_edit_loop_body(self, call, pid: str, loop_name: str):
        project = self._get_project(pid)
        if not project or loop_name not in project.get("loops", {}):
            return await self._edit_ui(call, self._t("project_not_found"))

        self._clear_state(call)
        self._set_state(
            call,
            flow="edit_loop_body",
            pid=pid,
            loop_name=loop_name,
            original_loop_name=loop_name,
            entity_kind="loop",
            editing_loop=True,
            interval=int(project["loops"][loop_name].get("interval") or 60),
        )
        await self._show_loop_body_mode(call, pid)

    async def _cb_delete_loop(self, call, pid: str, loop_name: str):
        project = self._get_project(pid)
        if not project or loop_name not in project.get("loops", {}):
            return await self._edit_ui(call, self._t("project_not_found"))

        project["loops"].pop(loop_name, None)
        self._save_project(pid, project)
        await self._edit_ui(call, self._t("loop_deleted", name=loop_name))
        await asyncio.sleep(1)
        await self._cb_loops_panel(call, pid)

    async def _cb_configs_panel(self, call, pid: str):
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        buttons = [[{"text": self._t("add_cfg_btn"), "callback": lambda c: self._cb_add_config(c, pid)}]]
        for cfg in project["configs"]:
            buttons.append(
                [{"text": self._config_button_text(cfg["key"]), "callback": lambda c, key=cfg['key']: self._cb_config_detail(c, pid, key)}]
            )
        buttons.append([{"text": self._t("back_btn"), "callback": lambda c: self._show_project_panel(c, pid)}])
        await self._edit_ui(call, self._configs_panel_text(project), reply_markup=buttons)

    async def _cb_config_detail(self, call, pid: str, key: str):
        project = self._get_project(pid)
        cfg = self._get_config_entry(project, key) if project else None
        if not project or not cfg:
            return await self._edit_ui(call, self._t("project_not_found"))

        await self._edit_ui(
            call,
            self._config_detail_text(cfg),
            reply_markup=[
                [
                    {"text": self._t("edit_default_btn"), "callback": lambda c: self._cb_edit_config_default(c, pid, key)},
                    {"text": self._t("edit_validator_btn"), "callback": lambda c: self._cb_edit_config_validator(c, pid, key)},
                ],
                [
                    {"text": self._t("delete_btn"), "callback": lambda c: self._cb_delete_config(c, pid, key)},
                    {"text": self._t("back_btn"), "callback": lambda c: self._cb_configs_panel(c, pid)},
                ],
            ],
        )

    async def _cb_edit_config_default(self, call, pid: str, key: str):
        project = self._get_project(pid)
        cfg = self._get_config_entry(project, key) if project else None
        if not project or not cfg:
            return await self._edit_ui(call, self._t("project_not_found"))

        self._clear_state(call)
        return await self._start_config_default_step(
            call,
            pid,
            key,
            cfg.get("validator", "String"),
            cfg.get("validator_args") or {},
            edit=True,
            current_default=cfg.get("default", ""),
        )

    async def _handle_edit_config_default(self, call, data: str, pid: str, key: str):
        project = self._get_project(pid)
        cfg = self._get_config_entry(project, key) if project else None
        if not project or not cfg:
            return await self._edit_ui(call, self._t("project_not_found"))
        return await self._handle_config_default_input(
            call,
            data,
            pid,
            key,
            cfg.get("validator", "String"),
            True,
        )

    def _config_default_prompt_text(self, validator: str, key: str) -> str:
        if validator == "Boolean":
            return self._t("ask_cfg_boolean_default", key=key)
        if validator == "Choice":
            return self._t("ask_cfg_choice_default", key=key)
        if validator == "MultiChoice":
            return self._t("ask_cfg_multichoice_default", key=key)
        return self._t("ask_cfg_default")

    async def _start_config_default_step(
        self,
        call,
        pid: str,
        key: str,
        validator: str,
        validator_args: dict,
        *,
        edit: bool,
        current_default: typing.Any = "",
    ):
        selected = current_default
        if validator == "MultiChoice":
            if isinstance(current_default, (list, tuple, set)):
                selected = list(current_default)
            elif current_default == "":
                selected = []
            else:
                selected = [current_default]

        self._set_state(
            call,
            flow="edit_config_default" if edit else "add_config_default",
            pid=pid,
            key=key,
            validator=validator,
            validator_args=validator_args or {},
            editing_config=edit,
            pending_default=selected,
        )

        cancel_callback = (
            (lambda c: self._cb_config_detail(c, pid, key))
            if edit
            else (lambda c: self._cb_configs_panel(c, pid))
        )

        if validator == "Boolean":
            return await self._show_boolean_default_picker(call, pid, key, edit)
        if validator == "Choice":
            return await self._show_choice_default_picker(call, pid, key, edit)
        if validator == "MultiChoice":
            return await self._show_multichoice_default_picker(call, pid, key, edit)

        return await self._prompt(
            call,
            self._config_default_prompt_text(validator, key),
            self._handle_config_default_input,
            args=(pid, key, validator, edit),
            placeholder=str(current_default) if current_default != "" else self._t("placeholder_cfg_default"),
            skip_handler=lambda c, project_id=pid, cfg_key=key, v=validator, is_edit=edit: self._handle_config_default_input(
                c, "", project_id, cfg_key, v, is_edit
            ),
            cancel_callback=cancel_callback,
        )

    async def _show_boolean_default_picker(self, call, pid: str, key: str, edit: bool):
        await self._edit_ui(
            call,
            self._config_default_prompt_text("Boolean", key),
            reply_markup=[
                [
                    {"text": self._t("true_btn"), "callback": lambda c: self._finalize_config_entry(c, pid, key, "Boolean", True, edit=edit)},
                    {"text": self._t("false_btn"), "callback": lambda c: self._finalize_config_entry(c, pid, key, "Boolean", False, edit=edit)},
                ],
                [
                    {"text": self._t("back_btn"), "callback": (lambda c: self._cb_config_detail(c, pid, key)) if edit else (lambda c: self._cb_configs_panel(c, pid))},
                ],
            ],
        )

    async def _show_choice_default_picker(self, call, pid: str, key: str, edit: bool):
        state = self._get_state(call)
        values = list((state.get("validator_args") or {}).get("possible_values") or [])
        buttons = []
        row = []
        for value in values:
            row.append(
                {
                    "text": str(value),
                    "callback": lambda c, selected=value: self._finalize_config_entry(
                        c,
                        pid,
                        key,
                        "Choice",
                        selected,
                        edit=edit,
                    ),
                }
            )
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([{"text": self._t("back_btn"), "callback": (lambda c: self._cb_config_detail(c, pid, key)) if edit else (lambda c: self._cb_configs_panel(c, pid))}])
        await self._edit_ui(call, self._config_default_prompt_text("Choice", key), reply_markup=buttons)

    async def _show_multichoice_default_picker(self, call, pid: str, key: str, edit: bool):
        state = self._get_state(call)
        values = list((state.get("validator_args") or {}).get("possible_values") or [])
        selected = set(state.get("pending_default") or [])
        buttons = []
        row = []
        for value in values:
            row.append(
                {
                    "text": "{} {}".format("✅" if value in selected else "▫️", value),
                    "callback": lambda c, current=value: self._toggle_multichoice_default(c, pid, key, current, edit),
                }
            )
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append(
            [
                {"text": self._t("save_btn"), "callback": lambda c: self._save_multichoice_default(c, pid, key, edit)},
                {"text": self._t("clear_btn"), "callback": lambda c: self._clear_multichoice_default(c, pid, key, edit)},
            ]
        )
        buttons.append(
            [
                {"text": self._t("back_btn"), "callback": (lambda c: self._cb_config_detail(c, pid, key)) if edit else (lambda c: self._cb_configs_panel(c, pid))},
            ]
        )
        text = "{}\n\n{}".format(
            self._config_default_prompt_text("MultiChoice", key),
            self._t("selected_values_line", values=_escape_html(self._config_default_label(list(selected)))),
        )
        await self._edit_ui(call, text, reply_markup=buttons)

    async def _toggle_multichoice_default(self, call, pid: str, key: str, value: typing.Any, edit: bool):
        state = self._get_state(call)
        selected = list(state.get("pending_default") or [])
        if value in selected:
            selected = [item for item in selected if item != value]
        else:
            selected.append(value)
        self._set_state(call, **{**state, "pending_default": selected})
        return await self._show_multichoice_default_picker(call, pid, key, edit)

    async def _clear_multichoice_default(self, call, pid: str, key: str, edit: bool):
        state = self._get_state(call)
        self._set_state(call, **{**state, "pending_default": []})
        return await self._show_multichoice_default_picker(call, pid, key, edit)

    async def _save_multichoice_default(self, call, pid: str, key: str, edit: bool):
        state = self._get_state(call)
        return await self._finalize_config_entry(
            call,
            pid,
            key,
            "MultiChoice",
            list(state.get("pending_default") or []),
            edit=edit,
        )

    async def _prompt_validator_meta(self, call, pid: str, key: str, validator: str, *, edit: bool):
        self._set_state(call, flow="edit_config_validator_meta" if edit else "config_validator_meta", pid=pid, key=key, validator=validator, editing_config=edit)
        await self._prompt(
            call,
            self._validator_followup_prompt(validator, key),
            self._handle_validator_meta,
            args=(pid, key, validator, edit),
            placeholder=self._t("placeholder_cfg_default"),
            cancel_callback=lambda c: self._cb_config_detail(c, pid, key) if edit else self._cb_configs_panel(c, pid),
        )

    async def _finalize_config_entry(
        self,
        call,
        pid: str,
        key: str,
        validator: str,
        default: typing.Any,
        *,
        edit: bool,
    ):
        project = self._get_project(pid)
        if not project:
            self._clear_state(call)
            return await self._edit_ui(call, self._t("project_not_found"))

        state = self._get_state(call)
        validator_args = state.get("validator_args") or {}
        parsed_default, error = _parse_default_for_validator(validator, default, validator_args)
        if error:
            return await self._edit_ui(call, self._t("validator_default_invalid", validator=validator))

        if edit:
            cfg = self._get_config_entry(project, key)
            if not cfg:
                self._clear_state(call)
                return await self._edit_ui(call, self._t("project_not_found"))
            cfg["validator"] = validator
            cfg["validator_args"] = validator_args or {}
            cfg["default"] = parsed_default
            self._save_project(pid, project)
            self._clear_state(call)
            await self._edit_ui(call, self._t("cfg_updated", key=key))
            await asyncio.sleep(1)
            return await self._cb_config_detail(call, pid, key)

        error = self._check_config_key(project, key)
        if error:
            self._clear_state(call)
            return await self._edit_ui(call, error)

        project["configs"].append(
            {
                "key": key,
                "default": parsed_default,
                "validator": validator,
                "validator_args": validator_args or {},
            }
        )
        project["strings"]["en"]["{}_doc".format(key)] = "Config: {}".format(key)
        self._save_project(pid, project)
        self._clear_state(call)
        await self._edit_ui(call, self._t("cfg_added", key=key))
        await asyncio.sleep(1)
        return await self._cb_configs_panel(call, pid)

    async def _handle_config_default_input(self, call, data: str, pid: str, key: str, validator: str, edit: bool):
        self._bind_origin(call)
        state = self._get_state(call)
        parsed_default, error = _parse_default_for_validator(
            validator,
            (data or "").strip(),
            state.get("validator_args") or {},
        )
        if error:
            return await self._prompt(
                call,
                self._t("validator_default_invalid", validator=validator) + "\n\n" + self._config_default_prompt_text(validator, key),
                self._handle_config_default_input,
                args=(pid, key, validator, edit),
                placeholder=self._t("placeholder_cfg_default"),
                cancel_callback=lambda c: self._cb_config_detail(c, pid, key) if edit else self._cb_configs_panel(c, pid),
            )

        return await self._finalize_config_entry(call, pid, key, validator, parsed_default, edit=edit)

    async def _handle_validator_meta(self, call, data: str, pid: str, key: str, validator: str, edit: bool):
        self._bind_origin(call)
        validator_args, error = _parse_validator_meta(validator, data)
        if error:
            return await self._prompt(
                call,
                self._t("invalid_validator_meta", validator=validator) + "\n\n" + self._validator_followup_prompt(validator, key),
                self._handle_validator_meta,
                args=(pid, key, validator, edit),
                placeholder=self._t("placeholder_cfg_default"),
                cancel_callback=lambda c: self._cb_config_detail(c, pid, key) if edit else self._cb_configs_panel(c, pid),
            )

        current_default = ""
        if edit:
            project = self._get_project(pid)
            cfg = self._get_config_entry(project, key) if project else None
            current_default = cfg.get("default", "") if cfg else ""
        return await self._start_config_default_step(
            call,
            pid,
            key,
            validator,
            validator_args or {},
            edit=edit,
            current_default=current_default,
        )

    async def _cb_edit_config_validator(self, call, pid: str, key: str):
        project = self._get_project(pid)
        cfg = self._get_config_entry(project, key) if project else None
        if not project or not cfg:
            return await self._edit_ui(call, self._t("project_not_found"))

        buttons = []
        row = []
        for validator in VALIDATORS_LIST:
            row.append(
                {
                    "text": validator,
                    "callback": lambda c, value=validator: self._handle_edit_config_validator(c, value, pid, key),
                }
            )
            if len(row) == 3:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([{"text": self._t("back_btn"), "callback": lambda c: self._cb_config_detail(c, pid, key)}])
        await self._edit_ui(call, self._t("ask_cfg_validator", key=key), reply_markup=buttons)

    async def _handle_edit_config_validator(self, call, validator: str, pid: str, key: str):
        project = self._get_project(pid)
        cfg = self._get_config_entry(project, key) if project else None
        current_default = cfg.get("default", "") if cfg else ""
        self._set_state(call, pid=pid, key=key, validator=validator, validator_args={}, editing_config=True)
        if validator in VALIDATOR_REQUIRED_META:
            return await self._prompt_validator_meta(call, pid, key, validator, edit=True)
        return await self._start_config_default_step(
            call,
            pid,
            key,
            validator,
            {},
            edit=True,
            current_default=current_default,
        )

    async def _cb_delete_config(self, call, pid: str, key: str):
        project = self._get_project(pid)
        cfg = self._get_config_entry(project, key) if project else None
        if not project or not cfg:
            return await self._edit_ui(call, self._t("project_not_found"))

        project["configs"] = [item for item in project["configs"] if item.get("key") != key]
        project["strings"].setdefault("en", {})
        project["strings"]["en"].pop(f"{key}_doc", None)
        self._save_project(pid, project)
        await self._edit_ui(call, self._t("cfg_deleted", key=key))
        await asyncio.sleep(1)
        await self._cb_configs_panel(call, pid)

    async def _cb_add_command(self, call, pid: str):
        if not self._get_project(pid):
            return await self._edit_ui(call, self._t("project_not_found"))

        self._clear_state(call)
        self._set_state(call, flow="add_command", pid=pid, entity_kind="command")
        await self._prompt(
            call,
            self._t("ask_cmd_name"),
            self._handle_cmd_name,
            args=(pid,),
            placeholder=self._t("placeholder_cmd_name"),
            cancel_callback=lambda c: self._show_project_panel(c, pid),
        )

    async def _handle_cmd_name(
        self,
        call,
        data: str,
        pid: str,
    ):
        self._bind_origin(call)
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        cmd_name = (data or "").strip().lower()
        error = self._check_command_name(project, cmd_name)
        if error:
            return await self._prompt(
                call,
                error + "\n\n" + self._t("ask_cmd_name"),
                self._handle_cmd_name,
                args=(pid,),
                placeholder=self._t("placeholder_cmd_name"),
                cancel_callback=lambda c: self._show_project_panel(c, pid),
            )

        state = self._get_state(call)
        state.update({"pid": pid, "cmd_name": cmd_name, "entity_kind": "command"})
        self._set_state(call, **state)
        await self._prompt(
            call,
            self._t("ask_cmd_doc_en"),
            self._handle_cmd_doc_en,
            args=(pid,),
            placeholder=self._t("placeholder_cmd_desc_en"),
            skip_handler=lambda c, project_id=pid: self._handle_cmd_doc_en(c, "", project_id),
            cancel_callback=lambda c: self._show_project_panel(c, pid),
        )

    async def _handle_cmd_doc_en(
        self,
        call,
        data: str,
        pid: str,
    ):
        self._bind_origin(call)
        state = self._get_state(call)
        state["doc_en"] = (data or "").strip() or state.get("cmd_name", "")
        self._set_state(call, **state)
        await self._prompt(
            call,
            self._t("ask_cmd_doc_ru"),
            self._handle_cmd_doc_ru,
            args=(pid,),
            placeholder=self._t("placeholder_cmd_desc_ru"),
            skip_handler=lambda c, project_id=pid: self._handle_cmd_doc_ru(c, "", project_id),
            cancel_callback=lambda c: self._show_project_panel(c, pid),
        )

    async def _handle_cmd_doc_ru(
        self,
        call,
        data: str,
        pid: str,
    ):
        self._bind_origin(call)
        state = self._get_state(call)
        state["doc_ru"] = (data or "").strip()
        self._set_state(call, **state)
        await self._show_body_mode(call, pid)

    async def _show_body_mode(self, call, pid: str):
        self._bind_origin(call)
        state = self._get_state(call)
        cmd_name = state.get("cmd_name", "?")
        cancel_callback = (
            (lambda c, project_id=pid, command=cmd_name: self._cb_command_detail(c, project_id, command))
            if state.get("editing_command")
            else (lambda c, project_id=pid: self._cb_commands_panel(c, project_id))
        )
        await self._edit_ui(
            call,
            self._t("body_mode", cmd=cmd_name),
            reply_markup=[
                [
                    {
                        "text": self._t("ai_body_btn"),
                        "input": self._t("ask_cmd_ai_prompt", cmd=cmd_name),
                        "handler": self._handle_cmd_ai_body,
                        "args": (pid,),
                    }
                ],
                [
                    {
                        "text": self._t("manual_body_btn"),
                        "input": self._t("ask_cmd_body", cmd=cmd_name),
                        "handler": self._handle_cmd_manual_body,
                        "args": (pid,),
                    },
                    {
                        "text": self._t("template_body_btn"),
                        "callback": lambda c, project_id=pid: self._cb_template_panel(c, project_id),
                    }
                ],
                [
                    {
                        "text": self._t("stub_body_btn"),
                        "callback": lambda c, project_id=pid: self._save_command(c, project_id, "pass"),
                    },
                    {
                        "text": self._t("cancel_btn"),
                        "callback": cancel_callback,
                    },
                ],
            ],
        )

    async def _cb_template_panel(self, call, pid: str):
        state = self._get_state(call)
        cmd_name = state.get("cmd_name", "?")
        buttons = []
        row = []
        for template_key, meta in COMMAND_TEMPLATES.items():
            row.append(
                {
                    "text": meta["title"],
                    "callback": lambda c, value=template_key: self._cb_apply_template(c, pid, value),
                }
            )
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([{"text": self._t("back_btn"), "callback": lambda c: self._show_body_mode(c, pid)}])
        await self._edit_ui(call, self._t("template_panel", cmd=cmd_name), reply_markup=buttons)

    async def _cb_apply_template(self, call, pid: str, template_key: str):
        meta = COMMAND_TEMPLATES.get(template_key)
        state = self._get_state(call)
        cmd_name = state.get("cmd_name", "?")
        if not meta:
            return await self._cb_template_panel(call, pid)
        if meta.get("needs_input"):
            prompt_key = meta.get("prompt_key")
            return await self._prompt(
                call,
                self._t(prompt_key, cmd=cmd_name),
                self._handle_template_input,
                args=(pid, template_key),
                placeholder=self._t("placeholder_cfg_default"),
                cancel_callback=lambda c: self._cb_template_panel(c, pid),
            )
        return await self._apply_template(call, pid, template_key, "")

    async def _handle_template_input(self, call, data: str, pid: str, template_key: str):
        self._bind_origin(call)
        return await self._apply_template(call, pid, template_key, data or "")

    async def _apply_template(self, call, pid: str, template_key: str, payload: str):
        state = self._get_state(call)
        cmd_name = state.get("cmd_name", "?")
        rendered = _template_body(template_key, payload, cmd_name=cmd_name)
        if not rendered:
            await self._edit_ui(call, self._t("invalid_code", err=self._t("template_payload_invalid")))
            await asyncio.sleep(2)
            return await self._cb_template_panel(call, pid)
        body, resources = rendered
        body_error = _validate_body_syntax(body)
        if body_error:
            await self._edit_ui(call, self._t("invalid_code", err=body_error))
            await asyncio.sleep(2)
            return await self._cb_template_panel(call, pid)
        await self._edit_ui(call, self._t("template_saved", cmd=cmd_name))
        await asyncio.sleep(1)
        await self._save_command(call, pid, body, resources=resources)

    async def _handle_cmd_ai_body(
        self,
        call,
        data: str,
        pid: str,
    ):
        self._bind_origin(call)
        prompt = (data or "").strip()
        state = self._get_state(call)
        cmd_name = state.get("cmd_name", "command")
        if not prompt:
            return await self._show_body_mode(call, pid)

        settings, settings_error = self._ai_settings()
        if settings_error:
            await self._edit_ui(call, settings_error)
            await asyncio.sleep(2)
            return await self._show_body_mode(call, pid)

        await self._edit_ui(call, self._t("ai_generating", cmd=cmd_name))
        project = self._get_project(pid)
        generated = await _ai_generate(self._build_ai_request(project or {}, state, prompt), settings)
        if generated.startswith("# AI Error"):
            await self._edit_ui(call, self._t("ai_error", err=generated))
            await asyncio.sleep(2)
            return await self._show_body_mode(call, pid)
        if not generated.strip():
            await self._edit_ui(call, self._t("ai_error", err=self._t("empty_code")))
            await asyncio.sleep(2)
            return await self._show_body_mode(call, pid)

        ai_contract_error = _validate_ai_body_contracts(generated)
        if ai_contract_error:
            await self._edit_ui(call, self._t("invalid_code", err=ai_contract_error))
            await asyncio.sleep(2)
            return await self._show_body_mode(call, pid)

        body_error = _validate_body_syntax(generated)
        if body_error:
            await self._edit_ui(call, self._t("invalid_code", err=body_error))
            await asyncio.sleep(2)
            return await self._show_body_mode(call, pid)

        await self._edit_ui(call, self._t("ai_done", cmd=cmd_name))
        await asyncio.sleep(1)
        await self._save_command(call, pid, generated)

    async def _handle_cmd_manual_body(
        self,
        call,
        data: str,
        pid: str,
    ):
        self._bind_origin(call)
        body = (data or "").strip() or "pass"
        body_error = _validate_body_syntax(body)
        if body_error:
            await self._edit_ui(call, self._t("invalid_code", err=body_error))
            await asyncio.sleep(2)
            return await self._show_body_mode(call, pid)

        await self._save_command(call, pid, body)

    async def _save_command(
        self,
        call,
        pid: str,
        body: str,
        *,
        resources: typing.Optional[dict] = None,
    ):
        state = self._get_state(call)
        project = self._get_project(pid)
        if not project:
            self._clear_state(call)
            return await self._edit_ui(call, self._t("project_not_found"))

        cmd_name = state.get("cmd_name")
        original_cmd_name = state.get("original_cmd_name")
        existing = bool(original_cmd_name and original_cmd_name in project["commands"])
        error = None
        if not existing or original_cmd_name != cmd_name:
            error = self._check_command_name(project, cmd_name)
        if error:
            self._clear_state(call)
            return await self._edit_ui(call, error)

        existing_command = project["commands"].get(original_cmd_name or cmd_name, {}) if isinstance(project.get("commands"), dict) else {}
        project["commands"][cmd_name] = {
            "body": body,
            "tags": dict(existing_command.get("tags", {})) if isinstance(existing_command, dict) else {},
        }
        project.setdefault("command_docs", {})
        project["command_docs"].setdefault(cmd_name, {})
        project["command_docs"][cmd_name]["en"] = state.get("doc_en", cmd_name)
        if state.get("doc_ru"):
            project["command_docs"][cmd_name]["ru"] = state["doc_ru"]
        else:
            project["command_docs"][cmd_name].pop("ru", None)
        project.setdefault("command_resources", {})
        if resources is not None:
            cmd_resources = project["command_resources"].setdefault(cmd_name, {})
            cmd_resources["en"] = {
                "texts": dict((resources or {}).get("texts") or {}),
                "lists": dict((resources or {}).get("lists") or {}),
                "values": dict((resources or {}).get("values") or {}),
            }
        self._save_project(pid, project)
        self._clear_state(call)

        await self._edit_ui(call, self._t("cmd_updated" if existing else "cmd_added", cmd=cmd_name))
        await asyncio.sleep(1)
        if existing:
            await self._cb_command_detail(call, pid, cmd_name)
        else:
            await self._cb_commands_panel(call, pid)

    async def _cb_add_config(self, call, pid: str):
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        self._clear_state(call)
        self._set_state(call, flow="add_config", pid=pid)
        await self._prompt(
            call,
            self._t("ask_cfg_key"),
            self._handle_cfg_key,
            args=(pid,),
            placeholder=self._t("placeholder_cfg_key"),
            cancel_callback=lambda c: self._show_project_panel(c, pid),
        )

    async def _handle_cfg_key(
        self,
        call,
        data: str,
        pid: str,
    ):
        self._bind_origin(call)
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        key = (data or "").strip()
        error = self._check_config_key(project, key)
        if error:
            return await self._prompt(
                call,
                error + "\n\n" + self._t("ask_cfg_key"),
                self._handle_cfg_key,
                args=(pid,),
                placeholder=self._t("placeholder_cfg_key"),
                cancel_callback=lambda c: self._show_project_panel(c, pid),
            )

        state = self._get_state(call)
        state["key"] = key
        self._set_state(call, **state)
        await self._handle_cfg_default(call, "", pid)

    async def _handle_cfg_default(
        self,
        call,
        data: str,
        pid: str,
    ):
        self._bind_origin(call)
        state = self._get_state(call)

        buttons = []
        row = []
        for validator in VALIDATORS_LIST:
            row.append(
                {
                    "text": validator,
                    "callback": lambda c, v=validator, project_id=pid: self._handle_cfg_validator(c, v, project_id),
                }
            )
            if len(row) == 3:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append(
            [
                {
                    "text": self._t("cancel_btn"),
                    "callback": lambda c: self._show_project_panel(c, pid),
                }
            ]
        )
        await self._edit_ui(call, self._t("ask_cfg_validator", key=state["key"]), reply_markup=buttons)

    async def _handle_cfg_validator(self, call, validator: str, pid: str):
        state = self._get_state(call)
        key = state.get("key")
        self._set_state(call, **{**state, "validator": validator, "validator_args": {}, "editing_config": False})
        if validator in VALIDATOR_REQUIRED_META:
            return await self._prompt_validator_meta(call, pid, key, validator, edit=False)
        return await self._start_config_default_step(call, pid, key, validator, {}, edit=False, current_default="")

    async def _cb_lang_panel(self, call, pid: str):
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        buttons = []
        row = []
        for lang in _project_languages(project):
            row.append(
                {
                    "text": self._lang_button_text(lang, project),
                    "callback": lambda c, l=lang: self._cb_edit_lang(c, pid, l),
                }
            )
            if len(row) == 3:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([{"text": self._t("add_lang_btn"), "callback": lambda c: self._cb_add_custom_lang(c, pid)}])
        buttons.append([{"text": self._t("back_btn"), "callback": lambda c: self._show_project_panel(c, pid)}])
        await self._edit_ui(call, self._t("lang_panel"), reply_markup=buttons)

    async def _cb_add_custom_lang(self, call, pid: str):
        self._clear_state(call)
        self._set_state(call, flow="add_custom_lang", pid=pid)
        await self._prompt(
            call,
            self._t("ask_lang_code"),
            self._handle_add_custom_lang,
            args=(pid,),
            placeholder="es",
            cancel_callback=lambda c: self._cb_lang_panel(c, pid),
        )

    async def _handle_add_custom_lang(self, call, data: str, pid: str):
        self._bind_origin(call)
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        lang = _normalize_lang_code(data)
        if not lang:
            return await self._prompt(
                call,
                self._t("invalid_lang_code") + "\n\n" + self._t("ask_lang_code"),
                self._handle_add_custom_lang,
                args=(pid,),
                placeholder="es",
                cancel_callback=lambda c: self._cb_lang_panel(c, pid),
            )

        extra_langs = list(project.get("extra_langs") or [])
        if lang not in REQUIRED_LANGS and lang not in SUPPORTED_LANGS and lang not in extra_langs:
            extra_langs.append(lang)
        project["extra_langs"] = extra_langs
        project["strings"].setdefault(lang, {})
        self._save_project(pid, project)
        self._clear_state(call)
        await self._edit_ui(call, self._t("lang_saved", lang=LANG_LABELS.get(lang, lang)))
        await asyncio.sleep(1)
        await self._cb_lang_panel(call, pid)

    async def _cb_edit_lang(self, call, pid: str, lang: str):
        await self._prompt(
            call,
            self._t("ask_lang_cls", lang=LANG_LABELS.get(lang, lang)),
            self._handle_lang_cls,
            args=(pid, lang),
            placeholder=self._t("placeholder_lang_desc"),
            skip_handler=lambda c, project_id=pid: self._cb_lang_panel(c, project_id),
            cancel_callback=lambda c: self._cb_lang_panel(c, pid),
        )

    async def _handle_lang_cls(
        self,
        call,
        data: str,
        pid: str,
        lang: str,
    ):
        self._bind_origin(call)
        value = (data or "").strip()
        if not value:
            return await self._cb_lang_panel(call, pid)

        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        project["strings"].setdefault(lang, {})
        project["strings"][lang]["_cls_doc"] = value
        self._save_project(pid, project)
        await self._edit_ui(call, self._t("lang_saved", lang=LANG_LABELS.get(lang, lang)))
        await asyncio.sleep(1)
        await self._cb_lang_panel(call, pid)

    async def _cb_compile(self, call, pid: str):
        self._bind_origin(call)
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        conflict = self._validate_project_conflicts(project)
        if conflict:
            return await self._edit_ui(
                call,
                conflict,
                reply_markup=[[{"text": self._t("back_btn"), "callback": lambda c: self._show_project_panel(c, pid)}]],
            )

        for cfg in project.get("configs", []):
            cfg_error = _validate_config_validator(cfg)
            if cfg_error:
                return await self._edit_ui(
                    call,
                    self._t("compile_error", err=cfg_error),
                    reply_markup=[[{"text": self._t("back_btn"), "callback": lambda c: self._cb_configs_panel(c, pid)}]],
                )

        await self._edit_ui(call, self._t("compile_start", name=project["meta"]["name"]))
        code, err, _resolved_imports = _build_module_code_with_import_repair(project)
        if err:
            return await self._edit_ui(
                call,
                self._t("compile_error", err=err),
                reply_markup=[[{"text": self._t("back_btn"), "callback": lambda c: self._show_project_panel(c, pid)}]],
            )

        file_obj = io.BytesIO(code.encode("utf-8"))
        file_obj.name = "{}.py".format(
            project["meta"].get("class_name", project["meta"]["name"])
        )
        await utils.answer_file(
            call,
            file_obj,
            caption=self._t(
                "compiled_caption",
                name=project["meta"]["name"],
                cmds=len(project["commands"]),
                watchers=len(project.get("watchers", {})),
                loops=len(project.get("loops", {})),
                cfgs=len(project["configs"]),
            ),
            parse_mode="html",
        )
        with contextlib.suppress(Exception):
            await call.delete()

    async def _cb_delete_confirm(self, call, pid: str):
        project = self._get_project(pid)
        if not project:
            return await self._edit_ui(call, self._t("project_not_found"))

        await self._edit_ui(
            call,
            self._t("delete_confirm", name=project["meta"]["name"]),
            reply_markup=[
                [
                    {"text": self._t("delete_btn"), "callback": lambda c: self._cb_do_delete(c, pid, project["meta"]["name"])},
                    {"text": self._t("cancel_btn"), "callback": lambda c: self._show_project_panel(c, pid)},
                ]
            ],
        )

    async def _cb_do_delete(self, call, pid: str, name: str):
        self._delete_project(pid)
        await self._edit_ui(call, self._t("project_deleted", name=name))
        await asyncio.sleep(1)
        await self._cb_back_to_main(call)

    async def _cb_back_to_main(self, call):
        projects = self._get_projects()
        buttons = [[{"text": self._t("create_btn"), "callback": self._cb_create_project}]]
        if projects:
            buttons.append([{"text": self._t("list_btn"), "callback": self._cb_list_projects}])
        await self._edit_ui(call, self._t("welcome", count=len(projects)), reply_markup=buttons)

    @loader.command(
        ru_doc="Открыть RedConstructor",
        en_doc="Open RedConstructor",
        de_doc="RedConstructor offnen",
        uk_doc="Відкрити RedConstructor",
        jp_doc="RedConstructorを開く",
        tiktok_doc="открыть билдер модулей",
        neofit_doc="открыть конструктор модулей",
        leet_doc="0p3n m0dul3 1d3",
        uwu_doc="open da moduwe ide",
    )
    async def rcbm(self, message: Message):
        """Open RedConstructor."""
        await self._show_main_menu(message)

    @loader.command(
        ru_doc="Показать справку RedConstructor",
        en_doc="Show RedConstructor help",
        de_doc="RedConstructor Hilfe",
        uk_doc="Показати довідку RedConstructor",
        jp_doc="RedConstructorヘルプ",
        tiktok_doc="показать хелп ide",
        neofit_doc="справка ide",
        leet_doc="5h0w 1d3 h3lp",
        uwu_doc="show ide hewp",
    )
    async def rchp(self, message: Message):
        """Show IDE help."""
        await utils.answer(message, self._t("help_text"))

    @loader.command(
        ru_doc="Показать проекты RedConstructor",
        en_doc="List RedConstructor projects",
        de_doc="Projekte von RedConstructor",
        uk_doc="Показати проєкти RedConstructor",
        jp_doc="RedConstructorのプロジェクト一覧",
        tiktok_doc="список проектов ide",
        neofit_doc="список проектов ide",
        leet_doc="l157 1d3 pr0j3c75",
        uwu_doc="wist ide pwojects",
    )
    async def rcls(self, message: Message):
        """List IDE projects."""
        self._sanitize_projects()
        projects = self._get_projects()
        if not projects:
            return await utils.answer(message, self._t("no_projects"))

        text = self._t("list_title") + "\n\n"
        for pid, proj in projects.items():
            text += self._t(
                "project_list_line_with_id",
                name=proj["meta"]["name"],
                pid=pid,
                cmds=len(proj["commands"]),
                cfgs=len(proj["configs"]),
            )

        await utils.answer(message, text)

    @loader.command(
        ru_doc="Удалить повреждённые проекты из базы данных IDE",
        en_doc="Remove corrupted projects from IDE database",
    )
    async def rcpg(self, message: Message):
        """Remove corrupted IDE projects from DB."""
        count_before = len(self._get_projects())
        self._sanitize_projects()
        count_after = len(self._get_projects())
        removed = count_before - count_after
        await utils.answer(message, self._t("purge_done", removed=removed, remaining=count_after))