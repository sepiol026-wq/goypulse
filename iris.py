# ====================================================================================================================
#   ██████╗  ██████╗ ██╗   ██╗███╗   ███╗ ██████╗ ██████╗ ██╗   ██╗██╗     ███████╗███████╗
#  ██╔════╝ ██╔═══██╗╚██╗ ██╔╝████╗ ████║██╔═══██╗██╔══██╗██║   ██║██║     ██╔════╝██╔════╝
#  ██║  ███╗██║   ██║ ╚████╔╝ ██╔████╔██║██║   ██║██║  ██║██║   ██║██║     █████╗  ███████╗
#  ██║   ██║██║   ██║  ╚██╔╝  ██║╚██╔╝██║██║   ██║██║  ██║██║   ██║██║     ██╔══╝  ╚════██║
#  ╚██████╔╝╚██████╔╝   ██║   ██║ ╚═╝ ██║╚██████╔╝██████╔╝╚██████╔╝███████╗███████╗███████║
#   ╚═════╝  ╚═════╝    ╚═╝   ╚═╝     ╚═╝ ╚═════╝ ╚═════╝  ╚═════╝ ╚══════╝╚══════╝╚══════╝
#
#   OFFICIAL USERNAMES: @goymodules | @samsepi0l_ovf
#   MODULE: iris
# ====================================================================================================================

# requires: html
# meta developer: @GoyModules
# authors: @goymodules
# Description: Iris-cm.
# meta banner: https://raw.githubusercontent.com/sepiol026-wq/GoyModules/refs/heads/main/assets/iris.png

import asyncio
import html
import logging
import random
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from herokutl import functions
from herokutl.extensions import html as herokutl_html
from herokutl.tl.types import InputPeerNotifySettings

from .. import loader, utils
from herokutl.types import Message

__version__ = (1, 0, 1)

logger = logging.getLogger(__name__)

E_OK = "<tg-emoji emoji-id=5255813619702049821>✅</tg-emoji>"
E_ERR = "<tg-emoji emoji-id=5253864872780769235>❗️</tg-emoji>"
E_FIRE = "<tg-emoji emoji-id=5253877736207821121>🔥</tg-emoji>"
E_GEAR = "<tg-emoji emoji-id=5253952855185829086>⚙️</tg-emoji>"
E_CLCK = "<tg-emoji emoji-id=5255971360965930740>🕔</tg-emoji>"
E_SYNC = "<tg-emoji emoji-id=5253464392850221514>🔃</tg-emoji>"
E_BAG = "<tg-emoji emoji-id=5256094480498436162>🎒</tg-emoji>"
E_USER = "<tg-emoji emoji-id=5255835635704408236>👤</tg-emoji>"
E_LIST = "<tg-emoji emoji-id=5256230583717079814>📝</tg-emoji>"
E_MSG = "<tg-emoji emoji-id=5253590213917158323>💬</tg-emoji>"
E_BELL = "<tg-emoji emoji-id=5253884483601442590>🔔</tg-emoji>"
E_BOX = "<tg-emoji emoji-id=5256058608931580017>📦</tg-emoji>"

IRIS_BOTS = [
    "iris_black_bot",
    "iris_dp_bot",
    "iris_bs_bot",
    "iris_moon_bot",
    "iris_cm_bot",
]
FARM_WORDS = ["Ферма", "Фарма", "Фармить"]
BASE_COOLDOWN = 4 * 3600
TZ_CHOICES = [f"UTC{offset:+d}" for offset in range(-12, 13)]


def _parse_icoins(text: str) -> int:
    m = re.search(r"([0-9][0-9 ]*)\s*i[¢c]", text, re.IGNORECASE)
    return int(m.group(1).replace(" ", "")) if m else 0


def _parse_cooldown_seconds(text: str) -> int | None:
    chunk = re.search(r"через\s+([^\n\r.!]+)", text.lower())
    if not chunk:
        return None
    part = chunk.group(1)
    hours = re.search(r"(\d+)\s*час", part)
    mins = re.search(r"(\d+)\s*мин", part)
    secs = re.search(r"(\d+)\s*сек", part)
    total = (
        int(hours.group(1)) * 3600 if hours else 0
    ) + (
        int(mins.group(1)) * 60 if mins else 0
    ) + (
        int(secs.group(1)) if secs else 0
    )
    return total or None


def _parse_all_loads(text: str) -> dict[str, float]:
    result = {}
    for bot in IRIS_BOTS:
        match = re.search(
            rf"t\.me/{re.escape(bot)}.{{0,450}}?Загружен:\s*([\d,.]+)\s*%",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if match:
            result[bot] = float(match.group(1).replace(",", "."))
    return result


@loader.tds
class Iris(loader.Module):
    """Iris manager."""

    strings = {
        "name": "Iris",
        "_cls_doc": "Premium Iris manager with smart autofarm, timezone-aware timers and custom UI.",
        "state_on": f"{E_OK} Enabled",
        "state_off": f"{E_ERR} Disabled",
        "unknown": "—",
        "now": "now",
        "not_yet": "not yet",
        "no_data": "no data",
        "loading_status": f"{E_SYNC} <b>Loading Iris status...</b>",
        "loading_logs": f"{E_SYNC} <b>Loading logs...</b>",
        "loading_bag": f"{E_SYNC} <b>Loading bag...</b>",
        "loading_profile": f"{E_SYNC} <b>Loading profile...</b>",
        "loading_super_top": f"{E_SYNC} <b>Loading super chat top...</b>",
        "loading_day_top": f"{E_SYNC} <b>Loading day chat top...</b>",
        "loading_reset": f"{E_SYNC} <b>Resetting module data...</b>",
        "checking_load": f"{E_SYNC} <b>Checking Iris bot load...</b>",
        "farm_already": f"{E_ERR} <b>Autofarm is already running.</b>",
        "farm_started": (
            f"{E_FIRE} <b>Iris launched</b>\n"
            f"{E_GEAR} Bot: <b>{{bot}}</b>\n"
            f"{E_OK} Load: <b>{{load}}%</b>\n"
            f"{E_CLCK} First run: <b>{{first}}</b>\n"
            f"{E_BOX} TZ: <b>{{tz}}</b>"
        ),
        "farm_stopped_already": f"{E_ERR} <b>Autofarm is already stopped.</b>",
        "farm_stopping": f"{E_OK} <b>Autofarm stopped.</b>\n{E_BOX} Dialogue cleanup completed.",
        "status_title": f"{E_FIRE} <b>Iris — status</b>",
        "status": (
            "{title}\n\n"
            f"{E_GEAR} Farm: {{state}}\n"
            f"{E_CLCK} Uptime: <b>{{uptime}}</b>\n"
            f"{E_BOX} Session: <b>{{session}} i¢</b> | Total: <b>{{total}} i¢</b>\n"
            f"{E_SYNC} Farms in session: <b>{{count}}</b>\n"
            f"{E_BELL} Time zone: <b>{{tz}}</b>\n\n"
            f"{E_CLCK} Last: {{last_farm}}\n"
            f"{E_CLCK} Iris cooldown: <b>{{cooldown_left}}</b> ({{cooldown_at}})\n"
            f"{E_FIRE} Planned run: <b>{{planned_left}}</b> ({{planned_at}})\n"
            f"{E_GEAR} Target bot: <b>{{active_bot}}</b>\n\n"
            "<b>Bot load</b>\n{{loads}}"
        ),
        "load_line": "{emoji} <b>{bot}</b>: <b>{load}%</b>",
        "load_unknown": "{emoji} <b>{bot}</b>: <b>{value}</b>",
        "logs_empty": f"{E_BOX} <b>Logs are empty.</b>",
        "logs_bad_arg": f"{E_ERR} <b>Format:</b> <code>.irlogs</code>, <code>.irlogs 10</code>, <code>.irlogs 26.07</code>",
        "logs_title": f"{E_LIST} <b>Logs ({{count}})</b>",
        "bag_title": f"{E_BAG} <b>Bag</b> ({{bot}})",
        "bag_text": (
            f"{E_USER} Owner: <b>{{owner}}</b>\n"
            "🍬 Candies: <b>{candies}</b> | 🌕 Iris Gold: <b>{gold}</b>\n"
            "☢️ i¢: <b>{icoins}</b> | ✨ Stars: <b>{stars}</b>"
        ),
        "bag_error": f"{E_ERR} <b>Could not get bag from {{bot}}:</b>\n<code>{{err}}</code>",
        "profile_title": f"{E_LIST} <b>Profile</b> ({{bot}})",
        "profile_error": f"{E_ERR} <b>Could not get profile from {{bot}}:</b>\n<code>{{err}}</code>",
        "profile_empty": f"{E_ERR} <b>Bot returned an empty profile.</b>",
        "super_top_title": f"{E_FIRE} <b>Super Chat Top</b> ({{bot}})",
        "day_top_title": f"{E_FIRE} <b>Day Chat Top</b> ({{bot}})",
        "top_empty": f"{E_ERR} <b>Could not parse chat top.</b>",
        "top_join_open": "Open join",
        "top_join_apply": "Ask via {name}",
        "top_card": "Catalog card",
        "top_entry": (
            "▫️ <b>{place}.</b> <b>{name}</b>\n"
            "☢️ <b>{coins} i¢</b>\n"
            "{join_line}{card_line}"
        ),
        "no_response": f"{E_ERR} <b>No response from {{bot}}.</b>",
        "reset_done": f"{E_OK} <b>Iris data reset completed.</b>",
        "last_farm_line": "{time} via <b>{bot}</b> (+{coins} i¢)",
        "start_now": "now",
    }

    strings_ru = {
        "name": "Iris",
        "_cls_doc": "Премиум-менеджер Iris: автофарм, точные таймеры, кастомный вывод и таймзона UTC.",
        "state_on": f"{E_OK} Включён",
        "state_off": f"{E_ERR} Выключен",
        "unknown": "—",
        "now": "сейчас",
        "not_yet": "ещё не было",
        "no_data": "нет данных",
        "loading_status": f"{E_SYNC} <b>Загружаю статус Iris...</b>",
        "loading_logs": f"{E_SYNC} <b>Загружаю логи...</b>",
        "loading_bag": f"{E_SYNC} <b>Загружаю мешок...</b>",
        "loading_profile": f"{E_SYNC} <b>Загружаю анкету...</b>",
        "loading_super_top": f"{E_SYNC} <b>Загружаю супер топ бесед...</b>",
        "loading_day_top": f"{E_SYNC} <b>Загружаю топ дня...</b>",
        "loading_reset": f"{E_SYNC} <b>Сбрасываю данные модуля...</b>",
        "checking_load": f"{E_SYNC} <b>Проверяю загрузку ботов Iris...</b>",
        "farm_already": f"{E_ERR} <b>Автофарм уже запущен.</b>",
        "farm_started": (
            f"{E_FIRE} <b>Iris запущен</b>\n"
            f"{E_GEAR} Бот: <b>{{bot}}</b>\n"
            f"{E_OK} Нагрузка: <b>{{load}}%</b>\n"
            f"{E_CLCK} Первый запуск: <b>{{first}}</b>\n"
            f"{E_BOX} Пояс: <b>{{tz}}</b>"
        ),
        "farm_stopped_already": f"{E_ERR} <b>Автофарм уже остановлен.</b>",
        "farm_stopping": f"{E_OK} <b>Автофарм остановлен.</b>\n{E_BOX} Очистка диалога завершена.",
        "status_title": f"{E_FIRE} <b>Iris — статус</b>",
        "status": (
            "{title}\n\n"
            f"{E_GEAR} Фарм: {{state}}\n"
            f"{E_CLCK} Аптайм: <b>{{uptime}}</b>\n"
            f"{E_BOX} Сессия: <b>{{session}} i¢</b> | Всего: <b>{{total}} i¢</b>\n"
            f"{E_SYNC} Фармов в сессии: <b>{{count}}</b>\n"
            f"{E_BELL} Часовой пояс: <b>{{tz}}</b>\n\n"
            f"{E_CLCK} Последний: {{last_farm}}\n"
            f"{E_CLCK} Iris-кд: <b>{{cooldown_left}}</b> ({{cooldown_at}})\n"
            f"{E_FIRE} План модуля: <b>{{planned_left}}</b> ({{planned_at}})\n"
            f"{E_GEAR} Целевой бот: <b>{{active_bot}}</b>\n\n"
            "<b>Загрузка ботов</b>\n{loads}"
        ),
        "load_line": "{emoji} <b>{bot}</b>: <b>{load}%</b>",
        "load_unknown": "{emoji} <b>{bot}</b>: <b>{value}</b>",
        "logs_empty": f"{E_BOX} <b>Логи пусты.</b>",
        "logs_bad_arg": f"{E_ERR} <b>Формат:</b> <code>.irlogs</code>, <code>.irlogs 10</code>, <code>.irlogs 26.07</code>",
        "logs_title": f"{E_LIST} <b>Логи ({{count}})</b>",
        "bag_title": f"{E_BAG} <b>Мешок</b> ({{bot}})",
        "bag_text": (
            f"{E_USER} Владелец: <b>{{owner}}</b>\n"
            "🍬 Ирисок: <b>{candies}</b> | 🌕 Ирис-голд: <b>{gold}</b>\n"
            "☢️ i¢: <b>{icoins}</b> | ✨ Звёздочек: <b>{stars}</b>"
        ),
        "bag_error": f"{E_ERR} <b>Не удалось получить мешок у {{bot}}:</b>\n<code>{{err}}</code>",
        "profile_title": f"{E_LIST} <b>Анкета</b> ({{bot}})",
        "profile_error": f"{E_ERR} <b>Не удалось получить анкету у {{bot}}:</b>\n<code>{{err}}</code>",
        "profile_empty": f"{E_ERR} <b>Бот вернул пустую анкету.</b>",
        "super_top_title": f"{E_FIRE} <b>Супер Топ Бесед</b> ({{bot}})",
        "day_top_title": f"{E_FIRE} <b>Топ Дня</b> ({{bot}})",
        "top_empty": f"{E_ERR} <b>Не удалось разобрать топ бесед.</b>",
        "top_join_open": "Войти",
        "top_join_apply": "Попроситься через {name}",
        "top_card": "Карточка Iris",
        "top_entry": (
            "▫️ <b>{place}.</b> <b>{name}</b>\n"
            "☢️ <b>{coins} i¢</b>\n"
            "{join_line}{card_line}"
        ),
        "no_response": f"{E_ERR} <b>Нет ответа от {{bot}}.</b>",
        "reset_done": f"{E_OK} <b>Сброс данных Iris завершён.</b>",
        "last_farm_line": "{time} через <b>{bot}</b> (+{coins} i¢)",
        "start_now": "сейчас",
    }

    strings_uk = strings_ru | {
        "_cls_doc": "Преміум-менеджер Iris: автофарм, точні таймери, кастомний вивід і зона UTC.",
        "state_on": f"{E_OK} Увімкнено",
        "state_off": f"{E_ERR} Вимкнено",
        "now": "зараз",
        "not_yet": "ще не було",
        "no_data": "немає даних",
        "loading_status": f"{E_SYNC} <b>Завантажую статус Iris...</b>",
        "loading_logs": f"{E_SYNC} <b>Завантажую логи...</b>",
        "loading_bag": f"{E_SYNC} <b>Завантажую мішок...</b>",
        "loading_profile": f"{E_SYNC} <b>Завантажую анкету...</b>",
        "loading_super_top": f"{E_SYNC} <b>Завантажую супер топ чатів...</b>",
        "loading_day_top": f"{E_SYNC} <b>Завантажую топ дня...</b>",
        "loading_reset": f"{E_SYNC} <b>Скидаю дані модуля...</b>",
        "checking_load": f"{E_SYNC} <b>Перевіряю навантаження ботів Iris...</b>",
        "farm_already": f"{E_ERR} <b>Автофарм уже запущено.</b>",
        "farm_stopped_already": f"{E_ERR} <b>Автофарм уже зупинено.</b>",
        "logs_empty": f"{E_BOX} <b>Логи порожні.</b>",
    }

    strings_de = strings_ru | {
        "_cls_doc": "Premium-Iris-Manager mit Autofarm, genauen Timern, benutzerdefinierter Ausgabe und UTC-Zeitzone.",
        "state_on": f"{E_OK} Aktiv",
        "state_off": f"{E_ERR} Inaktiv",
        "now": "jetzt",
        "not_yet": "noch nicht",
        "no_data": "keine Daten",
        "loading_status": f"{E_SYNC} <b>Iris-Status wird geladen...</b>",
        "loading_logs": f"{E_SYNC} <b>Logs werden geladen...</b>",
        "loading_bag": f"{E_SYNC} <b>Tasche wird geladen...</b>",
        "loading_profile": f"{E_SYNC} <b>Profil wird geladen...</b>",
        "loading_super_top": f"{E_SYNC} <b>Super-Chat-Top wird geladen...</b>",
        "loading_day_top": f"{E_SYNC} <b>Tages-Chat-Top wird geladen...</b>",
        "loading_reset": f"{E_SYNC} <b>Moduldaten werden zurückgesetzt...</b>",
        "checking_load": f"{E_SYNC} <b>Bot-Auslastung wird geprüft...</b>",
        "farm_already": f"{E_ERR} <b>Autofarm läuft bereits.</b>",
        "farm_stopped_already": f"{E_ERR} <b>Autofarm ist bereits gestoppt.</b>",
        "logs_empty": f"{E_BOX} <b>Logs sind leer.</b>",
    }

    strings_jp = strings_ru | {
        "_cls_doc": "Iris用プレミアム管理モジュール。自動ファーム、正確なタイマー、カスタム表示、UTCタイムゾーン対応。",
        "state_on": f"{E_OK} 有効",
        "state_off": f"{E_ERR} 無効",
        "now": "今",
        "not_yet": "まだありません",
        "no_data": "データなし",
        "loading_status": f"{E_SYNC} <b>Irisステータスを読み込み中...</b>",
        "loading_logs": f"{E_SYNC} <b>ログを読み込み中...</b>",
        "loading_bag": f"{E_SYNC} <b>バッグを読み込み中...</b>",
        "loading_profile": f"{E_SYNC} <b>プロフィールを読み込み中...</b>",
        "loading_super_top": f"{E_SYNC} <b>スーパー掲示板ランキングを読み込み中...</b>",
        "loading_day_top": f"{E_SYNC} <b>日間ランキングを読み込み中...</b>",
        "loading_reset": f"{E_SYNC} <b>モジュールデータをリセット中...</b>",
        "checking_load": f"{E_SYNC} <b>Irisボット負荷を確認中...</b>",
        "farm_already": f"{E_ERR} <b>自動ファームは既に動作中です。</b>",
        "farm_stopped_already": f"{E_ERR} <b>自動ファームは既に停止しています。</b>",
        "logs_empty": f"{E_BOX} <b>ログは空です。</b>",
    }

    strings_neofit = strings_ru | {
        "_cls_doc": "Ирис модуль по красоте: автофарм, таймеры, мешок и анкета без мусора.",
        "state_on": f"{E_OK} Работает",
        "state_off": f"{E_ERR} Спит",
        "loading_status": f"{E_SYNC} <b>Собираю движ по Iris...</b>",
        "loading_logs": f"{E_SYNC} <b>Поднимаю логи...</b>",
        "loading_bag": f"{E_SYNC} <b>Смотрю твой мешок...</b>",
        "loading_profile": f"{E_SYNC} <b>Собираю анкету...</b>",
        "loading_super_top": f"{E_SYNC} <b>Тащу супер топ...</b>",
        "loading_day_top": f"{E_SYNC} <b>Тащу дневной топ...</b>",
        "loading_reset": f"{E_SYNC} <b>Обнуляю модуль...</b>",
        "checking_load": f"{E_SYNC} <b>Пробиваю загруженность ботов...</b>",
        "status_title": f"{E_FIRE} <b>Iris — расклад</b>",
        "bag_title": f"{E_BAG} <b>Мешок</b> ({{bot}})",
        "bag_text": (
            f"{E_USER} Хозяин: <b>{{owner}}</b>\n"
            "🍬 Ириски: <b>{candies}</b> | 🌕 Голд: <b>{gold}</b>\n"
            "☢️ Коинов: <b>{icoins}</b> | ✨ Звёзд: <b>{stars}</b>\n"
            "⭐️ TG Stars: <b>{tg_stars}</b>"
        ),
        "profile_title": f"{E_LIST} <b>Анкета</b> ({{bot}})",
        "profile_text": (
            "{title}\n\n"
            f"{E_USER} Это <b>{{name}}</b> ({{status}})\n"
            "🆔 <b>{handle}</b>\n\n"
            "⏱ {universe_label}: <b>{since_date}</b> ({since_span})\n"
            "👨 Пол: <b>{gender}</b>\n"
            "📆 Дата рождения: <b>{birth}</b>\n"
            "🗺 Город: <b>{city}</b>\n"
            "📊 Активность: <b>{day}</b> | <b>{week}</b> | <b>{month}</b> | <b>{total}</b>\n"
            "✨ Звёздность: <b>{rank}</b>\n\n"
            f"{E_BAG} Баланс\n"
            "🍬 <b>{candies}</b> | ✨ <b>{stars}</b>\n"
            "☢️ <b>{icoins}</b> | 🌕 <b>{gold}</b>\n"
            "⭐️ <b>{tg_stars}</b>\n\n"
            "• твой профиль iris •"
        ),
        "profile_universe": "Во вселенной Iris",
        "super_top_title": f"{E_FIRE} <b>Супер Топ</b> ({{bot}})",
        "day_top_title": f"{E_FIRE} <b>Дневной Топ</b> ({{bot}})",
        "top_join_open": "Зайти",
        "top_join_apply": "Попроситься через {name}",
        "top_card": "Карточка",
    }
    strings_tiktok = strings_ru | {
        "_cls_doc": "skid iris: фарм, таймеры, мешок, анкета.",
        "state_on": f"{E_OK} вруб",
        "state_off": f"{E_ERR} выруб",
        "loading_status": f"{E_SYNC} <b>Лутаю стату...</b>",
        "loading_logs": f"{E_SYNC} <b>Тащу лог-дамп...</b>",
        "loading_bag": f"{E_SYNC} <b>Открываю лут...</b>",
        "loading_profile": f"{E_SYNC} <b>Пробиваю акк...</b>",
        "loading_super_top": f"{E_SYNC} <b>Гружу топ-лутач...</b>",
        "loading_day_top": f"{E_SYNC} <b>Гружу дейлик-топ...</b>",
        "loading_reset": f"{E_SYNC} <b>Делаю вайп...</b>",
        "checking_load": f"{E_SYNC} <b>Чекаю нагруз...</b>",
        "status_title": f"{E_FIRE} <b>Iris — skid stat</b>",
        "farm_started": (
            f"{E_FIRE} <b>Iris стартанул</b>\n"
            f"{E_GEAR} Таргет: <b>{{bot}}</b>\n"
            f"{E_OK} Нагруз: <b>{{load}}%</b>\n"
            f"{E_CLCK} Старт: <b>{{first}}</b>\n"
            f"{E_BOX} UTC: <b>{{tz}}</b>"
        ),
        "bag_title": f"{E_BAG} <b>Лут</b> ({{bot}})",
        "bag_text": (
            f"{E_USER} Хозяин лута: <b>{{owner}}</b>\n"
            "🍬 Ириски: <b>{candies}</b> | 🌕 Голда: <b>{gold}</b>\n"
            "☢️ Коины: <b>{icoins}</b> | ✨ Старсы: <b>{stars}</b>\n"
            "⭐️ Тг-старсы: <b>{tg_stars}</b>"
        ),
        "profile_title": f"{E_LIST} <b>Профайл</b> ({{bot}})",
        "profile_text": (
            "{title}\n\n"
            f"{E_USER} Это <b>{{name}}</b> ({{status}})\n"
            "🆔 <b>{handle}</b>\n\n"
            "⏱ {universe_label}: <b>{since_date}</b> ({since_span})\n"
            "👨 Пол: <b>{gender}</b>\n"
            "📆 Днюха: <b>{birth}</b>\n"
            "🗺 Лока: <b>{city}</b>\n"
            "📊 Актив: <b>{day}</b> | <b>{week}</b> | <b>{month}</b> | <b>{total}</b>\n"
            "✨ Ранг: <b>{rank}</b>\n\n"
            f"{E_BAG} Балик\n"
            "🍬 <b>{candies}</b> | ✨ <b>{stars}</b>\n"
            "☢️ <b>{icoins}</b> | 🌕 <b>{gold}</b>\n"
            "⭐️ <b>{tg_stars}</b>"
        ),
        "profile_universe": "Во вселенной ириса",
        "super_top_title": f"{E_FIRE} <b>Супер Топ</b> ({{bot}})",
        "day_top_title": f"{E_FIRE} <b>Топ Дня</b> ({{bot}})",
        "top_empty": f"{E_ERR} <b>Топ не распарсился.</b>",
        "top_join_open": "Залететь",
        "top_join_apply": "Зайти через {name}",
        "top_card": "Карточка",
    }
    strings_leet = strings_ru | {
        "_cls_doc": "Pr3m1um Ir15 m4n4g3r: 4ut0f4rm, pr3c153 t1m3r5, cu570m UI, UTC.",
        "state_on": f"{E_OK} 0N",
        "state_off": f"{E_ERR} 0FF",
        "loading_status": f"{E_SYNC} <b>L04d1n6 5747u5...</b>",
        "loading_logs": f"{E_SYNC} <b>L04d1n6 l065...</b>",
        "loading_bag": f"{E_SYNC} <b>L04d1n6 b46...</b>",
        "loading_profile": f"{E_SYNC} <b>L04d1n6 pr0f1l3...</b>",
        "loading_super_top": f"{E_SYNC} <b>L04d1n6 5up3r 70p...</b>",
        "loading_day_top": f"{E_SYNC} <b>L04d1n6 d4y 70p...</b>",
        "loading_reset": f"{E_SYNC} <b>R353771n6 d474...</b>",
        "checking_load": f"{E_SYNC} <b>Ch3ck1n6 b07 l04d...</b>",
        "status_title": f"{E_FIRE} <b>1r15 5747u5</b>",
        "bag_title": f"{E_BAG} <b>B46</b> ({{bot}})",
        "bag_text": (
            f"{E_USER} 0wn3r: <b>{{owner}}</b>\n"
            "🍬 C4nd135: <b>{candies}</b> | 🌕 60ld: <b>{gold}</b>\n"
            "☢️ 1¢: <b>{icoins}</b> | ✨ 574r5: <b>{stars}</b>\n"
            "⭐️ 76 574r5: <b>{tg_stars}</b>"
        ),
        "profile_title": f"{E_LIST} <b>Pr0f1l3</b> ({{bot}})",
        "profile_text": (
            "{title}\n\n"
            f"{E_USER} 17'5 <b>{{name}}</b> ({{status}})\n"
            "🆔 <b>{handle}</b>\n\n"
            "⏱ {universe_label}: <b>{since_date}</b> ({since_span})\n"
            "👨 63nd3r: <b>{gender}</b>\n"
            "📆 81r7hd4y: <b>{birth}</b>\n"
            "🗺 C17y: <b>{city}</b>\n"
            "📊 4c71v17y: <b>{day}</b> | <b>{week}</b> | <b>{month}</b> | <b>{total}</b>\n"
            "✨ R4nk: <b>{rank}</b>\n\n"
            f"{E_BAG} 84l4nc3\n"
            "🍬 <b>{candies}</b> | ✨ <b>{stars}</b>\n"
            "☢️ <b>{icoins}</b> | 🌕 <b>{gold}</b>\n"
            "⭐️ <b>{tg_stars}</b>\n\n"
            "• pr0f1l3 53c710n •"
        ),
        "profile_universe": "1r15 un1v3r53",
        "super_top_title": f"{E_FIRE} <b>5up3r Ch47 70p</b> ({{bot}})",
        "day_top_title": f"{E_FIRE} <b>D4y Ch47 70p</b> ({{bot}})",
        "top_empty": f"{E_ERR} <b>C4n'7 p4r53 ch47 70p.</b>",
        "top_join_open": "0p3n j01n",
        "top_join_apply": "45k v14 {name}",
        "top_card": "C474l06 c4rd",
    }
    strings_uwu = strings_ru | {
        "_cls_doc": "Iwis moduwe uwu: autofawm, cwockies, custom bag and pwofiwe uwu",
        "state_on": f"{E_OK} on uwu",
        "state_off": f"{E_ERR} off uwu",
        "loading_status": f"{E_SYNC} <b>Woading iwis status...</b>",
        "loading_logs": f"{E_SYNC} <b>Woading wogs...</b>",
        "loading_bag": f"{E_SYNC} <b>Wooking into bag...</b>",
        "loading_profile": f"{E_SYNC} <b>Woading pwofiwe...</b>",
        "loading_super_top": f"{E_SYNC} <b>Woading supew chat top...</b>",
        "loading_day_top": f"{E_SYNC} <b>Woading day top...</b>",
        "loading_reset": f"{E_SYNC} <b>Wesetting data...</b>",
        "checking_load": f"{E_SYNC} <b>Checking bot woad...</b>",
        "status_title": f"{E_FIRE} <b>Iwis status uwu</b>",
        "bag_title": f"{E_BAG} <b>Baggy</b> ({{bot}})",
        "bag_text": (
            f"{E_USER} Ownew: <b>{{owner}}</b>\n"
            "🍬 Candies: <b>{candies}</b> | 🌕 Gowd: <b>{gold}</b>\n"
            "☢️ i¢: <b>{icoins}</b> | ✨ Staws: <b>{stars}</b>\n"
            "⭐️ TG staws: <b>{tg_stars}</b>"
        ),
        "profile_title": f"{E_LIST} <b>Pwofiwe</b> ({{bot}})",
        "profile_text": (
            "{title}\n\n"
            f"{E_USER} It's <b>{{name}}</b> ({{status}})\n"
            "🆔 <b>{handle}</b>\n\n"
            "⏱ {universe_label}: <b>{since_date}</b> ({since_span})\n"
            "👨 Gendew: <b>{gender}</b>\n"
            "📆 Biwthday: <b>{birth}</b>\n"
            "🗺 City: <b>{city}</b>\n"
            "📊 Activity: <b>{day}</b> | <b>{week}</b> | <b>{month}</b> | <b>{total}</b>\n"
            "✨ Stawness: <b>{rank}</b>\n\n"
            f"{E_BAG} Bawance\n"
            "🍬 <b>{candies}</b> | ✨ <b>{stars}</b>\n"
            "☢️ <b>{icoins}</b> | 🌕 <b>{gold}</b>\n"
            "⭐️ <b>{tg_stars}</b>\n\n"
            "• pwofiwe section uwu •"
        ),
        "profile_universe": "Iwis univewse",
        "super_top_title": f"{E_FIRE} <b>Supew Chat Top</b> ({{bot}})",
        "day_top_title": f"{E_FIRE} <b>Day Chat Top</b> ({{bot}})",
        "top_empty": f"{E_ERR} <b>Couldn't pawse chat top uwu.</b>",
        "top_join_open": "Join nyow",
        "top_join_apply": "Ask via {name}",
        "top_card": "Catawog cawd",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "extra_delay_min",
                3,
                "Min random delay after Iris cooldown (minutes)",
                validator=loader.validators.Integer(minimum=0, maximum=30),
            ),
            loader.ConfigValue(
                "extra_delay_max",
                8,
                "Max random delay after Iris cooldown (minutes)",
                validator=loader.validators.Integer(minimum=0, maximum=60),
            ),
            loader.ConfigValue(
                "max_logs",
                300,
                "Maximum log entries",
                validator=loader.validators.Integer(minimum=20, maximum=1000),
            ),
            loader.ConfigValue(
                "load_refresh_minutes",
                20,
                "How often to refresh bot load in background (minutes)",
                validator=loader.validators.Integer(minimum=5, maximum=120),
            ),
            loader.ConfigValue(
                "response_timeout",
                120,
                "Bot response timeout in seconds",
                validator=loader.validators.Integer(minimum=30, maximum=300),
            ),
            loader.ConfigValue(
                "auto_cleanup",
                True,
                "Delete service messages after requests",
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "timezone_offset",
                "UTC+3",
                "Display timezone",
                validator=loader.validators.Choice(TZ_CHOICES),
            ),
        )
        self._running = False
        self._farm_task = None
        self._load_task = None
        self._sent_msgs = []
        self._bot_loads = {}
        self._bot_loads_updated = 0.0
        self._active_bot = IRIS_BOTS[0]
        self._session_count = 0
        self._start_ts = 0.0
        self._cooldown_until = 0.0
        self._next_run_at = 0.0
        self._last_farm_ts = 0.0
        self._last_farm_bot = ""
        self._last_farm_coins = 0
        self._watcher_resync = True

    async def client_ready(self, client, db):
        self.client = client
        self.db = db
        self._logs = self.db.get(self.strings["name"], "logs", [])
        self._bot_loads = self.db.get(self.strings["name"], "bot_loads", {}) or {}
        self._bot_loads_updated = float(self.db.get(self.strings["name"], "bot_loads_updated", 0.0) or 0.0)
        self._active_bot = self.db.get(self.strings["name"], "active_bot", self._active_bot) or self._active_bot
        self._restore_runtime_state()
        if self._running:
            await self._mute_all(True)
            if not self._farm_task or self._farm_task.done():
                self._farm_task = asyncio.create_task(self._farm_loop())
            if not self._load_task or self._load_task.done():
                self._load_task = asyncio.create_task(self._load_refresh_loop())
            self._add_log("↩️ Autofarm restored after restart")

    def _tz_hours(self) -> int:
        return int(self.config["timezone_offset"].replace("UTC", ""))

    def _tz(self):
        return timezone(timedelta(hours=self._tz_hours()))

    def _now_utc(self) -> datetime:
        return datetime.now(timezone.utc)

    def _now_local(self) -> datetime:
        return self._now_utc().astimezone(self._tz())

    def _now_ts(self) -> float:
        return self._now_utc().timestamp()

    def _lang_code(self) -> str:
        if hasattr(self, "getlang"):
            try:
                return (self.getlang() or "ru").split("_")[0]
            except Exception:
                return "ru"
        return "ru"

    def _format_dt(self, ts: float) -> str:
        if not ts:
            return self.strings["unknown"]
        return datetime.fromtimestamp(ts, timezone.utc).astimezone(self._tz()).strftime("%H:%M")

    def _format_td(self, seconds: int | float) -> str:
        total = max(0, int(seconds))
        if total <= 0:
            return self.strings["now"]
        hours, rem = divmod(total, 3600)
        mins, secs = divmod(rem, 60)
        parts = []
        lang = self._lang_code()
        if lang in {"en", "de", "neofit", "tiktok", "leet", "uwu"}:
            if hours:
                parts.append(f"{hours}h")
            if mins:
                parts.append(f"{mins}m")
            if secs and not hours:
                parts.append(f"{secs}s")
        elif lang == "jp":
            if hours:
                parts.append(f"{hours}時間")
            if mins:
                parts.append(f"{mins}分")
            if secs and not hours:
                parts.append(f"{secs}秒")
        else:
            if hours:
                parts.append(f"{hours}ч")
            if mins:
                parts.append(f"{mins}мин")
            if secs and not hours:
                parts.append(f"{secs}сек")
        return " ".join(parts) if parts else self.strings["now"]

    def _add_log(self, text: str):
        stamp = self._now_local().strftime("%d.%m %H:%M")
        self._logs.append(f"[{stamp}] {text}")
        limit = self.config["max_logs"]
        if len(self._logs) > limit:
            self._logs = self._logs[-limit:]
        self.db.set(self.strings["name"], "logs", self._logs)

    def _save_load_cache(self):
        self.db.set(self.strings["name"], "bot_loads", self._bot_loads)
        self.db.set(self.strings["name"], "bot_loads_updated", self._bot_loads_updated)
        self.db.set(self.strings["name"], "active_bot", self._active_bot)

    def _save_runtime_state(self):
        self.db.set(self.strings["name"], "running", self._running)
        self.db.set(self.strings["name"], "start_ts", self._start_ts)
        self.db.set(self.strings["name"], "session_count", self._session_count)
        self.db.set(self.strings["name"], "cooldown_until", self._cooldown_until)
        self.db.set(self.strings["name"], "next_run_at", self._next_run_at)
        self.db.set(self.strings["name"], "last_farm_ts", self._last_farm_ts)
        self.db.set(self.strings["name"], "last_farm_bot", self._last_farm_bot)
        self.db.set(self.strings["name"], "last_farm_coins", self._last_farm_coins)
        self.db.set(self.strings["name"], "watcher_resync", self._watcher_resync)

    def _restore_runtime_state(self):
        self._running = bool(self.db.get(self.strings["name"], "running", False))
        self._start_ts = float(self.db.get(self.strings["name"], "start_ts", 0.0) or 0.0)
        self._session_count = int(self.db.get(self.strings["name"], "session_count", 0) or 0)
        self._cooldown_until = float(self.db.get(self.strings["name"], "cooldown_until", 0.0) or 0.0)
        self._next_run_at = float(self.db.get(self.strings["name"], "next_run_at", 0.0) or 0.0)
        self._last_farm_ts = float(self.db.get(self.strings["name"], "last_farm_ts", 0.0) or 0.0)
        self._last_farm_bot = self.db.get(self.strings["name"], "last_farm_bot", "") or ""
        self._last_farm_coins = int(self.db.get(self.strings["name"], "last_farm_coins", 0) or 0)
        self._watcher_resync = bool(self.db.get(self.strings["name"], "watcher_resync", True))
        self._normalize_runtime_state()

    def _reset_runtime_state(self):
        self._running = False
        self._start_ts = 0.0
        self._session_count = 0
        self._cooldown_until = 0.0
        self._next_run_at = 0.0
        self._last_farm_ts = 0.0
        self._last_farm_bot = ""
        self._last_farm_coins = 0
        self._watcher_resync = True
        self._save_runtime_state()

    def _normalize_runtime_state(self):
        now = self._now_ts()
        if not self._running:
            return
        if self._last_farm_ts and self._cooldown_until < self._last_farm_ts:
            self._cooldown_until = self._last_farm_ts + BASE_COOLDOWN
        if self._cooldown_until and not self._next_run_at:
            self._next_run_at = self._cooldown_until
        if self._next_run_at and self._cooldown_until and self._next_run_at < self._cooldown_until:
            self._next_run_at = self._cooldown_until
        if not self._start_ts:
            self._start_ts = self._last_farm_ts or now
        if self._cooldown_until and (now - self._cooldown_until) > BASE_COOLDOWN * 3:
            self._cooldown_until = 0.0
        if self._next_run_at and (now - self._next_run_at) > BASE_COOLDOWN * 3:
            self._next_run_at = now
        self._save_runtime_state()

    def _extra_delay(self) -> int:
        low = self.config["extra_delay_min"] * 60
        high = self.config["extra_delay_max"] * 60
        if low >= high:
            return low
        return random.randint(low, high) + random.randint(0, 59)

    def _resolve_bot(self, arg: str) -> str:
        raw = (arg or "").strip().lower()
        if not raw:
            return self._active_bot if self._running else IRIS_BOTS[0]
        if raw in IRIS_BOTS:
            return raw
        short = raw.replace("@", "").replace("_bot", "")
        aliases = {
            "black": "iris_black_bot",
            "dp": "iris_dp_bot",
            "deep": "iris_dp_bot",
            "bs": "iris_bs_bot",
            "moon": "iris_moon_bot",
            "cm": "iris_cm_bot",
            "chat": "iris_cm_bot",
        }
        if short in aliases:
            return aliases[short]
        for bot in IRIS_BOTS:
            if short in bot:
                return bot
        return IRIS_BOTS[0]

    async def _mute_bot(self, username: str, mute: bool):
        try:
            peer = await self.client.get_input_entity(username)
            settings = InputPeerNotifySettings(mute_until=(2**31 - 1) if mute else 0)
            await self.client(functions.account.UpdateNotifySettingsRequest(peer=peer, settings=settings))
        except Exception as e:
            logger.debug("Mute %s failed: %s", username, e)

    async def _mute_all(self, mute: bool):
        for bot in IRIS_BOTS:
            await self._mute_bot(bot, mute)

    async def _mark_read(self, username: str):
        try:
            await self.client.send_read_acknowledge(username)
        except Exception:
            pass

    async def _delete_tracked(self, bot: str, ids: list[int]):
        if not ids:
            return
        try:
            await self.client.delete_messages(bot, ids)
        except Exception as e:
            logger.debug("Delete failed for %s: %s", bot, e)

    async def _cleanup_all(self):
        grouped = defaultdict(list)
        for bot, mid in self._sent_msgs:
            grouped[bot].append(mid)
        for bot, ids in grouped.items():
            await self._delete_tracked(bot, ids)
        self._sent_msgs.clear()

    async def _wait_response(self, bot: str, after_msg_id: int, after_date):
        timeout = self.config["response_timeout"]
        for _ in range(timeout):
            async for resp in self.client.iter_messages(bot, limit=6):
                if resp.out or resp.id <= after_msg_id:
                    continue
                try:
                    if after_date and resp.date and resp.date < after_date:
                        continue
                except Exception:
                    pass
                return resp
            await asyncio.sleep(1)
        return None

    async def _wait_response_match(self, bot: str, after_msg_id: int, after_date, matcher=None):
        fallback = None
        timeout = self.config["response_timeout"]
        for _ in range(timeout):
            async for resp in self.client.iter_messages(bot, limit=8):
                if resp.out or resp.id <= after_msg_id:
                    continue
                try:
                    if after_date and resp.date and resp.date < after_date:
                        continue
                except Exception:
                    pass
                if matcher is None:
                    return resp
                if matcher(resp):
                    return resp
                if fallback is None:
                    fallback = resp
            await asyncio.sleep(1)
        return fallback

    async def _request_bot(self, bot: str, command: str, matcher=None):
        sent = []
        try:
            msg = await self.client.send_message(bot, command)
            sent.append(msg.id)
            resp = await self._wait_response_match(bot, msg.id, msg.date, matcher)
            await self._mark_read(bot)
            if resp:
                sent.append(resp.id)
            return resp, sent
        finally:
            if self.config["auto_cleanup"] and sent:
                await self._delete_tracked(bot, sent)

    async def _fetch_loads(self) -> dict[str, float]:
        for probe_bot in IRIS_BOTS:
            try:
                await self._mute_bot(probe_bot, True)
                msg = await self.client.send_message(probe_bot, "🌺 Семейство ирисовых")
                self._sent_msgs.append((probe_bot, msg.id))
                resp = await self._wait_response(probe_bot, msg.id, msg.date)
                await self._mark_read(probe_bot)
                if not resp:
                    continue
                self._sent_msgs.append((probe_bot, resp.id))
                loads = _parse_all_loads(resp.text or resp.raw_text or "")
                if loads:
                    self._bot_loads_updated = self._now_ts()
                    self._bot_loads = loads
                    self._active_bot = sorted(loads.items(), key=lambda item: item[1])[0][0]
                    self._save_load_cache()
                    if self.config["auto_cleanup"]:
                        await self._delete_tracked(probe_bot, [msg.id, resp.id])
                        self._sent_msgs = [(b, mid) for b, mid in self._sent_msgs if not (b == probe_bot and mid in {msg.id, resp.id})]
                    return loads
                if self.config["auto_cleanup"]:
                    await self._delete_tracked(probe_bot, [msg.id, resp.id])
                    self._sent_msgs = [(b, mid) for b, mid in self._sent_msgs if not (b == probe_bot and mid in {msg.id, resp.id})]
            except Exception as e:
                logger.warning("Load fetch via %s failed: %s", probe_bot, e)
            finally:
                await self._mute_bot(probe_bot, False)
        self._bot_loads_updated = self._now_ts()
        self._add_log("⚠️ Load check failed for all Iris bots")
        return {}

    def _best_bot(self) -> tuple[str, float]:
        if not self._bot_loads:
            return IRIS_BOTS[0], -1.0
        return sorted(self._bot_loads.items(), key=lambda item: item[1])[0]

    async def _get_best_bot(self) -> tuple[str, float]:
        stale = (self._now_ts() - self._bot_loads_updated) > self.config["load_refresh_minutes"] * 60
        if not self._bot_loads or stale:
            fresh = await self._fetch_loads()
            if fresh:
                self._bot_loads = fresh
        return self._best_bot()

    async def _pick_request_bot(self) -> tuple[str, float]:
        bot, load = await self._get_best_bot()
        self._active_bot = bot
        self._save_load_cache()
        return bot, load

    async def _load_refresh_loop(self):
        while self._running:
            try:
                await asyncio.sleep(self.config["load_refresh_minutes"] * 60)
                if not self._running:
                    return
                fresh = await self._fetch_loads()
                if fresh:
                    line = ", ".join(f"{bot.replace('iris_', '').replace('_bot', '')}={load:.1f}%" for bot, load in sorted(fresh.items(), key=lambda item: item[1]))
                    self._add_log(f"🔄 Load refresh: {line}")
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning("load_refresh_loop: %s", e)

    def _plan_next_run(self, cooldown_seconds: int):
        base = self._now_ts() + max(0, cooldown_seconds)
        self._cooldown_until = base
        self._next_run_at = base + self._extra_delay()
        self._save_runtime_state()

    async def _do_farm(self, bot: str) -> int:
        mine = []
        theirs = []
        try:
            msg = await self.client.send_message(bot, random.choice(FARM_WORDS))
            mine.append(msg.id)
            self._sent_msgs.append((bot, msg.id))
            resp = await self._wait_response(bot, msg.id, msg.date)
            await self._mark_read(bot)
            if not resp:
                self._add_log(f"⚠️ [{bot}] no response")
                return 10 * 60
            theirs.append(resp.id)
            self._sent_msgs.append((bot, resp.id))
            text = resp.raw_text or ""
            upper = text.upper()
            if "ЗАЧЁТ" in upper and "НЕЗАЧЁТ" not in upper:
                coins = _parse_icoins(text)
                total = self.db.get(self.strings["name"], "total_icoins", 0) + coins
                session = self.db.get(self.strings["name"], "session_icoins", 0) + coins
                self.db.set(self.strings["name"], "total_icoins", total)
                self.db.set(self.strings["name"], "session_icoins", session)
                self._session_count += 1
                self._last_farm_ts = self._now_ts()
                self._last_farm_bot = bot
                self._last_farm_coins = coins
                self._plan_next_run(BASE_COOLDOWN)
                self._add_log(f"✅ [{bot}] +{coins} i¢ | total {total} i¢ | iris {self._format_dt(self._cooldown_until)} | run {self._format_dt(self._next_run_at)}")
                self._save_runtime_state()
                return max(1, int(self._next_run_at - self._now_ts()))
            if "НЕЗАЧЁТ" in upper:
                seconds = _parse_cooldown_seconds(text) or BASE_COOLDOWN
                self._plan_next_run(seconds)
                self._add_log(f"❌ [{bot}] cooldown {self._format_td(seconds)} | iris {self._format_dt(self._cooldown_until)} | run {self._format_dt(self._next_run_at)}")
                self._save_runtime_state()
                return max(1, int(self._next_run_at - self._now_ts()))
            self._add_log(f"❓ [{bot}] unknown response")
            return 10 * 60
        finally:
            if self.config["auto_cleanup"]:
                ids = mine + theirs
                await self._delete_tracked(bot, ids)
                self._sent_msgs = [(b, mid) for b, mid in self._sent_msgs if not (b == bot and mid in ids)]

    async def _farm_loop(self):
        while self._running:
            try:
                wait_left = max(0, int(self._next_run_at - self._now_ts())) if self._next_run_at else 0
                if wait_left:
                    elapsed = 0
                    while self._running and elapsed < wait_left:
                        chunk = min(60, wait_left - elapsed)
                        await asyncio.sleep(chunk)
                        elapsed += chunk
                    if not self._running:
                        return
                bot, load = await self._get_best_bot()
                self._active_bot = bot
                self._save_load_cache()
                self._add_log(f"🎯 {bot} selected ({load:.1f}% load)")
                delay = await self._do_farm(bot)
                elapsed = 0
                while self._running and elapsed < delay:
                    chunk = min(60, delay - elapsed)
                    await asyncio.sleep(chunk)
                    elapsed += chunk
            except asyncio.CancelledError:
                self._add_log("⛔ Farm loop stopped")
                return
            except Exception as e:
                self._add_log(f"💥 Loop error: {e}")
                logger.exception("farm loop")
                await asyncio.sleep(300)

    def _render_loads(self) -> str:
        lines = []
        for bot in IRIS_BOTS:
            load = self._bot_loads.get(bot)
            if load is None:
                lines.append(self.strings["load_unknown"].format(emoji="❔", bot=bot, value=self.strings["no_data"]))
                continue
            emoji = "💚" if load < 20 else "💛" if load < 45 else "🧡" if load < 70 else "❤️"
            lines.append(self.strings["load_line"].format(emoji=emoji, bot=bot, load=f"{load:.1f}"))
        return "\n".join(lines)

    def _last_farm_text(self) -> str:
        if not self._last_farm_ts:
            return self.strings["not_yet"]
        return self.strings["last_farm_line"].format(
            time=self._format_dt(self._last_farm_ts),
            bot=self._last_farm_bot,
            coins=self._last_farm_coins,
        )

    def _schedule_text(self, ts: float) -> tuple[str, str]:
        if not ts:
            return self.strings["unknown"], self.strings["unknown"]
        left = max(0, int(ts - self._now_ts()))
        return self._format_td(left), self._format_dt(ts)

    def _clean_iris_text(self, text: str) -> str:
        text = html.escape((text or "").strip())
        text = re.sub(r"(?im)^💬.*?$", "", text)
        text = re.sub(r"(?im)^•\s*описание.*?$", "", text)
        text = re.sub(r"(?im)^•\s*description.*?$", "", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _message_html(self, message) -> str:
        text = getattr(message, "message", None) or getattr(message, "raw_text", None) or getattr(message, "text", None) or ""
        entities = getattr(message, "entities", None) or []
        try:
            return herokutl_html.unparse(text, entities)
        except Exception:
            return html.escape(text)

    def _plain_text(self, html_text: str) -> str:
        return html.unescape(re.sub(r"<[^>]+>", "", html_text or "")).strip()

    def _extract_href(self, html_line: str) -> str:
        match = re.search(r'href="([^"]+)"', html_line or "")
        return html.escape(match.group(1), quote=True) if match else ""

    def _is_super_top_response(self, text: str) -> bool:
        source = (text or "").lower()
        return "топ чатов" in source or "супер топ бесед" in source

    def _is_day_top_response(self, text: str) -> bool:
        source = (text or "").lower()
        return "топ дня" in source and "бесед" in source

    def _localize_profile_value(self, value: str, field: str) -> str:
        lang = self._lang_code()
        normalized = html.unescape((value or "").strip()).lower()
        maps = {
            "tiktok": {
                "status": {
                    "был недавно": "недавно в сети",
                    "в сети": "в сети ща",
                },
                "gender": {
                    "не указан": "не пробит",
                    "не указана": "не пробит",
                },
                "birth": {
                    "не указан": "не пробит",
                    "не указана": "не пробит",
                },
                "city": {
                    "не указан": "не пробит",
                    "не указана": "не пробит",
                },
                "rank": {
                    "🙂 ноунейм (0)": "🙂 ноунейм skid (0)",
                },
            },
            "leet": {
                "status": {
                    "был недавно": "r3c3n7ly 0nl1n3",
                    "в сети": "0nl1n3",
                },
                "gender": {
                    "не указан": "n07 537",
                    "не указана": "n07 537",
                },
                "birth": {
                    "не указан": "n07 537",
                    "не указана": "n07 537",
                },
                "city": {
                    "не указан": "n07 537",
                    "не указана": "n07 537",
                },
                "rank": {
                    "🙂 ноунейм (0)": "🙂 n0n4m3 (0)",
                },
            },
            "uwu": {
                "status": {
                    "был недавно": "wecentwy onwine",
                    "в сети": "onwine uwu",
                },
                "gender": {
                    "не указан": "not set uwu",
                    "не указана": "not set uwu",
                },
                "birth": {
                    "не указан": "not set uwu",
                    "не указана": "not set uwu",
                },
                "city": {
                    "не указан": "not set uwu",
                    "не указана": "not set uwu",
                },
                "rank": {
                    "🙂 ноунейм (0)": "🙂 no-name uwu (0)",
                },
            },
            "en": {
                "status": {
                    "был недавно": "recently online",
                    "в сети": "online",
                },
                "gender": {
                    "не указан": "not set",
                    "не указана": "not set",
                },
                "birth": {
                    "не указан": "not set",
                    "не указана": "not set",
                },
                "city": {
                    "не указан": "not set",
                    "не указана": "not set",
                },
                "rank": {
                    "🙂 ноунейм (0)": "🙂 No-name (0)",
                },
            },
            "de": {
                "status": {
                    "был недавно": "kürzlich online",
                    "в сети": "online",
                },
                "gender": {
                    "не указан": "nicht angegeben",
                    "не указана": "nicht angegeben",
                },
                "birth": {
                    "не указан": "nicht angegeben",
                    "не указана": "nicht angegeben",
                },
                "city": {
                    "не указан": "nicht angegeben",
                    "не указана": "nicht angegeben",
                },
                "rank": {
                    "🙂 ноунейм (0)": "🙂 No-Name (0)",
                },
            },
            "jp": {
                "status": {
                    "был недавно": "最近オンライン",
                    "в сети": "オンライン",
                },
                "gender": {
                    "не указан": "未設定",
                    "не указана": "未設定",
                },
                "birth": {
                    "не указан": "未設定",
                    "не указана": "未設定",
                },
                "city": {
                    "не указан": "未設定",
                    "не указана": "未設定",
                },
                "rank": {
                    "🙂 ноунейм (0)": "🙂 ノーネーム (0)",
                },
            },
            "uk": {
                "status": {
                    "был недавно": "був нещодавно",
                    "в сети": "онлайн",
                },
                "gender": {
                    "не указан": "не вказано",
                    "не указана": "не вказано",
                },
                "birth": {
                    "не указан": "не вказано",
                    "не указана": "не вказано",
                },
                "city": {
                    "не указан": "не вказано",
                    "не указана": "не вказано",
                },
                "rank": {
                    "🙂 ноунейм (0)": "🙂 Нонейм (0)",
                },
            },
        }
        return html.escape(maps.get(lang, {}).get(field, {}).get(normalized, value))

    def _profile_map(self, text: str) -> dict[str, str]:
        source = text or ""
        data = {
            "name": self.strings["unknown"],
            "status": self.strings["unknown"],
            "handle": self.strings["unknown"],
            "since_date": self.strings["unknown"],
            "since_span": self.strings["unknown"],
            "gender": self.strings["unknown"],
            "birth": self.strings["unknown"],
            "city": self.strings["unknown"],
            "day": "0",
            "week": "0",
            "month": "0",
            "total": "0",
            "rank": self.strings["unknown"],
            "candies": "0",
            "stars": "0",
            "icoins": "0",
            "gold": "0",
            "tg_stars": "0",
        }
        who = re.search(r"👤\s*(?:Это|It'?s)\s+(.+?)\s+\((.+?)\)", source, re.IGNORECASE)
        if who:
            data["name"] = html.escape(who.group(1).strip())
            data["status"] = self._localize_profile_value(who.group(2).strip(), "status")
        handle = re.search(r"🆔\s*([^\n\r]+)", source)
        if handle:
            data["handle"] = html.escape(handle.group(1).strip())
        since = re.search(r"Во вселенной [^:]+:\s*с\s*([0-9.]+)\s*\(([^)]+)\)", source)
        if since:
            data["since_date"] = html.escape(since.group(1).strip())
            data["since_span"] = html.escape(since.group(2).strip())
        gender = re.search(r"Пол:\s*([^\n\r]+)", source)
        if gender:
            data["gender"] = self._localize_profile_value(gender.group(1).strip(), "gender")
        birth = re.search(r"Дата рождения:\s*([^\n\r]+)", source)
        if birth:
            data["birth"] = self._localize_profile_value(birth.group(1).strip(), "birth")
        city = re.search(r"Город:\s*([^\n\r]+)", source)
        if city:
            data["city"] = self._localize_profile_value(city.group(1).strip(), "city")
        activity = re.search(r"Активность.*?:\s*([0-9 ]+)\s*\|\s*([0-9 ]+)\s*\|\s*([0-9 ]+)\s*\|\s*([0-9 ]+)", source)
        if activity:
            data["day"] = activity.group(1).strip()
            data["week"] = activity.group(2).strip()
            data["month"] = activity.group(3).strip()
            data["total"] = activity.group(4).strip()
        rank = re.search(r"Зв[её]здность:\s*([^\n\r]+)", source)
        if rank:
            data["rank"] = self._localize_profile_value(rank.group(1).strip(), "rank")
        candies = re.search(r"🍬\s*([0-9 ]+)", source)
        if candies:
            data["candies"] = candies.group(1).strip()
        stars = re.search(r"✨\s*([0-9 ]+)\s*(?:зв[её]здочек|stars?)", source, re.IGNORECASE)
        if stars:
            data["stars"] = stars.group(1).strip()
        icoins = re.search(r"☢️\s*([0-9 ]+)\s*i[¢c]", source, re.IGNORECASE)
        if icoins:
            data["icoins"] = icoins.group(1).strip()
        gold = re.search(r"🌕\s*([0-9 ]+)", source)
        if gold:
            data["gold"] = gold.group(1).strip()
        tg_stars = re.search(r"⭐️\s*([0-9 ]+)\s*(?:tg[- ]?зв[её]зд|tg[- ]?stars?)", source, re.IGNORECASE)
        if tg_stars:
            data["tg_stars"] = tg_stars.group(1).strip()
        return data

    def _render_bag(self, bot: str, text: str) -> str:
        source = text or ""
        owner = re.search(r"В мешке\s+(.+?):", source)
        candies = re.search(r"🍬\s*([0-9 ]+)", source)
        gold = re.search(r"🌕\s*([0-9 ]+)", source)
        icoins = re.search(r"☢️\s*([0-9 ]+)\s*i[¢c]", source)
        stars = re.search(r"✨\s*([0-9 ]+)", source)
        tg_stars = re.search(r"⭐️\s*([0-9 ]+)", source)
        if owner and candies and gold and icoins and stars:
            return (
                f"{self.strings['bag_title'].format(bot=bot)}\n\n"
                + self.strings["bag_text"].format(
                    owner=html.escape(owner.group(1).strip()),
                    candies=candies.group(1).strip(),
                    gold=gold.group(1).strip(),
                    icoins=icoins.group(1).strip(),
                    stars=stars.group(1).strip(),
                    tg_stars=tg_stars.group(1).strip() if tg_stars else "0",
                )
            )
        cleaned = self._clean_iris_text(source)
        return f"{self.strings['bag_title'].format(bot=bot)}\n\n{cleaned or self.strings['unknown']}"

    def _render_profile(self, bot: str, text: str) -> str:
        data = self._profile_map(text)
        if data["name"] == self.strings["unknown"] and data["handle"] == self.strings["unknown"]:
            cleaned = self._clean_iris_text(text)
            return f"{self.strings['profile_title'].format(bot=bot)}\n\n{cleaned or self.strings['profile_empty']}"
        return self.strings["profile_text"].format(
            title=self.strings["profile_title"].format(bot=bot),
            universe_label=self.strings.get("profile_universe", "Во вселенной Iris"),
            **data,
        )

    def _parse_top_entries(self, source_html: str) -> list[dict[str, str]]:
        entries = []
        html_lines = (source_html or "").splitlines()
        current = None
        for html_line in html_lines:
            line = self._plain_text(html_line)
            if not line:
                continue
            top_match = re.match(r"🔸\s*(\d+)\.\s*«(.+?)»\s*\(([0-9 ][0-9 ]*)\s*i[¢c]\)", line, re.IGNORECASE)
            if top_match:
                if current:
                    entries.append(current)
                current = {
                    "place": top_match.group(1).strip(),
                    "name": html.escape(top_match.group(2).strip()),
                    "coins": top_match.group(3).strip(),
                    "join_kind": "",
                    "join_name": "",
                    "join_url": "",
                    "card_url": "",
                }
                continue
            if not current:
                continue
            if "войти в чат" in line.lower():
                current["join_kind"] = "open"
                current["join_url"] = self._extract_href(html_line)
                continue
            apply = re.search(r"попроситься в чат:\s*(.+)", line, re.IGNORECASE)
            if apply:
                current["join_kind"] = "apply"
                current["join_name"] = html.escape(apply.group(1).strip())
                current["join_url"] = self._extract_href(html_line)
                continue
            if "карточка" in line.lower():
                current["card_url"] = self._extract_href(html_line)
        if current:
            entries.append(current)
        return entries

    def _make_top_link(self, emoji: str, label: str, url: str) -> str:
        rendered = f'<a href="{url}">{label}</a>' if url else label
        return f"{emoji} <b>{rendered}</b>\n"

    def _render_top(self, bot: str, source_html: str, title_key: str) -> str:
        entries = self._parse_top_entries(source_html)
        if not entries:
            cleaned = self._clean_iris_text(self._plain_text(source_html))
            return f"{self.strings[title_key].format(bot=bot)}\n\n{cleaned or self.strings['top_empty']}"
        blocks = []
        for entry in entries:
            join_line = ""
            if entry["join_kind"] == "open":
                join_line = self._make_top_link("🚪", self.strings["top_join_open"], entry["join_url"])
            elif entry["join_kind"] == "apply":
                join_line = self._make_top_link(
                    "🙋",
                    self.strings["top_join_apply"].format(name=entry["join_name"]),
                    entry["join_url"],
                )
            card_line = self._make_top_link("🗂", self.strings["top_card"], entry["card_url"]) if entry["card_url"] else ""
            blocks.append(
                self.strings["top_entry"].format(
                    place=entry["place"],
                    name=entry["name"],
                    coins=entry["coins"],
                    join_line=join_line,
                    card_line=card_line,
                ).strip()
            )
        return f"{self.strings[title_key].format(bot=bot)}\n\n" + "\n\n".join(blocks)

    @loader.command(
        ru_doc="<on/off> | автофарм iris",
        en_doc="<on/off> | iris autofarm",
        uk_doc="<on/off> | автофарм iris",
        de_doc="<on/off> | iris autofarm",
        jp_doc="<on/off> | Iris自動ファーム",
        neofit_doc="<on/off> | фарм iris",
        tiktok_doc="<on/off> | iris farm toggle",
        leet_doc="<0n/0ff> | 1r15 4ut0f4rm",
        uwu_doc="<on/off> | iwis autofawm",
    )
    async def irf(self, message: Message):
        args = utils.get_args_raw(message).strip().lower()
        start = args in {"", "on", "start", "старт"}
        stop = args in {"off", "stop", "стоп"}
        if not args:
            start = not self._running
            stop = self._running
        if stop:
            if not self._running:
                await utils.answer(message, self.strings["farm_stopped_already"])
                return
            await utils.answer(message, self.strings["loading_reset"])
            self._running = False
            if self._farm_task:
                self._farm_task.cancel()
                self._farm_task = None
            if self._load_task:
                self._load_task.cancel()
                self._load_task = None
            self._session_count = 0
            self._cooldown_until = 0.0
            self._next_run_at = 0.0
            self._save_runtime_state()
            await self._mute_all(False)
            await self._cleanup_all()
            self._add_log("⛔ Manual stop")
            await utils.answer(message, self.strings["farm_stopping"])
            return
        if self._running:
            await utils.answer(message, self.strings["farm_already"])
            return
        self._running = True
        self._start_ts = self._now_ts()
        self._session_count = 0
        self._cooldown_until = 0.0
        self._next_run_at = 0.0
        self.db.set(self.strings["name"], "session_icoins", 0)
        self._save_runtime_state()
        await self._mute_all(True)
        await utils.answer(message, self.strings["checking_load"])
        best_bot, best_load = await self._get_best_bot()
        self._active_bot = best_bot
        self._save_load_cache()
        self._farm_task = asyncio.create_task(self._farm_loop())
        self._load_task = asyncio.create_task(self._load_refresh_loop())
        self._add_log(f"🚀 Started with {best_bot} ({best_load:.1f}%)")
        await utils.answer(
            message,
            self.strings["farm_started"].format(
                bot=best_bot,
                load=f"{best_load:.1f}" if best_load >= 0 else "?",
                first=self.strings["start_now"],
                tz=self.config["timezone_offset"],
            ),
        )

    @loader.command(
        ru_doc="| статус iris",
        en_doc="| iris status",
        uk_doc="| статус iris",
        de_doc="| iris status",
        jp_doc="| Irisステータス",
        neofit_doc="| статус iris",
        tiktok_doc="| skid stat iris",
        leet_doc="| 1r15 5747u5",
        uwu_doc="| iwis status",
    )
    async def iris(self, message: Message):
        msg = await utils.answer(message, self.strings["loading_status"])
        cooldown_left, cooldown_at = self._schedule_text(self._cooldown_until)
        planned_left, planned_at = self._schedule_text(self._next_run_at)
        uptime = self._format_td(self._now_ts() - self._start_ts) if self._start_ts else self.strings["unknown"]
        state = self.strings["state_on"] if self._running else self.strings["state_off"]
        active_bot = self._active_bot or self.strings["unknown"]
        if active_bot == IRIS_BOTS[0] and self._bot_loads:
            active_bot = self._best_bot()[0]
        text = self.strings["status"].format(
            title=self.strings["status_title"],
            state=state,
            uptime=uptime,
            session=self.db.get(self.strings["name"], "session_icoins", 0),
            total=self.db.get(self.strings["name"], "total_icoins", 0),
            count=self._session_count,
            tz=self.config["timezone_offset"],
            last_farm=self._last_farm_text(),
            cooldown_left=cooldown_left,
            cooldown_at=cooldown_at,
            planned_left=planned_left,
            planned_at=planned_at,
            active_bot=active_bot,
            loads=self._render_loads(),
        )
        await utils.answer(msg, text)

    @loader.command(
        ru_doc="<count/date> | логи фарма",
        en_doc="<count/date> | farm logs",
        uk_doc="<count/date> | логи фарму",
        de_doc="<count/date> | farm logs",
        jp_doc="<count/date> | ファームログ",
        neofit_doc="<count/date> | логи iris",
        tiktok_doc="<count/date> | iris log dump",
        leet_doc="<c0un7/d473> | f4rm l0g5",
        uwu_doc="<count/date> | fawm logs",
    )
    async def irlogs(self, message: Message):
        msg = await utils.answer(message, self.strings["loading_logs"])
        args = utils.get_args_raw(message).strip()
        logs = list(reversed(self.db.get(self.strings["name"], "logs", [])))
        if not args:
            logs = logs[:8]
        elif re.fullmatch(r"\d{1,3}", args):
            logs = logs[: max(1, min(self.config["max_logs"], int(args)))]
        elif re.fullmatch(r"\d{2}\.\d{2}", args):
            logs = [entry for entry in logs if args in entry]
        else:
            await utils.answer(msg, self.strings["logs_bad_arg"])
            return
        if not logs:
            await utils.answer(msg, self.strings["logs_empty"])
            return
        await utils.answer(msg, f"{self.strings['logs_title'].format(count=len(logs))}\n\n" + "\n".join(logs))

    @loader.command(
        ru_doc="| мешок",
        en_doc="| bag",
        uk_doc="| мішок",
        de_doc="| bag",
        jp_doc="| バッグ",
        neofit_doc="| мешок",
        tiktok_doc="| bag flex",
        leet_doc="| b46",
        uwu_doc="| baggy",
    )
    async def irbag(self, message: Message):
        msg = await utils.answer(message, self.strings["loading_bag"])
        bot, _ = await self._pick_request_bot()
        try:
            await self._mute_bot(bot, True)
            resp, _ = await self._request_bot(bot, "Мешок")
            if not resp:
                await utils.answer(msg, self.strings["no_response"].format(bot=bot))
                return
            await utils.answer(msg, self._render_bag(bot, resp.raw_text or resp.text or ""))
        except Exception as e:
            await utils.answer(msg, self.strings["bag_error"].format(bot=bot, err=html.escape(str(e))))
        finally:
            await self._mute_bot(bot, False)

    @loader.command(
        ru_doc="| анкета",
        en_doc="| profile",
        uk_doc="| анкета",
        de_doc="| profil",
        jp_doc="| プロフィール",
        neofit_doc="| анкета",
        tiktok_doc="| profile core",
        leet_doc="| pr0f1l3",
        uwu_doc="| pwofiwe",
    )
    async def irprofile(self, message: Message):
        msg = await utils.answer(message, self.strings["loading_profile"])
        bot, _ = await self._pick_request_bot()
        try:
            await self._mute_bot(bot, True)
            resp, _ = await self._request_bot(bot, "Анкета")
            if not resp:
                await utils.answer(msg, self.strings["no_response"].format(bot=bot))
                return
            await utils.answer(msg, self._render_profile(bot, resp.raw_text or resp.text or ""))
        except Exception as e:
            await utils.answer(msg, self.strings["profile_error"].format(bot=bot, err=html.escape(str(e))))
        finally:
            await self._mute_bot(bot, False)

    @loader.command(
        ru_doc="| супер топ",
        en_doc="| super top",
        uk_doc="| супер топ",
        de_doc="| super top",
        jp_doc="| スーパー top",
        neofit_doc="| супер топ",
        tiktok_doc="| super top era",
        leet_doc="| 5up3r 70p",
        uwu_doc="| supew top",
    )
    async def irsupertop(self, message: Message):
        msg = await utils.answer(message, self.strings["loading_super_top"])
        bot, _ = await self._pick_request_bot()
        try:
            await self._mute_bot(bot, True)
            resp, _ = await self._request_bot(
                bot,
                "🏆 Супер Топ бесед",
                matcher=lambda m: self._is_super_top_response(m.raw_text or m.text or ""),
            )
            if not resp:
                await utils.answer(msg, self.strings["no_response"].format(bot=bot))
                return
            await utils.answer(msg, self._render_top(bot, self._message_html(resp), "super_top_title"))
        except Exception as e:
            await utils.answer(msg, self.strings["bag_error"].format(bot=bot, err=html.escape(str(e))))
        finally:
            await self._mute_bot(bot, False)

    @loader.command(
        ru_doc="| топ дня",
        en_doc="| day top",
        uk_doc="| топ дня",
        de_doc="| tages top",
        jp_doc="| 日間 top",
        neofit_doc="| топ дня",
        tiktok_doc="| day top era",
        leet_doc="| d4y 70p",
        uwu_doc="| day top",
    )
    async def irdaytop(self, message: Message):
        msg = await utils.answer(message, self.strings["loading_day_top"])
        bot, _ = await self._pick_request_bot()
        try:
            await self._mute_bot(bot, True)
            resp, _ = await self._request_bot(
                bot,
                "🏅 Беседы: Топ дня",
                matcher=lambda m: self._is_day_top_response(m.raw_text or m.text or ""),
            )
            if not resp:
                await utils.answer(msg, self.strings["no_response"].format(bot=bot))
                return
            await utils.answer(msg, self._render_top(bot, self._message_html(resp), "day_top_title"))
        except Exception as e:
            await utils.answer(msg, self.strings["bag_error"].format(bot=bot, err=html.escape(str(e))))
        finally:
            await self._mute_bot(bot, False)

    @loader.command(
        ru_doc="| сброс data",
        en_doc="| reset data",
        uk_doc="| скинути data",
        de_doc="| reset data",
        jp_doc="| データリセット",
        neofit_doc="| reset iris",
        tiktok_doc="| вайп data",
        leet_doc="| r3537 d474",
        uwu_doc="| weset data",
    )
    async def irreset(self, message: Message):
        msg = await utils.answer(message, self.strings["loading_reset"])
        self._logs.clear()
        self._bot_loads.clear()
        self._bot_loads_updated = 0.0
        self._active_bot = IRIS_BOTS[0]
        self._start_ts = 0.0
        self._cooldown_until = 0.0
        self._next_run_at = 0.0
        self._last_farm_ts = 0.0
        self._last_farm_bot = ""
        self._last_farm_coins = 0
        self._session_count = 0
        self._running = False
        self.db.set(self.strings["name"], "total_icoins", 0)
        self.db.set(self.strings["name"], "session_icoins", 0)
        self.db.set(self.strings["name"], "logs", [])
        self._save_load_cache()
        self._save_runtime_state()
        await utils.answer(msg, self.strings["reset_done"])

    async def watcher(self, message: Message):
        if not self._running or not self._watcher_resync or message.out:
            return
        try:
            sender = await message.get_sender()
            username = (getattr(sender, "username", "") or "").lower()
        except Exception:
            return
        if username not in IRIS_BOTS:
            return
        text = message.raw_text or ""
        upper = text.upper()
        if "ЗАЧЁТ" in upper and "НЕЗАЧЁТ" not in upper:
            new_cooldown = self._now_ts() + BASE_COOLDOWN
            if abs(new_cooldown - self._cooldown_until) > 60:
                self._cooldown_until = new_cooldown
                self._next_run_at = new_cooldown + self._extra_delay()
                self._save_runtime_state()
                self._add_log(f"🔁 Watcher synced success from {username}")
        elif "НЕЗАЧЁТ" in upper:
            seconds = _parse_cooldown_seconds(text)
            if seconds:
                new_cooldown = self._now_ts() + seconds
                if abs(new_cooldown - self._cooldown_until) > 60:
                    self._cooldown_until = new_cooldown
                    self._next_run_at = new_cooldown + self._extra_delay()
                    self._save_runtime_state()
                    self._add_log(f"🔁 Watcher synced cooldown from {username}: {self._format_td(seconds)}")
