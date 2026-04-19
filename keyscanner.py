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

__version__ = (2, 1)
import re
import aiohttp
import asyncio
import json
import io
from urllib.parse import urlparse, unquote
from herokutl.types import Message
from herokutl.tl.functions.messages import CreateForumTopicRequest, GetForumTopicsByIDRequest, GetForumTopicsRequest
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

LOADING_TEXT = f"{E_BOX2} <b>Загрузка...</b>"

@loader.tds
class KeyScanner(loader.Module):
    """Spizdi ALL AI API KEYS in your chat"""

    strings = {
        "name": "KeyScanner",
        "scanning":      f"{E_SLOW} <b>Fast scanning via search...</b>\n{E_FOLD} Searching up to {{limit}} messages per prefix.",
        "found":         f"{E_OK} <b>Scan complete!</b>\n{E_FIRE} Valid keys found: <b>{{valid_count}}</b>\n{E_BATT} Saved to database.",
        "auto_on":       f"{E_BELL} Auto-scan <b>enabled</b> for this chat.\n{E_MSG} Catching: new messages · edits · files",
        "auto_off":      f"{E_MUTE} Auto-scan <b>disabled</b> for this chat.",
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
        "list_title":    f"{E_LIST} <b>Keys List (Page {{page}}/{{total_pages}}):</b>",
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
        "import_err":    f"{E_ERR} Reply to a TXT/JSON file or provide a raw URL.",
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
        "btn_sort_paid_free": "💳 Sort Paid / Free",
        "btn_del_free":       "🗑 Delete Free",
        "btn_del_paid":       "🗑 Delete Paid",
        "btn_exp_paid":       "💳 Export Paid",
        "btn_exp_free":       "🔋 Export Free",
        "sorting":       f"{E_SYNC} <b>Sorting keys by paid/free...</b>\n{{done}}/{{total}}",
        "sort_done":     f"{E_OK} <b>Sort complete!</b>\n\n{E_CARD} <b>Paid:</b> {{paid}}\n{E_BATT} <b>Free:</b> {{free}}\n❓ <b>Unknown:</b> {{unknown}}",
        "deleted_filter": f"{E_TRASH} Deleted <b>{{count}}</b> keys.",
    }

    strings_ru = {
        "scanning":      f"{E_SLOW} <b>Быстрый поиск ключей...</b>\n{E_FOLD} Поиск до {{limit}} сообщений на префикс.",
        "found":         f"{E_OK} <b>Сканирование завершено!</b>\n{E_FIRE} Новых валидных ключей: <b>{{valid_count}}</b>\n{E_BATT} Сохранено.",
        "auto_on":       f"{E_BELL} Авто-ловля <b>включена</b>.\n{E_MSG} Ловлю: новые сообщения · правки · файлы",
        "auto_off":      f"{E_MUTE} Авто-ловля <b>выключена</b>.",
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
        "list_title":    f"{E_LIST} <b>Список (Стр. {{page}}/{{total_pages}}):</b>",
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
        "import_err":    f"{E_ERR} Сделайте реплай на файл или укажите ссылку на raw.",
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
        "btn_sort_paid_free": "💳 Сортировать Платн/Беспл",
        "btn_del_free":       "🗑 Удалить бесплатные",
        "btn_del_paid":       "🗑 Удалить платные",
        "btn_exp_paid":       "💳 Выгрузить платные",
        "btn_exp_free":       "🔋 Выгрузить бесплатные",
        "sorting":       f"{E_SYNC} <b>Сортировка платные/бесплатные...</b>\n{{done}}/{{total}}",
        "sort_done":     f"{E_OK} <b>Сортировка завершена!</b>\n\n{E_CARD} <b>Платных:</b> {{paid}}\n{E_BATT} <b>Бесплатных:</b> {{free}}\n❓ <b>Неизвестно:</b> {{unknown}}",
        "deleted_filter": f"{E_TRASH} Удалено <b>{{count}}</b> ключей.",
    }

    def __init__(self):
        self.key_regex = re.compile(
            r"\b("
            r"sk-[a-zA-Z0-9\-_]{20,}|"
            r"sk-proj-[a-zA-Z0-9\-_]{20,}|"
            r"sk-ant-api[a-zA-Z0-9\-_]{50,}|"
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

    async def client_ready(self, client, db):
        self.client       = client
        self._client      = client
        self._db          = db
        self._keys        = self.get("keys_v2", {})
        self._auto_chats  = self.get("auto_v2", [])
        self._paid_status = self.get("paid_status", {})
        self._model_cache = self.get("models_v2", {})
        if not isinstance(self._model_cache, dict):
            self._model_cache = {}
        self._settings    = self.get("ks_settings", {
            "log_mode":  "none",
            "file_scan": True,
            "edit_scan": True,
        })


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

    def _get_main_markup(self):
        return [
            [
                self._btn(self.strings["btn_list"], self.ks_list, (0, "all"), "primary"),
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
                self._btn(self.strings["btn_settings"], self.ks_settings_menu, style="primary"),
                self._btn(self.strings["btn_clear"], self.ks_clr_menu, style="danger"),
            ],
        ]

    def _filtered_keys(self, filter_mode: str) -> dict:
        if filter_mode == "paid":
            return {k: v for k, v in self._keys.items() if self._paid_status.get(k) == "paid"}
        if filter_mode == "free":
            return {k: v for k, v in self._keys.items() if self._paid_status.get(k) == "free"}
        return dict(self._keys)

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
                create_result = await self.client(
                    CreateForumTopicRequest(
                        peer=entity,
                        title=title,
                        icon_emoji_id=5386397724338490996 if getattr(getattr(self.client, "heroku_me", None), "premium", False) else None,
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
        return None

    async def _register_key(self, session, key: str, provider: str, source_chat_id, via: str = "message"):
        models = await self._discover_models(session, key, provider)
        tier = self._tier_from_models(provider, models)
        if tier is None:
            tier = await self._check_paid(session, key, provider, models=models)
        self._keys[key] = provider
        self._paid_status[key] = tier
        if models:
            self._ensure_model_cache()[key] = models
        else:
            self._ensure_model_cache().pop(key, None)
        await self._handle_new_key(key, provider, source_chat_id, via=via)

    async def _handle_new_key(self, key: str, provider: str, source_chat_id, via: str = "message"):
        mode = self._settings.get("log_mode", "none")
        if mode == "none":
            return
        text = self.strings["new_key_notif"].format(
            provider=provider, key=key, chat_id=source_chat_id, via=via
        )
        await self._send_log_text(text)

    async def _gather_chunked(self, tasks, chunk_size: int = 30):
        res = []
        for i in range(0, len(tasks), chunk_size):
            res.extend(await asyncio.gather(*tasks[i:i + chunk_size]))
            await asyncio.sleep(0.3)
        return res

    async def _process_text(self, text: str, chat_id, via: str = "message") -> int:
        """Extract, validate and store new keys from arbitrary text. Returns new-key count."""
        matches  = self.key_regex.findall(text)
        new_keys = [k for k in set(matches) if k not in self._keys]
        if not new_keys:
            return 0
        count = 0
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
                    return "Gemini", r.status == 200

            elif key.startswith("sk-ant-"):
                ant_h = {"x-api-key": key, "anthropic-version": "2023-06-01", "content-type": "application/json"}
                data  = {"model": "claude-3-haiku-20240307", "max_tokens": 1, "messages": [{"role": "user", "content": "a"}]}
                async with session.post("https://api.anthropic.com/v1/messages",
                                        headers=ant_h, json=data, timeout=5) as r:
                    return "Anthropic", r.status == 200

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
                providers = [
                    ("OpenAI",      "https://api.openai.com/v1",                 "gpt-4o-mini"),
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
                async with session.get(
                    "https://api.openai.com/v1/dashboard/billing/subscription",
                    headers=headers, timeout=5,
                ) as r:
                    if r.status == 200:
                        d      = await r.json()
                        has_pm = d.get("has_payment_method", False)
                        plan   = d.get("plan", {}).get("id", "")
                        soft   = d.get("soft_limit_usd", 0)
                        return "paid" if (has_pm or plan not in ("", "free") or soft > 0) else "free"
                    return "free" if r.status == 403 else "unknown"

            elif provider == "Anthropic" or key.startswith("sk-ant-"):
                ant_h = {"x-api-key": key, "anthropic-version": "2023-06-01"}
                async with session.get("https://api.anthropic.com/v1/organizations",
                                       headers=ant_h, timeout=5) as r:
                    if r.status == 200:
                        d = await r.json()
                        for org in d.get("data", []):
                            if org.get("billing_type", "") not in ("free_tier", ""):
                                return "paid"
                        return "free"

            elif provider == "OpenRouter" or key.startswith("sk-or-v1-"):
                async with session.get("https://openrouter.ai/api/v1/auth/key",
                                       headers=headers, timeout=5) as r:
                    if r.status == 200:
                        d       = await r.json()
                        credits = d.get("data", {}).get("limit", None)
                        is_free = d.get("data", {}).get("is_free_tier", True)
                        return "paid" if (not is_free or (credits and credits > 1)) else "free"

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

    
    @loader.command(
        ru_doc="[лимит] - Поиск ключей через поиск сообщений.",
        en_doc="[limit] - Fast key scan via Telegram search.",
    )
    async def scanllm(self, message: Message):
        args  = utils.get_args_raw(message)
        limit = int(args) if args.isdigit() else 500
        msg   = await utils.answer(message, self.strings["scanning"].format(limit=limit))
        found = set()
        for query in self.search_queries:
            try:
                async for m in self.client.iter_messages(message.to_id, search=query, limit=limit):
                    if getattr(m, "raw_text", None):
                        found.update(self.key_regex.findall(m.raw_text))
            except FloodWaitError as e:
                wait = getattr(e, "seconds", None) or getattr(e, "x", 5)
                await asyncio.sleep(int(wait))
                try:
                    async for m in self.client.iter_messages(message.to_id, search=query, limit=limit):
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
                tasks   = [self._validate_key(session, k) for k in found]
                results = await self._gather_chunked(tasks)
                for key, (prov, ok) in zip(found, results):
                    if ok and key not in self._keys:
                        valid_count += 1
                        await self._register_key(session, key, prov,
                            getattr(message.to_id, "chat_id", "ScanLLM"), via="scan")
            self._save()
        await utils.answer(msg, self.strings["found"].format(valid_count=valid_count))

    @loader.command(
        ru_doc="[лимит] - Глобальный поиск ключей по всем диалогам.",
        en_doc="[limit] - Global key scan across all dialogs.",
    )
    async def scanglobal(self, message: Message):
        args  = utils.get_args_raw(message)
        limit = int(args) if args.isdigit() else 100
        msg   = await utils.answer(message, self.strings["global_scanning"].format(limit=limit))
        found = set()
        for query in self.search_queries:
            try:
                async for m in self.client.iter_messages(None, search=query, limit=limit):
                    if getattr(m, "raw_text", None):
                        found.update(self.key_regex.findall(m.raw_text))
            except FloodWaitError as e:
                wait = getattr(e, "seconds", None) or getattr(e, "x", 5)
                await asyncio.sleep(int(wait))
                try:
                    async for m in self.client.iter_messages(None, search=query, limit=limit):
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
                tasks   = [self._validate_key(session, k) for k in found]
                results = await self._gather_chunked(tasks)
                for key, (prov, ok) in zip(found, results):
                    if ok and key not in self._keys:
                        valid_count += 1
                        await self._register_key(session, key, prov, "Global Scan", via="global")
            self._save()
        await utils.answer(msg, self.strings["found"].format(valid_count=valid_count))

    @loader.command(ru_doc="Вкл/выкл авто-ловлю", en_doc="Toggle auto-scan")
    async def autokeys(self, message: Message):
        cid = message.chat_id
        if cid in self._auto_chats:
            self._auto_chats.remove(cid)
            await utils.answer(message, self.strings["auto_off"])
        else:
            self._auto_chats.append(cid)
            await utils.answer(message, self.strings["auto_on"])
            if self._settings.get("log_mode") == "heroku":
                try:
                    await self._bootstrap_heroku_logs()
                except Exception:
                    pass
        self._save()

    @loader.command(ru_doc="Переключить режим логирования", en_doc="Cycle log mode")
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

    @loader.command(ru_doc="Удалить все невалидные ключи", en_doc="Remove all invalid keys")
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

    @loader.command(ru_doc="Меню ключей", en_doc="Keys menu")
    async def mykeys(self, message: Message):
        if not self._keys:
            return await utils.answer(message, self.strings["empty"])

        form = await self.inline.form(
            text=LOADING_TEXT,
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
        if cid not in self._auto_chats:
            return

        text = getattr(message, "raw_text", None) or ""
        if text:
            asyncio.create_task(self._process_text(text, cid, via="message"))

        if self._settings.get("file_scan", True) and getattr(message, "file", None):
            mime = getattr(message.file, "mime_type", "") or ""
            name = (getattr(message.file, "name", "") or "").lower()
            TEXT_EXTS = (".txt", ".json", ".env", ".py", ".js", ".ts", ".sh",
                         ".yaml", ".yml", ".toml", ".ini", ".cfg", ".log", ".md",
                         ".xml", ".csv", ".conf", ".properties")
            TEXT_MIMES = ("text/", "application/json", "application/x-yaml",
                          "application/xml", "application/x-sh")
            is_text = any(mime.startswith(m) for m in TEXT_MIMES) or any(name.endswith(e) for e in TEXT_EXTS)
            if is_text:
                async def _scan_file(msg=message):
                    try:
                        raw = await self.client.download_media(msg, bytes)
                        if raw:
                            await self._process_text(raw.decode("utf-8", errors="ignore"), cid, via="file")
                    except Exception:
                        pass
                asyncio.create_task(_scan_file())

    @loader.watcher()
    async def edit_watcher(self, message: Message):
        """Catch keys in edited messages with 150 ms debounce — near-instant, zero flood."""
        if not self._settings.get("edit_scan", True):
            return
        cid = getattr(message, "chat_id", None)
        if cid not in self._auto_chats:
            return
        if not getattr(message, "edit_date", None):
            return
        text = getattr(message, "raw_text", None) or ""
        if not text:
            return

        slot = f"{cid}:{getattr(message, 'id', 0)}"
        old  = self._edit_tasks.get(slot)
        if old and not old.done():
            old.cancel()

        async def _debounced(t=text, c=cid, s=slot):
            await asyncio.sleep(0.15)
            await self._process_text(t, c, via="edit")
            self._edit_tasks.pop(s, None)

        self._edit_tasks[slot] = asyncio.create_task(_debounced())

    async def ks_list(self, call, page, filter_mode="all"):
        all_keys = sorted(self._keys.keys())
        if filter_mode == "paid":
            keys_list = [k for k in all_keys if self._paid_status.get(k) == "paid"]
        elif filter_mode == "free":
            keys_list = [k for k in all_keys if self._paid_status.get(k) == "free"]
        else:
            keys_list   = all_keys
            filter_mode = "all"

        per_page    = 6
        total_pages = max(1, (len(keys_list) + per_page - 1) // per_page)
        page        = max(0, min(page, total_pages - 1))
        start       = page * per_page
        cur_keys    = keys_list[start:start + per_page]

        markup = [[
            self._btn(("✅ " if filter_mode == "all"  else "") + self.strings["btn_filter_all"],  self.ks_list, (0, "all"), "primary" if filter_mode == "all" else None),
            self._btn(("✅ " if filter_mode == "paid" else "") + self.strings["btn_filter_paid"], self.ks_list, (0, "paid"), "success" if filter_mode == "paid" else None),
            self._btn(("✅ " if filter_mode == "free" else "") + self.strings["btn_filter_free"], self.ks_list, (0, "free"), "danger" if filter_mode == "free" else None),
        ]]
        for k in cur_keys:
            idx     = all_keys.index(k)
            prov    = self._keys[k]
            tier_ic = {"paid": "💳", "free": "🆓"}.get(self._paid_status.get(k, ""), "❓")
            short   = f"{k[:4]}{'*'*8}{k[-4:]}" if len(k) > 12 else f"{k[:2]}***{k[-2:]}"
            markup.append([{"text": f"{tier_ic} [{prov}] {short}", "callback": self.ks_key_menu, "args": (idx, True)}])
        if total_pages > 1:
            markup.append([
                self._btn("◀️", self.ks_list, (page - 1, filter_mode), "primary"),
                self._btn(f"{page + 1}/{total_pages}", self.ks_list, (page, filter_mode), "success"),
                self._btn("▶️", self.ks_list, (page + 1, filter_mode), "primary"),
            ])
        markup.append([self._btn(self.strings["btn_back"], self.ks_back, style="primary")])
        await call.edit(
            text=self.strings["list_title"].format(page=page + 1, total_pages=total_pages),
            reply_markup=markup,
        )

    async def ks_key_menu(self, call, idx, hidden=True):
        all_keys = sorted(self._keys.keys())
        if idx >= len(all_keys):
            return
        k    = all_keys[idx]
        prov = self._keys[k]
        tier = {"paid": f"{E_CARD} Paid", "free": f"{E_BATT} Free"}.get(
            self._paid_status.get(k, ""), "❓ Unknown")
        models = self._ensure_model_cache().get(k, [])
        display = f"{k[:4]}{'*'*(len(k)-8)}{k[-4:]}" if (hidden and len(k) > 8) else k
        markup = [
            [self._btn(self.strings["btn_show_key"] if hidden else self.strings["btn_hide_key"],
                       self.ks_key_menu, (idx, not hidden), "primary")],
            [
                self._btn(self.strings["btn_check_single"], self.ks_val_single, (idx,), "success"),
                self._btn(self.strings["btn_del_single"], self.ks_del_single, (idx,), "danger"),
            ],
            [self._btn(self.strings["btn_back"], self.ks_list, (0, "all"), "primary")],
        ]
        await call.edit(
            text=self.strings["key_info"].format(provider=prov, tier=tier, key=display, models=self._models_text(models)),
            reply_markup=markup,
        )

    async def ks_val_single(self, call, idx):
        all_keys = sorted(self._keys.keys())
        if idx >= len(all_keys):
            return
        k = all_keys[idx]
        async with aiohttp.ClientSession() as session:
            prov, ok = await self._validate_key(session, k)
        status = self.strings["status_valid"] if ok else self.strings["status_invalid"]
        await call.edit(
            text=self.strings["check_res_single"].format(provider=prov, status=status),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self.ks_key_menu, "args": (idx, True)}]],
        )

    async def ks_del_single(self, call, idx):
        all_keys = sorted(self._keys.keys())
        if idx < len(all_keys):
            k = all_keys[idx]
            self._keys.pop(k, None)
            self._paid_status.pop(k, None)
            self._save()
        await call.edit(
            text=self.strings["deleted"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self.ks_list, "args": (0, "all")}]],
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
        self._save()
        self._invalid_keys_cache.clear()
        await call.edit(
            text=self.strings["deleted"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self.ks_back}]],
        )

    async def ks_stats(self, call):
        summary = {}
        for key, provider in self._keys.items():
            if provider not in summary:
                summary[provider] = {"total": 0, "paid": 0, "free": 0, "unknown": 0}
            summary[provider]["total"] += 1
            tier = self._paid_status.get(key, "unknown")
            if tier not in ("paid", "free"):
                tier = "unknown"
            summary[provider][tier] += 1

        stats_text = "\n".join(
            f"{E_PIN} <b>{provider}</b>: total <b>{s['total']}</b>  "
            f"{E_CARD} {s['paid']}  {E_BATT} {s['free']}  ❓ {s['unknown']}"
            for provider, s in sorted(summary.items(), key=lambda item: item[1]["total"], reverse=True)
        ) or "—"

        await call.edit(
            text=self.strings["stats"].format(stats_text=stats_text),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self.ks_back, "style": "primary"}]],
        )

    async def ks_exp_menu(self, call):
        paid_c = sum(1 for k in self._keys if self._paid_status.get(k) == "paid")
        free_c = sum(1 for k in self._keys if self._paid_status.get(k) == "free")
        markup = [[
            self._btn(self.strings["btn_exp_json"], self.ks_exp_json, ("all",), "primary"),
            self._btn(self.strings["btn_exp_txt"],  self.ks_exp_txt,  ("all",), "primary"),
        ]]
        if paid_c:
            markup.append([
                self._btn(f"{self.strings['btn_exp_paid']} JSON ({paid_c})", self.ks_exp_json, ("paid",), "success"),
                self._btn(f"{self.strings['btn_exp_paid']} TXT",             self.ks_exp_txt,  ("paid",), "success"),
            ])
        if free_c:
            markup.append([
                self._btn(f"{self.strings['btn_exp_free']} JSON ({free_c})", self.ks_exp_json, ("free",), "danger"),
                self._btn(f"{self.strings['btn_exp_free']} TXT",             self.ks_exp_txt,  ("free",), "danger"),
            ])
        markup.append([self._btn(self.strings["btn_back"], self.ks_back, style="primary")])
        await call.edit(text=f"{E_DOWN} <b>Select export format:</b>", reply_markup=markup)

    async def ks_exp_json(self, call, filter_mode="all"):
        data  = self._filtered_keys(filter_mode)
        label = {"paid": " [PAID]", "free": " [FREE]"}.get(filter_mode, "")
        fd    = io.BytesIO(json.dumps(data, indent=4).encode("utf-8"))
        fd.name = f"keys{label.replace(' ', '_')}.json"
        await self.client.send_file(
            "me", file=fd,
            caption=f"{E_COPY} <b>Exported{label}</b> ({len(data)} keys)",
            parse_mode="html",
        )
        await call.edit(text=self.strings["exported"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self.ks_back}]])

    async def ks_exp_txt(self, call, filter_mode="all"):
        data  = self._filtered_keys(filter_mode)
        label = {"paid": " [PAID]", "free": " [FREE]"}.get(filter_mode, "")
        fd    = io.BytesIO("\n".join(f"{k} | {p}" for k, p in data.items()).encode("utf-8"))
        fd.name = f"keys{label.replace(' ', '_')}.txt"
        await self.client.send_file(
            "me", file=fd,
            caption=f"{E_COPY} <b>Exported{label}</b> ({len(data)} keys)",
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
                    self._ensure_model_cache()[key] = models
                else:
                    self._ensure_model_cache().pop(key, None)
                status = await self._check_paid(session, key, prov, models=models)
                if status == "unknown":
                    status = self._tier_from_models(prov, models) or "unknown"
                self._paid_status[key] = status
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
        self._save()
        await call.edit(
            text=self.strings["deleted_filter"].format(count=len(to_del)),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self.ks_back}]],
        )

    async def ks_settings_menu(self, call):
        mode      = self._settings.get("log_mode",  "none")
        file_scan = self._settings.get("file_scan", True)
        edit_scan = self._settings.get("edit_scan", True)
        target_text = self._log_target_text()
        markup = [
            [self._btn(self.strings["btn_log_cycle"], self.ks_cycle_log, style="primary")],
            [self._btn(self.strings["btn_log_target"], self.ks_logchat_help, style="success")],
            [self._btn(self.strings["btn_toggle_file"] + (" ✅" if file_scan else " ❌"), self.ks_toggle_file, style="success" if file_scan else "danger")],
            [self._btn(self.strings["btn_toggle_edit"] + (" ✅" if edit_scan else " ❌"), self.ks_toggle_edit, style="success" if edit_scan else "danger")],
            [self._btn(self.strings["btn_back"], self.ks_back, style="primary")],
        ]
        await call.edit(
            text=self.strings["settings_title"].format(
                log_mode  = mode.upper(),
                file_scan = "ON" if file_scan else "OFF",
                edit_scan = "ON" if edit_scan else "OFF",
            ) + f"\n\n{self.strings['log_target_label'].format(target=target_text)}\n{self.strings['log_topic_label'].format(topic=self._log_target().get('topic_title') or 'Logs')}",
            reply_markup=markup,
        )

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
        await self.ks_settings_menu(call)

    @loader.command(
        ru_doc="<чат/@username/id> [топик] — чат: задать чат логов; .kslogchat topic <название> — сменить топик",
        en_doc="<chat/@username/id> [topic] — set log chat; .kslogchat topic <title> — rename topic",
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
                [self._btn(self.strings["btn_back"], self.ks_settings_menu, style="primary")],
            ],
        )

    async def ks_toggle_file(self, call):
        self._settings["file_scan"] = not self._settings.get("file_scan", True)
        self._save()
        await self.ks_settings_menu(call)

    async def ks_toggle_edit(self, call):
        self._settings["edit_scan"] = not self._settings.get("edit_scan", True)
        self._save()
        await self.ks_settings_menu(call)

    def _is_ru_locale(self):
        return "Очист" in self.strings.get("btn_clear", "") or "Бесплатн" in self.strings.get("btn_filter_free", "")

    def _clear_all_warnings(self):
        if self._is_ru_locale():
            return [
                "⚠️ Это удалит всю БД. Ты уверен?",
                "⚠️ Это удалит вообще всё. Ты точно уверен?",
                "⚠️ Это уже не шутка — база реально уйдёт. Ты уверен?",
                "⚠️ Ты точно понимаешь, что данные пропадут?",
                "⚠️ Ещё один шанс передумать. Ты уверен?",
                "⚠️ База будет очищена полностью. Без отката.",
                "⚠️ Серьёзно, всё удалится. Ты точно хочешь этого?",
                "⚠️ Последний нормальный шанс остановиться.",
                "⚠️ Сейчас будет удаление всей базы. Ты уверен?",
                "⚠️ Почти финал. Подумай ещё раз.",
                "⚠️ Если ты всё ещё тут — жми финальную кнопку.",
            ]
        return [
            "⚠️ This will delete the entire database. Are you sure?",
            "⚠️ This will delete everything. Are you really sure?",
            "⚠️ This is not a joke — the database will be gone. Are you sure?",
            "⚠️ Do you fully understand this is irreversible?",
            "⚠️ One more chance to back out. Are you sure?",
            "⚠️ The database will be wiped completely. No undo.",
            "⚠️ Seriously, everything will be removed. Do you want this?",
            "⚠️ Last normal chance to stop.",
            "⚠️ Deleting the whole database next. Are you sure?",
            "⚠️ Almost there. Think again.",
            "⚠️ If you're still here, hit the final button.",
        ]

    async def ks_clr_menu(self, call):
        paid = sum(1 for k in self._keys if self._paid_status.get(k) == "paid")
        free = sum(1 for k in self._keys if self._paid_status.get(k) == "free")
        title = "🗑 <b>Очистка базы:</b>" if self._is_ru_locale() else "🗑 <b>Cleanup menu:</b>"
        subtitle = "Выберите, что удалить." if self._is_ru_locale() else "Choose what to delete."
        markup = [
            [
                self._btn(f"{self.strings['btn_del_paid']} ({paid})", self.ks_clr_paid_confirm, style="danger"),
                self._btn(f"{self.strings['btn_del_free']} ({free})", self.ks_clr_free_confirm, style="danger"),
            ],
            [self._btn(self.strings["btn_clear"], self.ks_clr_all_step, (0,), style="danger")],
            [self._btn(self.strings["btn_back"], self.ks_back, style="primary")],
        ]
        await call.edit(text=f"{title}\n{subtitle}", reply_markup=markup)

    async def ks_clr_all(self, call):
        await self.ks_clr_menu(call)

    async def ks_clr_paid_confirm(self, call):
        count = sum(1 for k in self._keys if self._paid_status.get(k) == "paid")
        if not count:
            return await call.edit(text=self.strings["empty"], reply_markup=[[self._btn(self.strings["btn_back"], self.ks_clr_menu, style="primary")]])
        text = "⚠️ Это удалит все платные ключи. Ты уверен?" if self._is_ru_locale() else "⚠️ This will delete all paid keys. Are you sure?"
        yes = "Да, удалить платные" if self._is_ru_locale() else "Yes, delete paid"
        markup = [
            [self._btn(yes, self.ks_clr_paid_execute, style="danger")],
            [self._btn(self.strings["btn_back"], self.ks_clr_menu, style="primary")],
        ]
        await call.edit(text=text, reply_markup=markup)

    async def ks_clr_free_confirm(self, call):
        count = sum(1 for k in self._keys if self._paid_status.get(k) == "free")
        if not count:
            return await call.edit(text=self.strings["empty"], reply_markup=[[self._btn(self.strings["btn_back"], self.ks_clr_menu, style="primary")]])
        text = "⚠️ Это удалит все бесплатные ключи. Ты уверен?" if self._is_ru_locale() else "⚠️ This will delete all free keys. Are you sure?"
        yes = "Да, удалить бесплатные" if self._is_ru_locale() else "Yes, delete free"
        markup = [
            [self._btn(yes, self.ks_clr_free_execute, style="danger")],
            [self._btn(self.strings["btn_back"], self.ks_clr_menu, style="primary")],
        ]
        await call.edit(text=text, reply_markup=markup)

    async def ks_clr_paid_execute(self, call):
        to_del = [k for k in list(self._keys.keys()) if self._paid_status.get(k) == "paid"]
        for k in to_del:
            self._keys.pop(k, None)
            self._paid_status.pop(k, None)
            self._ensure_model_cache().pop(k, None)
        self._save()
        msg = f"🗑 Удалено платных ключей: <b>{len(to_del)}</b>" if self._is_ru_locale() else f"🗑 Removed paid keys: <b>{len(to_del)}</b>"
        await call.edit(text=msg, reply_markup=[[self._btn(self.strings["btn_back"], self.ks_clr_menu, style="primary")]])

    async def ks_clr_free_execute(self, call):
        to_del = [k for k in list(self._keys.keys()) if self._paid_status.get(k) == "free"]
        for k in to_del:
            self._keys.pop(k, None)
            self._paid_status.pop(k, None)
            self._ensure_model_cache().pop(k, None)
        self._save()
        msg = f"🗑 Удалено бесплатных ключей: <b>{len(to_del)}</b>" if self._is_ru_locale() else f"🗑 Removed free keys: <b>{len(to_del)}</b>"
        await call.edit(text=msg, reply_markup=[[self._btn(self.strings["btn_back"], self.ks_clr_menu, style="primary")]])

    async def ks_clr_all_step(self, call, step=0):
        warns = self._clear_all_warnings()
        step = max(0, min(step, len(warns) - 1))
        if step < len(warns) - 1:
            next_btn = "Дальше" if self._is_ru_locale() else "Next"
            markup = [
                [self._btn(next_btn, self.ks_clr_all_step, (step + 1,), style="danger")],
                [self._btn(self.strings["btn_back"], self.ks_clr_menu, style="primary")],
            ]
            await call.edit(text=warns[step], reply_markup=markup)
            return
        final_btn = "Я 1000% уверен блять"
        markup = [
            [self._btn(final_btn, self.ks_clr_all_execute, style="danger")],
            [self._btn(self.strings["btn_back"], self.ks_clr_menu, style="primary")],
        ]
        await call.edit(text=warns[step], reply_markup=markup)

    async def ks_clr_all_execute(self, call):
        self._keys.clear()
        self._paid_status.clear()
        self._ensure_model_cache().clear()
        self._save()
        msg = "🗑 Удалена вся база." if self._is_ru_locale() else "🗑 Entire database removed."
        await call.edit(text=msg, reply_markup=[[self._btn(self.strings["btn_back"], self.ks_back, style="primary")]])

    async def ks_back(self, call):
        await call.edit(text=self._db_stats_text(), reply_markup=self._get_main_markup())