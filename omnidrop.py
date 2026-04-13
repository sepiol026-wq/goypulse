# requires: yt-dlp imageio-ffmpeg
# meta developer: @ЮЗЕРНЕЙМ
# meta banner: https://raw.githubusercontent.com/sepiol026-wq/goypulse/main/banner.png
# author: @goy_ai

__version__ = (1, 0, 0)

import asyncio
import contextlib
import json
import os
import re
import tempfile
import uuid
from typing import Dict, Optional, Tuple

import imageio_ffmpeg
from telethon.tl.types import DocumentAttributeAudio, DocumentAttributeVideo, Message

from .. import loader, utils


@loader.tds
class OmniDropXMod(loader.Module):
    """Universal async downloader with inline quality/format selector powered by yt-dlp."""

    strings = {
        "name": "OmniDropX",
        "no_args": "📎 <b>Send link or query:</b> <code>.omni &lt;url/query&gt;</code>",
        "searching": "🔍 <b>Scanning source and metadata...</b>",
        "no_data": "💀 <b>Nothing found.</b>",
        "pick_mode": "🎛 <b>{title}</b>\n👤 <b>{uploader}</b>\n⏱ <b>{duration}</b>\n🌍 <b>{site}</b>\n\nChoose format/quality:",
        "dl_start": "🚀 <b>Starting:</b> <code>{label}</code>",
        "dl_fail": "💀 <b>Download failed:</b> <code>{err}</code>",
        "uploading": "📤 <b>Uploading to Telegram...</b>",
        "done": "✅ <b>Done:</b> <code>{name}</code>",
        "expired": "⌛ <b>Session expired, run command again.</b>",
        "cancelled": "🛑 <b>Cancelled.</b>",
        "cfg_saved": "⚙️ <b>Config updated.</b>",
        "watch_hint": "⚡ <b>Quick drop:</b> <code>.omni {url}</code>",
    }

    strings_ru = {
        "no_args": "📎 <b>Дай ссылку или запрос:</b> <code>.omni &lt;url/запрос&gt;</code>",
        "searching": "🔍 <b>Пробиваю источник и метаданные...</b>",
        "no_data": "💀 <b>Ничего не найдено.</b>",
        "pick_mode": "🎛 <b>{title}</b>\n👤 <b>{uploader}</b>\n⏱ <b>{duration}</b>\n🌍 <b>{site}</b>\n\nВыбери формат/качество:",
        "dl_start": "🚀 <b>Стартую:</b> <code>{label}</code>",
        "dl_fail": "💀 <b>Ошибка загрузки:</b> <code>{err}</code>",
        "uploading": "📤 <b>Заливаю в Telegram...</b>",
        "done": "✅ <b>Готово:</b> <code>{name}</code>",
        "expired": "⌛ <b>Сессия устарела, запусти команду снова.</b>",
        "cancelled": "🛑 <b>Отменено.</b>",
        "cfg_saved": "⚙️ <b>Конфиг обновлён.</b>",
        "watch_hint": "⚡ <b>Быстрый дамп:</b> <code>.omni {url}</code>",
    }

    strings_de = {
        "no_args": "📎 <b>Sende Link oder Suchanfrage:</b> <code>.omni &lt;url/query&gt;</code>",
        "searching": "🔍 <b>Quelle und Metadaten werden geprüft...</b>",
        "no_data": "💀 <b>Nichts gefunden.</b>",
        "pick_mode": "🎛 <b>{title}</b>\n👤 <b>{uploader}</b>\n⏱ <b>{duration}</b>\n🌍 <b>{site}</b>\n\nFormat/Qualität wählen:",
        "dl_start": "🚀 <b>Starte:</b> <code>{label}</code>",
        "dl_fail": "💀 <b>Download-Fehler:</b> <code>{err}</code>",
        "uploading": "📤 <b>Upload zu Telegram...</b>",
        "done": "✅ <b>Fertig:</b> <code>{name}</code>",
        "expired": "⌛ <b>Sitzung abgelaufen, bitte neu starten.</b>",
        "cancelled": "🛑 <b>Abgebrochen.</b>",
        "cfg_saved": "⚙️ <b>Konfiguration gespeichert.</b>",
        "watch_hint": "⚡ <b>Schnell-Download:</b> <code>.omni {url}</code>",
    }

    strings_jp = {
        "no_args": "📎 <b>リンクまたは検索語を入力:</b> <code>.omni &lt;url/query&gt;</code>",
        "searching": "🔍 <b>ソースとメタデータを確認中...</b>",
        "no_data": "💀 <b>見つかりませんでした。</b>",
        "pick_mode": "🎛 <b>{title}</b>\n👤 <b>{uploader}</b>\n⏱ <b>{duration}</b>\n🌍 <b>{site}</b>\n\n形式/品質を選択:",
        "dl_start": "🚀 <b>開始:</b> <code>{label}</code>",
        "dl_fail": "💀 <b>ダウンロード失敗:</b> <code>{err}</code>",
        "uploading": "📤 <b>Telegramへ送信中...</b>",
        "done": "✅ <b>完了:</b> <code>{name}</code>",
        "expired": "⌛ <b>セッション期限切れ。再実行してください。</b>",
        "cancelled": "🛑 <b>キャンセルしました。</b>",
        "cfg_saved": "⚙️ <b>設定を保存しました。</b>",
        "watch_hint": "⚡ <b>クイック保存:</b> <code>.omni {url}</code>",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "watch_links",
                False,
                "Watcher hint for raw links",
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "default_caption",
                "🎬 <b>{title}</b>\n👤 {uploader}\n🌍 {site}\n#omnidrop",
                "Caption template",
                validator=loader.validators.String(),
            ),
        )
        self._jobs: Dict[str, dict] = {}

    async def _run_proc(self, cmd: list, timeout: int = 180) -> Tuple[int, bytes, bytes]:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            return proc.returncode, stdout, stderr
        except asyncio.TimeoutError:
            with contextlib.suppress(Exception):
                proc.kill()
            return -1, b"", b"Timeout"

    def _fmt_duration(self, value: Optional[int]) -> str:
        value = int(value or 0)
        h, rem = divmod(value, 3600)
        m, s = divmod(rem, 60)
        return f"{h:02}:{m:02}:{s:02}" if h else f"{m:02}:{s:02}"

    async def _extract_info(self, query: str) -> Optional[dict]:
        target = query if re.match(r"https?://", query, re.I) else f"ytsearch1:{query}"
        cmd = [
            "python3",
            "-m",
            "yt_dlp",
            "--no-playlist",
            "--dump-single-json",
            target,
        ]
        rc, out, _ = await self._run_proc(cmd, timeout=90)
        if rc != 0 or not out:
            return None
        try:
            data = json.loads(out.decode("utf-8", "ignore"))
        except Exception:
            return None
        if "entries" in data and isinstance(data["entries"], list) and data["entries"]:
            data = data["entries"][0]
        return data if isinstance(data, dict) else None

    def _build_markup(self, token: str):
        return [
            [
                {"text": "🎵 MP3 320", "callback": self._cb_pick, "args": (token, "a_mp3_320")},
                {"text": "🎵 M4A", "callback": self._cb_pick, "args": (token, "a_m4a")},
                {"text": "🎵 OPUS", "callback": self._cb_pick, "args": (token, "a_opus")},
            ],
            [
                {"text": "🎬 MP4 1080", "callback": self._cb_pick, "args": (token, "v_mp4_1080")},
                {"text": "🎬 MP4 720", "callback": self._cb_pick, "args": (token, "v_mp4_720")},
                {"text": "🎬 MP4 480", "callback": self._cb_pick, "args": (token, "v_mp4_480")},
            ],
            [
                {"text": "📦 MKV Best", "callback": self._cb_pick, "args": (token, "v_mkv_best")},
                {"text": "📹 WEBM Best", "callback": self._cb_pick, "args": (token, "v_webm_best")},
            ],
            [{"text": "🛑 Cancel", "callback": self._cb_cancel, "args": (token,)}],
        ]

    def _mode_cfg(self, mode: str) -> dict:
        table = {
            "a_mp3_320": {"label": "MP3 320", "format": "bestaudio/best", "post": ["-x", "--audio-format", "mp3", "--audio-quality", "0"]},
            "a_m4a": {"label": "M4A", "format": "bestaudio[ext=m4a]/bestaudio/best", "post": ["-x", "--audio-format", "m4a"]},
            "a_opus": {"label": "OPUS", "format": "bestaudio", "post": ["-x", "--audio-format", "opus"]},
            "v_mp4_1080": {"label": "MP4 1080", "format": "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best", "post": ["--remux-video", "mp4"]},
            "v_mp4_720": {"label": "MP4 720", "format": "bestvideo[height<=720]+bestaudio/best[height<=720]/best", "post": ["--remux-video", "mp4"]},
            "v_mp4_480": {"label": "MP4 480", "format": "bestvideo[height<=480]+bestaudio/best[height<=480]/best", "post": ["--remux-video", "mp4"]},
            "v_mkv_best": {"label": "MKV Best", "format": "bestvideo+bestaudio/best", "post": ["--merge-output-format", "mkv"]},
            "v_webm_best": {"label": "WEBM Best", "format": "bestvideo[ext=webm]+bestaudio[ext=webm]/best[ext=webm]/best", "post": []},
        }
        return table.get(mode, table["a_mp3_320"])

    async def _download(self, query: str, mode: str) -> Tuple[Optional[str], Optional[str]]:
        cfg = self._mode_cfg(mode)
        ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()
        tmp = tempfile.mkdtemp(prefix="omnidrop_")
        out_tpl = os.path.join(tmp, "%(title).180B_%(id)s.%(ext)s")
        cmd = [
            "python3",
            "-m",
            "yt_dlp",
            "--no-playlist",
            "--format",
            cfg["format"],
            "--ffmpeg-location",
            ffmpeg_path,
            "--embed-metadata",
            "--embed-thumbnail",
            "--add-metadata",
            "--write-info-json",
            "-o",
            out_tpl,
            query,
        ] + cfg["post"]
        rc, _, err = await self._run_proc(cmd, timeout=420)
        if rc != 0:
            return None, err.decode("utf-8", "ignore")[-350:]
        files = [f for f in os.listdir(tmp) if not f.endswith((".json", ".webp", ".jpg", ".png", ".part"))]
        if not files:
            return None, "No output file"
        files.sort(key=lambda x: os.path.getsize(os.path.join(tmp, x)), reverse=True)
        return os.path.join(tmp, files[0]), None

    async def _send_media(self, call, path: str, info: dict):
        title = info.get("title") or "Unknown"
        uploader = info.get("uploader") or info.get("channel") or "Unknown"
        duration = int(info.get("duration") or 0)
        site = info.get("extractor_key") or info.get("extractor") or "Source"
        is_audio = os.path.splitext(path)[1].lower() in {".mp3", ".m4a", ".opus", ".ogg", ".flac", ".wav"}
        cap = self.config["default_caption"].format(
            title=utils.escape_html(title),
            uploader=utils.escape_html(uploader),
            site=utils.escape_html(site),
        )
        if is_audio:
            attrs = [DocumentAttributeAudio(duration=duration, title=title, performer=uploader)]
        else:
            attrs = [DocumentAttributeVideo(duration=duration, w=info.get("width") or 1280, h=info.get("height") or 720, supports_streaming=True)]
        await call.edit(self.strings("uploading"), reply_markup=None)
        await self._client.send_file(call.form.get("chat"), path, caption=cap, attributes=attrs)

    @loader.command(
        en_doc="<url/query> - open inline universal downloader",
        ru_doc="<url/запрос> - открыть инлайн универсальный загрузчик",
        de_doc="<url/query> - universellen Inline-Downloader öffnen",
        jp_doc="<url/query> - インラインダウンローダーを開く",
    )
    async def omnicmd(self, message: Message):
        query = utils.get_args_raw(message)
        if not query:
            return await utils.answer(message, self.strings("no_args"))
        msg = await utils.answer(message, self.strings("searching"))
        info = await self._extract_info(query)
        if not info:
            return await utils.answer(msg, self.strings("no_data"))
        token = uuid.uuid4().hex[:12]
        self._jobs[token] = {
            "query": query,
            "info": info,
            "chat": utils.get_chat_id(message),
        }
        txt = self.strings("pick_mode").format(
            title=utils.escape_html((info.get("title") or "Unknown")[:120]),
            uploader=utils.escape_html((info.get("uploader") or info.get("channel") or "Unknown")[:120]),
            duration=self._fmt_duration(info.get("duration")),
            site=utils.escape_html(str(info.get("extractor_key") or info.get("extractor") or "unknown")),
        )
        await self.inline.form(text=txt, message=msg, reply_markup=self._build_markup(token))

    async def _cb_cancel(self, call, token: str):
        self._jobs.pop(token, None)
        await call.edit(self.strings("cancelled"), reply_markup=None)

    async def _cb_pick(self, call, token: str, mode: str):
        job = self._jobs.get(token)
        if not job:
            return await call.edit(self.strings("expired"), reply_markup=None)
        cfg = self._mode_cfg(mode)
        await call.edit(self.strings("dl_start").format(label=cfg["label"]), reply_markup=None)
        path, err = await self._download(job["query"], mode)
        if err or not path:
            return await call.edit(self.strings("dl_fail").format(err=utils.escape_html((err or "error")[:300])), reply_markup=None)
        try:
            await self._send_media(call, path, job["info"])
            await call.delete()
        finally:
            with contextlib.suppress(Exception):
                os.remove(path)
            with contextlib.suppress(Exception):
                os.rmdir(os.path.dirname(path))
            self._jobs.pop(token, None)

    @loader.command(
        en_doc="on/off - toggle watcher hint for plain links",
        ru_doc="on/off - включить или выключить подсказку watcher",
        de_doc="on/off - Watcher-Hinweis aktivieren/deaktivieren",
        jp_doc="on/off - watcherヒントの切替",
    )
    async def omnicfgcmd(self, message: Message):
        arg = (utils.get_args_raw(message) or "").strip().lower()
        self.config["watch_links"] = arg in {"1", "on", "true", "yes", "да", "вкл"}
        await utils.answer(message, self.strings("cfg_saved"))

    @loader.watcher(only_messages=True, no_commands=True)
    async def omniwatcher(self, message: Message):
        if not self.config["watch_links"]:
            return
        text = getattr(message, "raw_text", None) or getattr(message, "text", None)
        if not text or not re.search(r"https?://", text, re.I):
            return
        if getattr(message, "out", False):
            return
        m = re.search(r"https?://\S+", text, re.I)
        if not m:
            return
        await utils.answer(message, self.strings("watch_hint").format(url=utils.escape_html(m.group(0))))
