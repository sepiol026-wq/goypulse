# ====================================================================================================================
#   ██████╗  ██████╗ ██╗   ██╗███╗   ███╗ ██████╗ ██████╗ ██╗   ██╗██╗     ███████╗███████╗
#  ██╔════╝ ██╔═══██╗╚██╗ ██╔╝████╗ ████║██╔═══██╗██╔══██╗██║   ██║██║     ██╔════╝██╔════╝
#  ██║  ███╗██║   ██║ ╚████╔╝ ██╔████╔██║██║   ██║██║  ██║██║   ██║██║     █████╗  ███████╗
#  ██║   ██║██║   ██║  ╚██╔╝  ██║╚██╔╝██║██║   ██║██║  ██║██║   ██║██║     ██╔══╝  ╚════██║
#  ╚██████╔╝╚██████╔╝   ██║   ██║ ╚═╝ ██║╚██████╔╝██████╔╝╚██████╔╝███████╗███████╗███████║
#   ╚═════╝  ╚═════╝    ╚═╝   ╚═╝     ╚═╝ ╚═════╝ ╚═════╝  ╚═════╝ ╚══════╝╚══════╝╚══════╝
#
#   OFFICIAL USERNAMES: @goymodules | @samsepi0l_ovf
#   MODULE: omni
#
#   THIS MODULE IS LICENSED UNDER GNU AGPLv3, PROTECTED AGAINST UNAUTHORIZED COPYING/RESALE,
#   AND ITS ORIGINAL AUTHORSHIP BELONGS TO @samsepi0l_ovf.
#   ALL OFFICIAL UPDATES, RELEASE NOTES, AND PATCHES ARE PUBLISHED IN THE TELEGRAM CHANNEL @goymodules.
# ====================================================================================================================

# requires: yt-dlp imageio-ffmpeg
# meta developer: @goymodules
# authors: @goymodules
# Description: Universal media downloader.
# meta banner: https://raw.githubusercontent.com/sepiol026-wq/goypulse/main/assets/omniload.png

__version__ = (1, 5)
import asyncio
import contextlib
import json
import logging
import os
import shutil
import tempfile
import sys
import time
from telethon.tl.types import Message, DocumentAttributeAudio, DocumentAttributeVideo
import imageio_ffmpeg
from .. import loader, utils

logger = logging.getLogger(__name__)

@loader.tds
class OmniLoad(loader.Module):
    """Универсальный загрузчик медиа."""

    strings = {
        "name": "OmniLoad",
        "no_args": "<tg-emoji emoji-id=5256079005731271025>📟</tg-emoji> <b>No URL provided.</b> Please specify a link.",
        "fetching": "<tg-emoji emoji-id=5255971360965930740>🕔</tg-emoji> <b>Parsing target...</b>",
        "menu": "<tg-emoji emoji-id=5253877736207821121>🔥</tg-emoji> <b>Target:</b> <i>{title}</i>\nChoose format & quality:",
        "downloading": "<tg-emoji emoji-id=5253877736207821121>🔥</tg-emoji> <b>Downloading & rendering...</b>",
        "uploading": "<tg-emoji emoji-id=5255971360965930740>🕔</tg-emoji> <b>Uploading to Telegram...</b>",
        "error": "<tg-emoji emoji-id=5256054975389247793>📛</tg-emoji> <b>Error:</b> <code>{error}</code>",
        "expired": "<tg-emoji emoji-id=5256054975389247793>📛</tg-emoji> <b>Cache expired.</b> Please search again.",
        "caption": "<tg-emoji emoji-id=5253651477330667400>🎞</tg-emoji> <b>{title}</b>\n<tg-emoji emoji-id=5255835635704408236>👤</tg-emoji> {author}\n<tg-emoji emoji-id=5253490441826870592>🔗</tg-emoji> <a href='{url}'>Source</a>"
    }

    strings_ru = {
        "no_args": "<tg-emoji emoji-id=5256079005731271025>📟</tg-emoji> <b>Аргументы где?</b> Укажи ссылку.",
        "fetching": "<tg-emoji emoji-id=5255971360965930740>🕔</tg-emoji> <b>Паршу таргет...</b>",
        "menu": "<tg-emoji emoji-id=5253877736207821121>🔥</tg-emoji> <b>Таргет:</b> <i>{title}</i>\nВыбирай качество:",
        "downloading": "<tg-emoji emoji-id=5253877736207821121>🔥</tg-emoji> <b>Дамплю сурс & рендерю...</b>",
        "uploading": "<tg-emoji emoji-id=5255971360965930740>🕔</tg-emoji> <b>Аплоад в Telegram...</b>",
        "error": "<tg-emoji emoji-id=5256054975389247793>📛</tg-emoji> <b>Ошибка:</b> <code>{error}</code>",
        "expired": "<tg-emoji emoji-id=5256054975389247793>📛</tg-emoji> <b>Кэш устарел.</b> Сделай запрос заново.",
        "caption": "<tg-emoji emoji-id=5253651477330667400>🎞</tg-emoji> <b>{title}</b>\n<tg-emoji emoji-id=5255835635704408236>👤</tg-emoji> {author}\n<tg-emoji emoji-id=5253490441826870592>🔗</tg-emoji> <a href='{url}'>Сурс</a>"
    }

    strings_de = {
        "no_args": "<tg-emoji emoji-id=5256079005731271025>📟</tg-emoji> <b>Kein URL angegeben.</b>",
        "fetching": "<tg-emoji emoji-id=5255971360965930740>🕔</tg-emoji> <b>Ziel wird analysiert...</b>",
        "menu": "<tg-emoji emoji-id=5253877736207821121>🔥</tg-emoji> <b>Ziel:</b> <i>{title}</i>\nWählen Sie die Qualität:",
        "downloading": "<tg-emoji emoji-id=5253877736207821121>🔥</tg-emoji> <b>Herunterladen & Verarbeiten...</b>",
        "uploading": "<tg-emoji emoji-id=5255971360965930740>🕔</tg-emoji> <b>Hochladen zu Telegram...</b>",
        "error": "<tg-emoji emoji-id=5256054975389247793>📛</tg-emoji> <b>Fehler:</b> <code>{error}</code>",
        "expired": "<tg-emoji emoji-id=5256054975389247793>📛</tg-emoji> <b>Cache abgelaufen.</b> Bitte erneut versuchen.",
        "caption": "<tg-emoji emoji-id=5253651477330667400>🎞</tg-emoji> <b>{title}</b>\n<tg-emoji emoji-id=5255835635704408236>👤</tg-emoji> {author}\n<tg-emoji emoji-id=5253490441826870592>🔗</tg-emoji> <a href='{url}'>Quelle</a>"
    }

    strings_jp = {
        "no_args": "<tg-emoji emoji-id=5256079005731271025>📟</tg-emoji> <b>URLが提供されていません。</b>",
        "fetching": "<tg-emoji emoji-id=5255971360965930740>🕔</tg-emoji> <b>ターゲットを解析中...</b>",
        "menu": "<tg-emoji emoji-id=5253877736207821121>🔥</tg-emoji> <b>ターゲット:</b> <i>{title}</i>\n品質を選択してください:",
        "downloading": "<tg-emoji emoji-id=5253877736207821121>🔥</tg-emoji> <b>ダウンロードとレンダリング中...</b>",
        "uploading": "<tg-emoji emoji-id=5255971360965930740>🕔</tg-emoji> <b>Telegramにアップロード中...</b>",
        "error": "<tg-emoji emoji-id=5256054975389247793>📛</tg-emoji> <b>エラー:</b> <code>{error}</code>",
        "expired": "<tg-emoji emoji-id=5256054975389247793>📛</tg-emoji> <b>キャッシュの期限切れ。</b> 再試行してください。",
        "caption": "<tg-emoji emoji-id=5253651477330667400>🎞</tg-emoji> <b>{title}</b>\n<tg-emoji emoji-id=5255835635704408236>👤</tg-emoji> {author}\n<tg-emoji emoji-id=5253490441826870592>🔗</tg-emoji> <a href='{url}'>ソース</a>"
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue("ytdl_timeout", 300, "Timeout for downloading", validator=loader.validators.Integer(minimum=30)),
            loader.ConfigValue("upload_timeout", 600, "Timeout for Telegram upload", validator=loader.validators.Integer(minimum=60))
        )
        self._cache = {}
        self.storage_dir = os.path.join(os.getcwd(), "omniload_storage")
        os.makedirs(self.storage_dir, exist_ok=True)

    async def _run_proc(self, cmd: list, timeout: int = 60):
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            return proc.returncode, stdout, stderr
        except asyncio.TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            return -1, b"", b"TimeoutError"

    @loader.command(
        ru_doc="<ссылка> - Скачать медиа (Видео/Аудио) из любого сервиса",
        en_doc="<url> - Download media (Video/Audio) from any service",
        de_doc="<url> - Medien (Video/Audio) herunterladen",
        jp_doc="<url> - メディア（ビデオ/オーディオ）をダウンロード"
    )
    async def dlcmd(self, message: Message):
        args = utils.get_args_raw(message)
        if not args:
            return await utils.answer(message, self.strings("no_args"))

        msg = await utils.answer(message, self.strings("fetching"))
        ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()

        cmd = [
            sys.executable, "-m", "yt_dlp", "--no-warnings", "--dump-json",
            "--extractor-args", "youtube:player_client=android",
            "--ffmpeg-location", ffmpeg_path, args
        ]

        ret, stdout, stderr = await self._run_proc(cmd, timeout=45)

        if ret != 0 or not stdout:
            err_text = stderr.decode('utf-8', errors='ignore')[-100:]
            return await utils.answer(msg, self.strings("error").format(error=err_text or "Extraction failed"))

        try:
            info = json.loads(stdout.decode('utf-8').split('\n')[0])
        except json.JSONDecodeError:
            return await utils.answer(msg, self.strings("error").format(error="JSON Parse failed"))

        call_id = str(message.id)
        target_chat_id = utils.get_chat_id(message)
        reply_id = message.id

        self._cache[call_id] = {"info": info, "url": args}
        title = info.get("title", "Unknown")[:40]

        keyboard = [
            [
                {"text": "🎬 Video 1080p", "callback": self._dl_callback, "args": (call_id, "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best", "video", target_chat_id, reply_id)},
                {"text": "🎬 Video 720p", "callback": self._dl_callback, "args": (call_id, "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best", "video", target_chat_id, reply_id)}
            ],
            [
                {"text": "🎧 Audio MP3", "callback": self._dl_callback, "args": (call_id, "bestaudio/best", "audio", target_chat_id, reply_id)},
                {"text": "🎧 Audio FLAC", "callback": self._dl_callback, "args": (call_id, "bestaudio/best", "flac", target_chat_id, reply_id)}
            ],
            [{"text": "❌ Cancel", "callback": self._cancel_callback, "args": (call_id,)}]
        ]

        await self.inline.form(
            self.strings("menu").format(title=utils.escape_html(title)),
            message=msg,
            reply_markup=keyboard
        )

    async def _dl_callback(self, call, call_id: str, format_spec: str, media_type: str, target_chat_id: int, reply_id: int):
        with contextlib.suppress(Exception):
            await call.answer("<tg-emoji emoji-id=5253613479754999811>➡️</tg-emoji> Processing...")

        if call_id not in self._cache:
            with contextlib.suppress(Exception):
                await call.edit(self.strings("expired"), reply_markup=None)
            return

        with contextlib.suppress(Exception):
            await call.edit(self.strings("downloading"), reply_markup=None)

        data = self._cache.pop(call_id)
        info = data["info"]
        url = data["url"]

        dl_dir = tempfile.mkdtemp(prefix="omniload_")
        ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()

        cmd = [
            sys.executable, "-m", "yt_dlp",
            "--format", format_spec,
            "--extractor-args", "youtube:player_client=android",
            "--ffmpeg-location", ffmpeg_path,
            "-o", os.path.join(dl_dir, "%(id)s.%(ext)s"),
            "--embed-metadata",
        ]

        if media_type == "video":
            cmd.extend(["--merge-output-format", "mp4"])
        elif media_type in ("audio", "flac"):
            ext = "flac" if media_type == "flac" else "mp3"
            cmd.extend(["-x", "--audio-format", ext])

        cmd.append(url)

        ret, _, stderr = await self._run_proc(cmd, timeout=self.config["ytdl_timeout"])

        try:
            out_files = [f for f in os.listdir(dl_dir) if os.path.isfile(os.path.join(dl_dir, f))]
            target_file = out_files[0] if out_files else None

            if ret != 0 or not target_file:
                err_text = stderr.decode('utf-8', errors='ignore')[-100:]
                with contextlib.suppress(Exception):
                    await call.edit(self.strings("error").format(error=err_text or "Download failed"))
                return

            final_path = os.path.join(dl_dir, target_file)

            last_edit_time = 0

            async def upload_progress(current, total):
                nonlocal last_edit_time
                now = time.time()
                if now - last_edit_time > 4:
                    percent = round((current / total) * 100, 1) if total else 0
                    with contextlib.suppress(Exception):
                        text = self.strings("uploading").replace("...", f" {percent}%")
                        await call.edit(text)
                    last_edit_time = now

            with contextlib.suppress(Exception):
                await call.edit(self.strings("uploading").replace("...", " 0%"))

            try:
                uploaded_file = await self._client.upload_file(
                    final_path,
                    part_size_kb=512,
                    progress_callback=upload_progress
                )
            except Exception as e:
                with contextlib.suppress(Exception):
                    await call.edit(self.strings("error").format(error=f"Upload Error: {str(e)[:100]}"))
                return

            title = info.get("title", "Unknown")
            author = info.get("uploader", info.get("channel", "Unknown User"))
            duration = int(info.get("duration") or 0)

            caption = self.strings("caption").format(
                title=utils.escape_html(title),
                author=utils.escape_html(author),
                url=url
            )

            if info.get("tags"):
                tags_str = " ".join([f"#{t.replace(' ', '_')}" for t in info["tags"][:5]])
                caption += f"\n\n{tags_str}"

            attrs = None
            if media_type == "video":
                w = int(info.get("width") or 0)
                h = int(info.get("height") or 0)

                if w > 0 and h > 0:
                    attrs = [DocumentAttributeVideo(
                        duration=duration,
                        w=w,
                        h=h,
                        supports_streaming=True
                    )]
            else:
                attrs = [DocumentAttributeAudio(
                    duration=duration,
                    title=title,
                    performer=author
                )]

            upload_kwargs = {
                "entity": target_chat_id,
                "file": uploaded_file,
                "caption": caption,
                "reply_to": reply_id
            }
            if attrs:
                upload_kwargs["attributes"] = attrs

            try:
                try:
                    await self._client.send_file(**upload_kwargs)
                except Exception as e:
                    if "reply" in str(e).lower():
                        upload_kwargs.pop("reply_to", None)
                        await self._client.send_file(**upload_kwargs)
                    else:
                        raise e

                with contextlib.suppress(Exception):
                    await call.delete()

            except Exception as upload_err:
                err_msg = self.strings("error").format(error=f"TG Send Error: {upload_err}")
                try:
                    await call.edit(err_msg)
                except Exception:
                    await self._client.send_message(target_chat_id, err_msg)

        finally:
            shutil.rmtree(dl_dir, ignore_errors=True)

    async def _cancel_callback(self, call, call_id: str):
        self._cache.pop(call_id, None)
        with contextlib.suppress(Exception):
            await call.answer("Canceled")
        with contextlib.suppress(Exception):
            await call.delete()
