# requires: telethon pillow requests yt-dlp imageio-ffmpeg
# meta developer: @samsepi0l_ovf
# authors: @goy_ai
# meta banner: https://raw.githubusercontent.com/sepiol026-wq/goypulse/main/banner.png
# Description: лень писать итак всё ясно нахуй
__version__ = (3, 1)

import asyncio
import contextlib
import io
import json
import logging
import os
import sys
import textwrap
import tempfile
import shutil

import requests
import imageio_ffmpeg
from PIL import Image, ImageDraw, ImageEnhance, ImageFilter, ImageFont
from telethon.tl.types import Message, DocumentAttributeAudio

from .. import loader, utils

logger = logging.getLogger(__name__)

def _fetch_sync(url: str) -> bytes:
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return r.content

class Banners:
    def __init__(self, title: str, artists: list, duration: int, progress: int, track_cover: bytes, font_bytes: bytes, blur: int):
        self.title = title
        self.artists = ", ".join(artists) if isinstance(artists, list) else artists
        self.duration = duration
        self.progress = progress
        self.track_cover = track_cover
        self.font_bytes = font_bytes
        self.blur_intensity = blur

    def _get_font(self, size, font_bytes):
        return ImageFont.truetype(io.BytesIO(font_bytes), size)

    def _prepare_cover(self, size, radius):
        cover = Image.open(io.BytesIO(self.track_cover)).convert("RGBA")
        cover = cover.resize((size, size), Image.Resampling.LANCZOS)
        mask = Image.new("L", (size, size), 0)
        draw = ImageDraw.Draw(mask)
        draw.rounded_rectangle((0, 0, size, size), radius=radius, fill=255)
        output = Image.new("RGBA", (size, size), (0, 0, 0, 0))
        output.paste(cover, (0, 0), mask=mask)
        return output

    def _prepare_background(self, w, h):
        bg = Image.open(io.BytesIO(self.track_cover)).convert("RGBA")
        bg = bg.resize((w, h), Image.Resampling.LANCZOS)
        bg = bg.filter(ImageFilter.GaussianBlur(radius=self.blur_intensity))
        bg = ImageEnhance.Brightness(bg).enhance(0.35) 
        return bg

    def _draw_progress_bar(self, draw, x, y, w, h, progress_pct, color="white", bg_color="#6b6b6b"):
        draw.rounded_rectangle((x, y, x + w, y + h), radius=h/2, fill=bg_color)
        fill_w = int(w * progress_pct)
        if fill_w > 0:
            draw.rounded_rectangle((x, y, x + fill_w, y + h), radius=h/2, fill=color)

    def horizontal(self):
        W, H = 1500, 600
        padding = 60
        cover_size = 480
        title_font = self._get_font(55, self.font_bytes)
        artist_font = self._get_font(45, self.font_bytes)
        time_font = self._get_font(25, self.font_bytes)

        img = self._prepare_background(W, H)
        draw = ImageDraw.Draw(img)
        
        cover = self._prepare_cover(cover_size, 30)
        img.paste(cover, (padding, (H - cover_size) // 2), cover)

        text_x = padding + cover_size + 60
        text_y_start = 100
        text_width_limit = W - text_x - padding

        wrapper = textwrap.TextWrapper(width=23)
        title_lines = wrapper.wrap(self.title)
        
        if len(title_lines) > 2:
            title_lines = title_lines[:2]
            title_lines[-1] += "..."

        current_y = text_y_start
        title_height = title_font.getbbox("Ah")[3] + 15

        for line in title_lines:
            draw.text((text_x, current_y), line, font=title_font, fill="white")
            current_y += title_height
        
        display_artist = self.artists
        while artist_font.getlength(display_artist) > text_width_limit and len(display_artist) > 0:
            display_artist = display_artist[:-1]
        if len(display_artist) < len(self.artists): display_artist += "…"

        artist_y = current_y + 10 
        draw.text((text_x, artist_y), display_artist, font=artist_font, fill="#b3b3b3")

        cur_time = f"{(self.progress//1000//60):02}:{(self.progress//1000%60):02}"
        dur_time = f"{(self.duration//1000//60):02}:{(self.duration//1000%60):02}"
        cur_w = time_font.getlength(cur_time)
        dur_w = time_font.getlength(dur_time)
        bar_y = 480
        bar_h = 8
        gap = 25
        
        draw.text((text_x, bar_y - 12), cur_time, font=time_font, fill="white")
        bar_start_x = text_x + cur_w + gap
        bar_end_x = text_x + text_width_limit - dur_w - gap
        bar_w = bar_end_x - bar_start_x
        prog_pct = self.progress / self.duration if self.duration > 0 else 0
        self._draw_progress_bar(draw, bar_start_x, bar_y, bar_w, bar_h, prog_pct)
        draw.text((bar_end_x + gap, bar_y - 12), dur_time, font=time_font, fill="white")

        by = io.BytesIO()
        img.save(by, format="PNG")
        by.seek(0)
        by.name = "banner.png"
        return by

    def vertical(self):
        W, H = 1000, 1500
        padding = 80
        cover_size = 800
        title_font = self._get_font(60, self.font_bytes)
        artist_font = self._get_font(45, self.font_bytes)
        time_font = self._get_font(35, self.font_bytes)

        img = self._prepare_background(W, H)
        draw = ImageDraw.Draw(img)

        cover = self._prepare_cover(cover_size, 40)
        cover_x = (W - cover_size) // 2
        cover_y = 120
        img.paste(cover, (cover_x, cover_y), cover)

        text_area_y = cover_y + cover_size + 120
        text_width_limit = W - (padding * 2)

        wrapper = textwrap.TextWrapper(width=23)
        title_lines = wrapper.wrap(self.title)
        
        if len(title_lines) > 2:
            title_lines = title_lines[:2]
            title_lines[-1] += "..."

        current_y = text_area_y
        title_height = title_font.getbbox("Ah")[3] + 15

        for line in title_lines:
            w = title_font.getlength(line)
            draw.text(((W - w) / 2, current_y), line, font=title_font, fill="white")
            current_y += title_height

        display_artist = self.artists
        while artist_font.getlength(display_artist) > text_width_limit and len(display_artist) > 0:
            display_artist = display_artist[:-1]
        if len(display_artist) < len(self.artists): display_artist += "…"

        artist_w = artist_font.getlength(display_artist)
        draw.text(((W - artist_w) / 2, current_y + 15), display_artist, font=artist_font, fill="#b3b3b3")

        bar_y = text_area_y + 260
        if len(title_lines) > 1:
            bar_y += 60

        bar_h = 8
        bar_w = W - (padding * 2)
        prog_pct = self.progress / self.duration if self.duration > 0 else 0
        self._draw_progress_bar(draw, padding, bar_y, bar_w, bar_h, prog_pct, color="white", bg_color="#6b6b6b")

        cur_time = f"{(self.progress//1000//60):02}:{(self.progress//1000%60):02}"
        dur_time = f"{(self.duration//1000//60):02}:{(self.duration//1000%60):02}"
        draw.text((padding, bar_y + 40), cur_time, font=time_font, fill="white")
        dur_w = time_font.getlength(dur_time)
        draw.text((W - padding - dur_w, bar_y + 40), dur_time, font=time_font, fill="white")

        by = io.BytesIO()
        img.save(by, format="PNG")
        by.seek(0)
        by.name = "banner.png"
        return by


@loader.tds
class YTMusic(loader.Module):
    """Эт коррче тулза для дампа музыки из YT с карточками и плейлистами. трахать себе мозг и ставить ffmpeg не нужно"""

    strings = {
        "name": "YTMusic",
        "no_query": "👾 <b>Аргументы где?</b> Укажи таргет или сделай реплай на аудио.",
        "no_tracks": "🕳 <b>Нихуя не найдено.</b> Гугл молчит.",
        "searching": "⏳ <b>Снифаю ютуб...</b>",
        "search_results": "💉 <b>Снифф резалтс для:</b> <i>{query}</i>\nТаргет на мушку:",
        "downloading": "🔥 <b>Дамплю сурс & рендерю...</b>",
        "pl_empty": "🕳 <b>База пуста.</b>",
        "pl_list": "📁 <b>Локальные БД (Плейлисты):</b>\nВыбирай с чем работать:",
        "pl_added": "💉 <b>Вколото в {pl}:</b> {track}",
        "pl_removed": "🗑 <b>Вырезано из {pl}:</b> {track}",
        "pl_view": "📁 <b>База:</b> {pl}\nТреков: {count}",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue("custom_text", "👾 <b>Таргет:</b> {track}\n👤 <b>Хакер:</b> {artists}\n🔗 <b>Лонг-линк:</b> <a href='{ytm_url}'>Сурс</a>", "Шаблон текста под аудио", validator=loader.validators.String()),
            loader.ConfigValue("font", "https://raw.githubusercontent.com/kamekuro/assets/master/fonts/Onest-Bold.ttf", "URL шрифта .ttf", validator=loader.validators.String()),
            loader.ConfigValue("banner_version", "horizontal", "Стиль карточки", validator=loader.validators.Choice(["horizontal", "vertical"])),
            loader.ConfigValue("blur_intensity", 40, "Блюр фона", validator=loader.validators.Integer(minimum=0)),
        )
        self._search_cache = {}
        self.storage_dir = os.path.join(os.getcwd(), "ytmusic_storage")
        os.makedirs(self.storage_dir, exist_ok=True)

    def _cleanup_cache(self):
        if len(self._search_cache) > 30:
            keys_to_remove = list(self._search_cache.keys())[:-20]
            for k in keys_to_remove:
                self._search_cache.pop(k, None)

    def _get_base_dlp_args(self):
        return [sys.executable, "-m", "yt_dlp"]

    def _parse_yt_error(self, stderr_bytes):
        text = stderr_bytes.decode('utf-8', errors='ignore').lower()
        if "403" in text:
            return "Гугл кинул 403 (Форбидден). Айпи в бане или куки сдохли."
        if any(e in text for e in ["timeout", "name or service not known", "unreachable", "connection reset"]):
            return "РКН порвал коннект (DNS/DPI блок Ютуба в РФ). Подрубай проксю/VPN на хосте."
        if "sign in" in text:
            return "Гугл просит логин/капчу. Нужно скормить cookies."
        return "Хз че за дичь, чекай логи хоста."

    async def _search_tracks(self, query, limit=5):
        cmd = self._get_base_dlp_args() + [
            "--dump-json", "--no-warnings", "--ignore-errors",
            f"ytsearch{limit}:{query}"
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        
        results = []
        if stdout:
            for line in stdout.decode('utf-8', errors='ignore').split('\n'):
                if not line.strip(): continue
                try:
                    results.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
        return results

    async def _dl_and_send(self, call_or_msg, track_info, target_chat_id):
        track_id = str(track_info.get("id"))
        source = track_info.get("source", "yt")
        
        track_title = track_info.get("title", "Unknown")
        track_artist = track_info.get("uploader") or track_info.get("channel") or "Unknown Artist"
        duration_sec = track_info.get("duration", 0)

        audio_path = os.path.join(self.storage_dir, f"{track_id}.mp3")
        thumb_path = os.path.join(self.storage_dir, f"{track_id}.jpg")
        banner_path = os.path.join(self.storage_dir, f"{track_id}_banner.png")

        if source == "tg":
            if not os.path.exists(audio_path):
                await utils.answer(call_or_msg, "💀 <b>Файл отсутствует в локальном кэше.</b> Удали из БД и сохрани заново.")
                return
            audio_attrs = [DocumentAttributeAudio(duration=duration_sec, title=track_title, performer=track_artist)]
            await self._client.send_file(target_chat_id, audio_path, caption=f"📁 <b>Локал-база:</b> {utils.escape_html(track_title)}", attributes=audio_attrs)
            with contextlib.suppress(Exception):
                await call_or_msg.delete()
            return

        yt_url = f"https://music.youtube.com/watch?v={track_id}"
        is_cached = os.path.exists(audio_path) and os.path.exists(thumb_path) and os.path.exists(banner_path)

        if not is_cached:
            dl_dir = tempfile.mkdtemp(prefix="ytmusic_cache_")
            try:
                ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()
                cmd = self._get_base_dlp_args() + [
                    "-x", "--audio-format", "mp3",
                    "--ffmpeg-location", ffmpeg_path,
                    "--embed-thumbnail", "--embed-metadata",
                    "--parse-metadata", "title:%(title)s",
                    "--parse-metadata", "artist:%(artist,creator,uploader)s",
                    "--audio-quality", "0", "--no-warnings",
                    "-o", f"{dl_dir}/%(id)s.%(ext)s",
                    yt_url
                ]

                proc = await asyncio.create_subprocess_exec(
                    *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                _, stderr = await proc.communicate()

                if proc.returncode != 0 or not os.path.exists(f"{dl_dir}/{track_id}.mp3"):
                    err_cause = self._parse_yt_error(stderr)
                    err_text = f"💀 <b>Фейл дампа.</b>\n⚠️ {err_cause}"
                    if hasattr(call_or_msg, "edit"):
                        await call_or_msg.edit(err_text)
                    else:
                        await utils.answer(call_or_msg, err_text)
                    return

                cover_url = track_info.get("thumbnails", [{}])[-1].get("url", "https://via.placeholder.com/800")
                
                loop = asyncio.get_running_loop()
                cover_bytes = await loop.run_in_executor(None, _fetch_sync, cover_url)
                font_bytes = await loop.run_in_executor(None, _fetch_sync, self.config["font"])
                
                with open(f"{dl_dir}/thumb.jpg", "wb") as f:
                    f.write(cover_bytes)

                banners = Banners(
                    title=track_title,
                    artists=track_artist,
                    duration=duration_sec * 1000,
                    progress=0, 
                    track_cover=cover_bytes,
                    font_bytes=font_bytes,
                    blur=self.config["blur_intensity"],
                )
                banner_file = banners.vertical() if self.config["banner_version"] == "vertical" else banners.horizontal()
                
                with open(f"{dl_dir}/banner.png", "wb") as f:
                    f.write(banner_file.read())

                shutil.copy(f"{dl_dir}/{track_id}.mp3", audio_path)
                shutil.copy(f"{dl_dir}/thumb.jpg", thumb_path)
                shutil.copy(f"{dl_dir}/banner.png", banner_path)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Download Error: {e}")
                err_text = f"💀 <b>Крах скрипта:</b> <code>{str(e)[:50]}</code>"
                if hasattr(call_or_msg, "edit"):
                    with contextlib.suppress(Exception):
                        await call_or_msg.edit(err_text)
                return
            finally:
                if 'dl_dir' in locals() and os.path.exists(dl_dir):
                    shutil.rmtree(dl_dir, ignore_errors=True)

        audio_attrs = [DocumentAttributeAudio(duration=duration_sec, title=track_title, performer=track_artist)]
        sdata = {
            "track": utils.escape_html(track_title),
            "artists": utils.escape_html(track_artist),
            "duration": f"{duration_sec//60}:{duration_sec%60:02}",
            "yt_url": yt_url,
            "ytm_url": yt_url,
        }
        
        try:
            caption = self.config["custom_text"].format(**sdata)
        except Exception:
            caption = "👾 <b>Таргет:</b> {track}\n👤 <b>Хакер:</b> {artists}\n🔗 <b>Лонг-линк:</b> <a href='{ytm_url}'>Сурс</a>".format(**sdata)

        await self._client.send_file(target_chat_id, banner_path, caption="")
        await self._client.send_file(target_chat_id, audio_path, caption=caption, attributes=audio_attrs, thumb=thumb_path)
        
        with contextlib.suppress(Exception):
            await call_or_msg.delete()

    @loader.command(ru_doc="<запрос> - Дамп трека из YT", alias="yt")
    async def ytcmd(self, message: Message):
        """<query> - Search & download track"""
        args = utils.get_args_raw(message)
        if not args:
            return await utils.answer(message, self.strings("no_query"))

        msg = await utils.answer(message, self.strings("searching"))
        results = await self._search_tracks(args)
        
        if not results:
            return await utils.answer(msg, self.strings("no_tracks"))

        call_id = str(message.id)
        target_chat_id = utils.get_chat_id(message)
        self._search_cache[call_id] = results
        self._cleanup_cache()

        keyboard = []
        for idx, track in enumerate(results):
            title = track.get("title", "Unknown")[:30]
            artist = track.get("uploader") or track.get("channel") or "Unknown"
            artist = artist[:15]
            
            keyboard.append([{
                "text": f"{title} — {artist}",
                "callback": self._inline_search_dl,
                "args": (call_id, idx, target_chat_id)
            }])

        await self.inline.form(
            self.strings("search_results").format(query=utils.escape_html(args)),
            message=msg,
            reply_markup=keyboard
        )

    async def _inline_search_dl(self, call, call_id: str, track_idx: int, target_chat_id: int):
        with contextlib.suppress(Exception):
            await call.answer("🚀 Полетели...")
            await call.edit(self.strings("downloading"), reply_markup=None)

        results = self._search_cache.get(call_id)
        if not results or track_idx >= len(results):
            with contextlib.suppress(Exception):
                await call.edit("💀 <b>Кэш сдох.</b> Ищи заново.")
            return

        track_info = results[track_idx]
        await self._dl_and_send(call, track_info, target_chat_id)

        if call_id in self._search_cache:
            self._search_cache.pop(call_id, None)

    # если ты это читаешь то пошёл нахуй, я этот мусор заебался фиксить

    @loader.command(ru_doc="Менеджер локальных БД (плейлистов)", alias="ytpl")
    async def ytplcmd(self, message: Message):
        """Playlists manager"""
        playlists = self.get("playlists", {"Favs": []})
        if not playlists:
            self.set("playlists", {"Favs": []})
            playlists = {"Favs": []}
            
        keyboard = []
        for pl_name, tracks in playlists.items():
            keyboard.append([{"text": f"📁 {pl_name} [{len(tracks)}]", "callback": self._inline_pl_view, "args": (pl_name,)}])
            
        await self.inline.form(self.strings("pl_list"), message=message, reply_markup=keyboard)

    @loader.command(ru_doc="<база> <запрос/реплай> - Фаст инжект трека в базу", alias="ytadd")
    async def ytaddcmd(self, message: Message):
        """<playlist> <query/reply> - Fast add to playlist"""
        args = utils.get_args_raw(message)
        reply = await message.get_reply_message()
        
        if not args:
            return await utils.answer(message, "👾 <b>Юзаж:</b> <code>.ytadd [БД] [Таргет/Реплай на аудио]</code>")
        
        parts = args.split(" ", 1)
        pl_name = parts[0].strip()
        query = parts[1].strip() if len(parts) > 1 else ""
        
        playlists = self.get("playlists", {"Favs": []})
        if pl_name not in playlists:
            playlists[pl_name] = []
            
        if reply and reply.audio:
            msg = await utils.answer(message, "⏳ <b>Кэширую локально...</b>")
            audio = reply.document
            title = "Unknown"
            artist = "Unknown"
            duration = 0
            
            for attr in audio.attributes:
                if isinstance(attr, DocumentAttributeAudio):
                    title = attr.title or title
                    artist = attr.performer or artist
                    duration = attr.duration or duration
            
            track_id = f"tg_{audio.id}"
            if any(t.get("id") == track_id for t in playlists[pl_name]):
                return await utils.answer(msg, f"⚠️ <b>Таргет уже есть в БД {utils.escape_html(pl_name)}</b>")
                
            storage_path = os.path.join(self.storage_dir, f"{track_id}.mp3")
            if not os.path.exists(storage_path):
                try:
                    await reply.download_media(file=storage_path)
                except Exception as e:
                    return await utils.answer(msg, f"💀 <b>Ошибка скачивания:</b> <code>{str(e)[:50]}</code>")
                
            track = {"id": track_id, "title": title, "uploader": artist, "duration": duration, "source": "tg"}
            playlists[pl_name].append(track)
            self.set("playlists", playlists)
            return await utils.answer(msg, self.strings("pl_added").format(pl=utils.escape_html(pl_name), track=utils.escape_html(title)))

        if not query:
            return await utils.answer(message, self.strings("no_query"))
            
        msg = await utils.answer(message, self.strings("searching"))
        results = await self._search_tracks(query, limit=1)
        if not results:
            return await utils.answer(msg, self.strings("no_tracks"))
            
        track = results[0]
        track["source"] = "yt"
        
        if any(t.get("id") == str(track.get("id")) for t in playlists[pl_name]):
            return await utils.answer(msg, f"⚠️ <b>Таргет уже есть в БД {utils.escape_html(pl_name)}:</b> {utils.escape_html(track.get('title', ''))}")
            
        playlists[pl_name].append(track)
        self.set("playlists", playlists)
        await utils.answer(msg, self.strings("pl_added").format(pl=utils.escape_html(pl_name), track=utils.escape_html(track.get('title', 'Unknown'))))

    @loader.command(ru_doc="<база> <имя_трека> - Вырезать трек из базы", alias="ytrm")
    async def ytrmcmd(self, message: Message):
        """<playlist> <query> - Remove from playlist"""
        args = utils.get_args_raw(message)
        if not args or " " not in args:
            return await utils.answer(message, "👾 <b>Юзаж:</b> <code>.ytrm [БД] [Имя трека]</code>")
            
        pl_name, query = args.split(" ", 1)
        playlists = self.get("playlists", {"Favs": []})
        
        if pl_name not in playlists or not playlists[pl_name]:
            return await utils.answer(message, self.strings("pl_empty"))
            
        original_len = len(playlists[pl_name])
        playlists[pl_name] = [t for t in playlists[pl_name] if query.lower() not in t.get("title", "").lower()]
        
        if len(playlists[pl_name]) == original_len:
            return await utils.answer(message, "💀 <b>Таргет не найден в базе.</b>")
            
        self.set("playlists", playlists)
        await utils.answer(message, self.strings("pl_removed").format(pl=utils.escape_html(pl_name), track=utils.escape_html(query)))

    #  я же сказал те.

    async def _inline_pl_view(self, call, pl_name: str):
        playlists = self.get("playlists", {"Favs": []})
        tracks = playlists.get(pl_name, [])
        
        text = self.strings("pl_view").format(pl=pl_name, count=len(tracks))
        keyboard = []
        
        for idx, t in enumerate(tracks[:15]):
            title = t.get("title", "Unknown")[:30]
            keyboard.append([{"text": f"▶️ {title}", "callback": self._inline_pl_play, "args": (pl_name, idx, call.message.chat_id)}])
            
        keyboard.append([{"text": "🔙 Назад", "callback": self._inline_pl_back}])
        
        with contextlib.suppress(Exception):
            await call.edit(text, reply_markup=keyboard)

    async def _inline_pl_back(self, call):
        playlists = self.get("playlists", {"Favs": []})
        keyboard = []
        for pl_name, tracks in playlists.items():
            keyboard.append([{"text": f"📁 {pl_name} [{len(tracks)}]", "callback": self._inline_pl_view, "args": (pl_name,)}])
        with contextlib.suppress(Exception):
            await call.edit(self.strings("pl_list"), reply_markup=keyboard)

    async def _inline_pl_play(self, call, pl_name: str, track_idx: int, target_chat_id: int):
        playlists = self.get("playlists", {})
        if pl_name not in playlists or track_idx >= len(playlists[pl_name]):
            with contextlib.suppress(Exception):
                await call.answer("💀 База сдохла или трек пропал.")
            return
            
        track_info = playlists[pl_name][track_idx]
        with contextlib.suppress(Exception):
            await call.answer("🚀 Полетели...")
            await call.edit(self.strings("downloading"), reply_markup=None)
            
        await self._dl_and_send(call, track_info, target_chat_id)
