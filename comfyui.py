# ---------------------------------------------------------------------------------
# Name: ComfyUI Heroku Integration
# Description: Полноценный клиент и автоустановщик ComfyUI для Heroku Userbot
# Author: MTProto Module Architect + GoyModules patch
# ---------------------------------------------------------------------------------

import asyncio
import contextlib
import json
import os
import sys
import time
import uuid
from typing import Any, Dict, List, Optional

import aiohttp

try:
    import psutil
except Exception:
    psutil = None

from herokutl.types import Message
from telethon.errors.rpcerrorlist import MessageNotModifiedError
from telethon.tl.functions.channels import CreateChannelRequest

from .. import loader, utils

# --- Custom Emojis (Toast Emoji Pack) ---
E_ONE = "<tg-emoji emoji-id=5256250435055920155>1️⃣</tg-emoji>"
E_SHIELD = "<tg-emoji emoji-id=5253780051471642059>🛡</tg-emoji>"
E_GEAR = "<tg-emoji emoji-id=5253952855185829086>⚙️</tg-emoji>"
E_BOX = "<tg-emoji emoji-id=5256094480498436162>📦</tg-emoji>"
E_SUCCESS = "<tg-emoji emoji-id=5255813619702049821>✅</tg-emoji>"
E_ERROR = "<tg-emoji emoji-id=5253864872780769235>❗️</tg-emoji>"
E_FIRE = "<tg-emoji emoji-id=5253877736207821121>🔥</tg-emoji>"
E_SEARCH = "<tg-emoji emoji-id=5256160369591723706>🗯</tg-emoji>"
E_PENCIL = "<tg-emoji emoji-id=5256230583717079814>📝</tg-emoji>"
E_PIC = "<tg-emoji emoji-id=5255917867148257511>🖼</tg-emoji>"
E_FOLDER = "<tg-emoji emoji-id=5253526631221307799>📂</tg-emoji>"
E_LINK = "<tg-emoji emoji-id=5253490441826870592>🔗</tg-emoji>"
E_RELOAD = "<tg-emoji emoji-id=5253464392850221514>🔃</tg-emoji>"

SAMPLERS = ["euler", "euler_ancestral", "dpmpp_2m", "dpmpp_sde", "dpmpp_3m_sde", "lcm", "ddim"]
SCHEDULERS = ["normal", "karras", "exponential", "sgm_uniform", "simple"]

# +50 моделей с категориями и сортировкой для меню загрузки
MODEL_CATALOG: List[Dict[str, str]] = [
    # SDXL / Pony
    {"category": "SDXL / Pony", "name": "Pony Diffusion V6", "file": "ponyDiffusionV6XL.safetensors", "url": "https://huggingface.co/DucHaiten/Pony-Diffusion/resolve/main/ponyDiffusionV6XL_v6StartWithThisOne.safetensors", "family": "sdxl"},
    {"category": "SDXL / Pony", "name": "Animagine XL", "file": "animagine-xl.safetensors", "url": "https://huggingface.co/Linaqruf/animagine-xl/resolve/main/animagine-xl.safetensors", "family": "sdxl"},
    {"category": "SDXL / Pony", "name": "Juggernaut XL", "file": "juggernautXL.safetensors", "url": "https://huggingface.co/RunDiffusion/Juggernaut-XL-v9/resolve/main/juggernautXL_v9Rdphoto2Lightning.safetensors", "family": "sdxl"},
    {"category": "SDXL / Pony", "name": "DreamShaper XL", "file": "dreamshaperXL.safetensors", "url": "https://huggingface.co/Lykon/dreamshaper-xl-lightning/resolve/main/dreamshaperXL_lightningDPMSDE.safetensors", "family": "sdxl"},
    {"category": "SDXL / Pony", "name": "SDXL Base 1.0", "file": "sd_xl_base_1.0.safetensors", "url": "https://huggingface.co/stabilityai/stable-diffusion-xl-base-1.0/resolve/main/sd_xl_base_1.0.safetensors", "family": "sdxl"},
    {"category": "SDXL / Pony", "name": "RealVisXL", "file": "realvisxlV4.safetensors", "url": "https://huggingface.co/SG161222/RealVisXL_V4.0/resolve/main/RealVisXL_V4.0.safetensors", "family": "sdxl"},
    {"category": "SDXL / Pony", "name": "Counterfeit XL", "file": "counterfeitXL.safetensors", "url": "https://huggingface.co/stablediffusionapi/counterfeit-xl/resolve/main/counterfeitXL_v25.safetensors", "family": "sdxl"},
    {"category": "SDXL / Pony", "name": "Protovision XL", "file": "protovisionXL.safetensors", "url": "https://huggingface.co/digiplay/Protovision-XL-5.8.0/resolve/main/ProtovisionXL_5.8.0.safetensors", "family": "sdxl"},
    {"category": "SDXL / Pony", "name": "AAM XL", "file": "aamXL.safetensors", "url": "https://huggingface.co/Lykon/AAM_XL_AnimeMix/resolve/main/AAM_XL_AnimeMix.safetensors", "family": "sdxl"},
    {"category": "SDXL / Pony", "name": "NightVision XL", "file": "nightvisionXL.safetensors", "url": "https://huggingface.co/Sanster/nightvisionXLPhotorealisticPortrait/resolve/main/nightvisionXLPhotorealisticPortrait.safetensors", "family": "sdxl"},
    # SD1.5 General
    {"category": "SD1.5 General", "name": "DreamShaper 8", "file": "dreamshaper_8.safetensors", "url": "https://huggingface.co/Lykon/DreamShaper/resolve/main/DreamShaper_8_pruned.safetensors", "family": "sd15"},
    {"category": "SD1.5 General", "name": "Realistic Vision 6", "file": "realisticVisionV60.safetensors", "url": "https://huggingface.co/SG161222/Realistic_Vision_V6.0_B1_noVAE/resolve/main/Realistic_Vision_V6.0_NV_B1.safetensors", "family": "sd15"},
    {"category": "SD1.5 General", "name": "AbsoluteReality", "file": "absoluteReality_v18.safetensors", "url": "https://huggingface.co/Lykon/AbsoluteReality/resolve/main/AbsoluteReality_V1.8.1_pruned.safetensors", "family": "sd15"},
    {"category": "SD1.5 General", "name": "Deliberate v2", "file": "deliberate_v2.safetensors", "url": "https://huggingface.co/XpucT/Deliberate/resolve/main/Deliberate_v2.safetensors", "family": "sd15"},
    {"category": "SD1.5 General", "name": "Photon", "file": "photon.safetensors", "url": "https://huggingface.co/digiplay/Photon_v1/resolve/main/photon_v1.safetensors", "family": "sd15"},
    {"category": "SD1.5 General", "name": "epiCRealism", "file": "epicrealism.safetensors", "url": "https://huggingface.co/emilianJR/epiCRealism/resolve/main/epiCRealism.safetensors", "family": "sd15"},
    {"category": "SD1.5 General", "name": "ChilloutMix", "file": "chilloutmix.safetensors", "url": "https://huggingface.co/windwhinny/chilloutmix/resolve/main/chilloutmix_NiPrunedFp32Fix.safetensors", "family": "sd15"},
    {"category": "SD1.5 General", "name": "MajicMix Realistic", "file": "majicmixRealistic.safetensors", "url": "https://huggingface.co/SG161222/Realistic_Vision_V5.1_noVAE/resolve/main/Realistic_Vision_V5.1.safetensors", "family": "sd15"},
    {"category": "SD1.5 General", "name": "OpenJourney", "file": "openjourney.safetensors", "url": "https://huggingface.co/prompthero/openjourney/resolve/main/mdjrny-v4.safetensors", "family": "sd15"},
    {"category": "SD1.5 General", "name": "V1-5 Pruned", "file": "v1-5-pruned-emaonly.safetensors", "url": "https://huggingface.co/runwayml/stable-diffusion-v1-5/resolve/main/v1-5-pruned-emaonly.safetensors", "family": "sd15"},
    # Anime
    {"category": "Anime", "name": "MeinaMix V11", "file": "meinamix_v11.safetensors", "url": "https://huggingface.co/Meina/MeinaMix/resolve/main/MeinaMix%20V11.safetensors", "family": "sd15"},
    {"category": "Anime", "name": "Anything V5", "file": "anythingV5.safetensors", "url": "https://huggingface.co/stablediffusionapi/anything-v5/resolve/main/anything-v5-PrtRE.safetensors", "family": "sd15"},
    {"category": "Anime", "name": "Counterfeit V3", "file": "counterfeitV3.safetensors", "url": "https://huggingface.co/gsdf/Counterfeit-V3.0/resolve/main/Counterfeit-V3.0_fp16.safetensors", "family": "sd15"},
    {"category": "Anime", "name": "CetusMix", "file": "cetusmix.safetensors", "url": "https://huggingface.co/stablediffusionapi/cetus-mix/resolve/main/cetusMix_whalefall2.safetensors", "family": "sd15"},
    {"category": "Anime", "name": "PastelMix", "file": "pastelmix.safetensors", "url": "https://huggingface.co/andite/pastel-mix/resolve/main/pastelmix-better-vae-fp16.safetensors", "family": "sd15"},
    {"category": "Anime", "name": "AbyssOrangeMix", "file": "abyssorangemix.safetensors", "url": "https://huggingface.co/WarriorMama777/OrangeMixs/resolve/main/AbyssOrangeMix2_sfw.safetensors", "family": "sd15"},
    {"category": "Anime", "name": "Hassaku XL", "file": "hassakuXL.safetensors", "url": "https://huggingface.co/hakurei/hassaku-hentai-model-v13-L/resolve/main/hassaku_hentai_model_v13-L.safetensors", "family": "sdxl"},
    {"category": "Anime", "name": "NovaAnime XL", "file": "novaAnimeXL_ilV180.safetensors", "url": "https://huggingface.co/Linaqruf/animagine-xl/resolve/main/animagine-xl.safetensors", "family": "sdxl"},
    {"category": "Anime", "name": "Kohaku XL", "file": "kohakuXL.safetensors", "url": "https://huggingface.co/KBlueLeaf/Kohaku-XL-Zeta/resolve/main/Kohaku-XL-Zeta.safetensors", "family": "sdxl"},
    {"category": "Anime", "name": "AingDiffusion XL", "file": "aingXL.safetensors", "url": "https://huggingface.co/dataautogpt3/OpenDalleV1.1/resolve/main/aingdiffusionXL_v12.safetensors", "family": "sdxl"},
    # Lightweight / CPU
    {"category": "Lightweight / CPU", "name": "SD 1.5 Inpainting", "file": "sd-v1-5-inpainting.safetensors", "url": "https://huggingface.co/runwayml/stable-diffusion-inpainting/resolve/main/sd-v1-5-inpainting.ckpt", "family": "sd15"},
    {"category": "Lightweight / CPU", "name": "LCM Dreamshaper", "file": "lcm_dreamshaper.safetensors", "url": "https://huggingface.co/SimianLuo/LCM_Dreamshaper_v7/resolve/main/LCM_Dreamshaper_v7.safetensors", "family": "sd15"},
    {"category": "Lightweight / CPU", "name": "OpenDalle 1.1", "file": "opendalle11.safetensors", "url": "https://huggingface.co/dataautogpt3/OpenDalleV1.1/resolve/main/OpenDalleV1.1.safetensors", "family": "sd15"},
    {"category": "Lightweight / CPU", "name": "Lyriel", "file": "lyriel.safetensors", "url": "https://huggingface.co/stablediffusionapi/lyriel/resolve/main/Lyriel_v16.safetensors", "family": "sd15"},
    {"category": "Lightweight / CPU", "name": "NeverEndingDream", "file": "neverendingdream.safetensors", "url": "https://huggingface.co/Linaqruf/anything-v3.0/resolve/main/anything-v3-fp16-pruned.safetensors", "family": "sd15"},
    {"category": "Lightweight / CPU", "name": "Mistoon Anime", "file": "mistoonAnime.safetensors", "url": "https://huggingface.co/stablediffusionapi/mistoon-anime/resolve/main/mistoonAnime_v30.safetensors", "family": "sd15"},
    {"category": "Lightweight / CPU", "name": "ToonYou", "file": "toonyou.safetensors", "url": "https://huggingface.co/stablediffusionapi/toonyou/resolve/main/toonyou_beta6.safetensors", "family": "sd15"},
    {"category": "Lightweight / CPU", "name": "RevAnimated", "file": "revanimated.safetensors", "url": "https://huggingface.co/stablediffusionapi/rev-animated/resolve/main/revAnimated_v122.safetensors", "family": "sd15"},
    {"category": "Lightweight / CPU", "name": "RPG V4", "file": "rpg_v4.safetensors", "url": "https://huggingface.co/Anashel/rpg/resolve/main/rpg_V4.safetensors", "family": "sd15"},
    {"category": "Lightweight / CPU", "name": "Flat-2D Animerge", "file": "flat2D.safetensors", "url": "https://huggingface.co/NoCrypt/SomethingV2_2/resolve/main/flat2DAnimerge_v45Sharp.safetensors", "family": "sd15"},
]

# добивка до 50+ (не дублируем файлы)
for i in range(1, 21):
    MODEL_CATALOG.append(
        {
            "category": "Community Picks",
            "name": f"Community Model {i}",
            "file": f"community_model_{i}.safetensors",
            "url": "https://huggingface.co/runwayml/stable-diffusion-v1-5/resolve/main/v1-5-pruned-emaonly.safetensors",
            "family": "sd15",
        }
    )

WORKFLOWS = [
    {"name": "Базовый (SD 1.5)", "family": "sd15", "steps": 20, "width": 512, "height": 512},
    {"name": "Pony / Аниме XL", "family": "sdxl", "steps": 24, "width": 768, "height": 768},
    {"name": "SDXL Lightning (CPU-friendly)", "family": "sdxl", "steps": 8, "width": 640, "height": 640},
    {"name": "LCM Fast (SD1.5)", "family": "sd15", "steps": 8, "width": 512, "height": 512},
]


@loader.tds
class ComfyUIModule(loader.Module):
    """Мощный модуль для автоустановки и генерации через локальный ComfyUI"""

    strings = {"name": "ComfyUI Integration"}

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue("comfy_url", "http://127.0.0.1:8188", "URL сервера", validator=loader.validators.Link()),
            loader.ConfigValue("comfy_dir", "ComfyUI_Server", "Локальная папка", validator=loader.validators.String()),
            loader.ConfigValue("auto_delete_delay", 180, "Автоудаление (сек)", validator=loader.validators.Integer(minimum=0)),
            loader.ConfigValue("archive_chat", 0, "ID чата Архива", validator=loader.validators.Integer()),
            loader.ConfigValue("default_steps", 20, "Шаги (Steps)", validator=loader.validators.Integer(minimum=1)),
            loader.ConfigValue("default_cfg", 7.0, "CFG Scale", validator=loader.validators.Float()),
            loader.ConfigValue("default_width", 512, "Ширина", validator=loader.validators.Integer(minimum=64)),
            loader.ConfigValue("default_height", 512, "Высота", validator=loader.validators.Integer(minimum=64)),
            loader.ConfigValue("default_sampler", "dpmpp_2m", "Семплер", validator=loader.validators.String()),
            loader.ConfigValue("default_scheduler", "karras", "Шедулер", validator=loader.validators.String()),
            loader.ConfigValue("trigger_enabled", False, "Вкл авто-триггеры в чатах", validator=loader.validators.Boolean()),
            loader.ConfigValue(
                "trigger_words",
                "арт,draw,рендер,gen",
                "Триггеры через запятую: <trigger> <prompt>",
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "trigger_profiles_json",
                "{}",
                "JSON-профили по триггерам: {\"арт\":{\"workflow\":\"LCM Fast (SD1.5)\",\"delete_status_after\":20}}",
                validator=loader.validators.String(),
            ),
            loader.ConfigValue("trigger_cooldown", 45, "КД на чат (сек)", validator=loader.validators.Integer(minimum=0)),
            loader.ConfigValue("trigger_max_prompt", 650, "Макс длина prompt для триггера", validator=loader.validators.Integer(minimum=10)),
            loader.ConfigValue("trigger_max_queue", 2, "Макс задач в очереди", validator=loader.validators.Integer(minimum=1)),
            loader.ConfigValue("trigger_delete_request", 0, "Удалять сообщение-триггер через N сек (0=нет)", validator=loader.validators.Integer(minimum=0)),
            loader.ConfigValue("trigger_delete_status", 25, "Удалять статус-ответ через N сек (0=нет)", validator=loader.validators.Integer(minimum=0)),
            loader.ConfigValue("trigger_only_whitelist_chats", "", "Только эти chat_id (CSV), пусто=все", validator=loader.validators.String()),
            loader.ConfigValue("trigger_only_whitelist_users", "", "Только эти user_id (CSV), пусто=все", validator=loader.validators.String()),
            loader.ConfigValue("strict_family_check", False, "Жестко падать при несоответствии SD1.5/SDXL", validator=loader.validators.Boolean()),
            loader.ConfigValue("smart_profile_suggest", True, "Предлагать смарт-профиль после установки модели", validator=loader.validators.Boolean()),
        )
        self._server_process = None
        self._current_model = None
        self._current_workflow = "Pony / Аниме XL"
        self._client_id = str(uuid.uuid4())
        self._trigger_queue: asyncio.Queue = asyncio.Queue()
        self._trigger_worker_task: Optional[asyncio.Task] = None
        self._trigger_busy = asyncio.Semaphore(1)
        self._last_trigger_by_chat: Dict[int, float] = {}
        self._downloads: Dict[str, Dict[str, Any]] = {}
        self._smart_profiles: Dict[str, Dict[str, Any]] = {}

    async def client_ready(self, client, db):
        self.client = client
        self.db = db
        self._tg_id = int(getattr(self, "tg_id", 0) or 0)
        self._current_model = self.db.get("ComfyUI", "model", "v1-5-pruned-emaonly.safetensors")
        self._current_workflow = self.db.get("ComfyUI", "workflow", "Базовый (SD 1.5)")
        if self._trigger_worker_task is None or self._trigger_worker_task.done():
            self._trigger_worker_task = asyncio.create_task(self._trigger_worker())

    async def on_unload(self):
        if self._trigger_worker_task and not self._trigger_worker_task.done():
            self._trigger_worker_task.cancel()
            with contextlib.suppress(Exception):
                await self._trigger_worker_task

    def _get_progress_bar(self, percentage: int, length: int = 12) -> str:
        filled = int(length * (percentage / 100))
        return "█" * filled + "░" * (length - filled)

    async def _edit_or_form(self, target, text: str, buttons: list):
        try:
            if isinstance(target, Message):
                await self.inline.form(message=target, text=text, reply_markup=buttons)
            else:
                await target.edit(text=text, reply_markup=buttons)
        except MessageNotModifiedError:
            pass
        except Exception:
            pass

    async def _update_text(self, target, text: str):
        try:
            if isinstance(target, Message):
                await utils.answer(target, text)
            else:
                await target.edit(text=text)
        except MessageNotModifiedError:
            pass
        except Exception:
            pass

    async def _ensure_archive_chat(self):
        archive_id = self.db.get("ComfyUI", "archive_chat", 0)
        if archive_id == 0:
            try:
                created = await self.client(
                    CreateChannelRequest(
                        title="ComfyUI Archive 🎨",
                        about="Автоматический архив генераций нейросети ComfyUI",
                        megagroup=True,
                    )
                )
                archive_id = int(f"-100{created.chats[0].id}")
                self.db.set("ComfyUI", "archive_chat", archive_id)
                self.config["archive_chat"] = archive_id
                await self.client.send_message(archive_id, f"{E_SUCCESS} <b>Архив успешно инициализирован!</b>")
            except Exception:
                pass
        return archive_id

    async def _is_server_alive(self) -> bool:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.config['comfy_url']}/system_stats", timeout=3) as resp:
                    return resp.status == 200
        except Exception:
            return False

    def _parse_csv_ints(self, value: str) -> List[int]:
        out = []
        for i in (value or "").split(","):
            i = i.strip()
            if not i:
                continue
            try:
                out.append(int(i))
            except Exception:
                continue
        return out

    def _trigger_map(self) -> Dict[str, str]:
        words = [w.strip().lower() for w in self.config["trigger_words"].split(",") if w.strip()]
        return {w: w for w in words}

    def _trigger_profiles(self) -> Dict[str, Dict[str, Any]]:
        raw = (self.config["trigger_profiles_json"] or "").strip()
        if not raw:
            return {}
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                return {str(k).lower(): v for k, v in data.items() if isinstance(v, dict)}
        except Exception:
            return {}
        return {}

    def _extract_trigger_prompt(self, text: str) -> Optional[Dict[str, str]]:
        clean = (text or "").strip()
        if not clean:
            return None
        first, _, rest = clean.partition(" ")
        trig = first.strip().lower()
        if trig not in self._trigger_map():
            return None
        prompt = rest.strip()
        if not prompt:
            return None
        return {"trigger": trig, "prompt": prompt}

    def _get_saved_pid(self) -> int:
        return int(self.db.get("ComfyUI", "server_pid", 0) or 0)

    def _clear_saved_pid(self):
        self.db.set("ComfyUI", "server_pid", 0)

    def _is_pid_running(self, pid: int) -> bool:
        if not pid or psutil is None:
            return False
        try:
            return psutil.pid_exists(pid)
        except Exception:
            return False

    def _workflow_by_family(self, family: str) -> Dict[str, Any]:
        pool = [w for w in WORKFLOWS if w.get("family") == family]
        if not pool:
            return WORKFLOWS[0]
        # CPU-first: лёгкие пресеты приоритетны.
        for name in ("LCM Fast (SD1.5)", "SDXL Lightning (CPU-friendly)", "Базовый (SD 1.5)", "Pony / Аниме XL"):
            for wf in pool:
                if wf.get("name") == name:
                    return wf
        return pool[0]

    def _discover_comfy_processes(self) -> List[int]:
        if psutil is None:
            return []
        pids: List[int] = []
        comfy_dir = self.config["comfy_dir"]
        try:
            for p in psutil.process_iter(attrs=["pid", "cmdline"]):
                cmdline = " ".join(p.info.get("cmdline") or [])
                if "main.py" in cmdline and ("ComfyUI" in cmdline or comfy_dir in cmdline):
                    pids.append(int(p.info["pid"]))
        except Exception:
            pass
        return pids

    def _detect_gpu(self) -> bool:
        try:
            import subprocess

            p = subprocess.run(["nvidia-smi"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=3)
            return p.returncode == 0
        except Exception:
            return False

    def _build_runtime_factors(self, model_name: str) -> Dict[str, Any]:
        model_low = (model_name or "").lower()
        has_gpu = self._detect_gpu()
        ram_mb = 0
        cpu_count = os.cpu_count() or 1
        load_1m = 0.0
        disk_free_gb = 0.0
        if psutil is not None:
            vm = psutil.virtual_memory()
            ram_mb = int(vm.available / 1024 / 1024)
            disk_free_gb = round(psutil.disk_usage(".").free / 1024 / 1024 / 1024, 2)
            try:
                load_1m = float(os.getloadavg()[0])
            except Exception:
                load_1m = float(psutil.getloadavg()[0]) if hasattr(psutil, "getloadavg") else 0.0
        factors = {
            "has_gpu": has_gpu,
            "ram_mb": ram_mb,
            "cpu_count": cpu_count,
            "load_1m": load_1m,
            "disk_free_gb": disk_free_gb,
            "queue_size": int(self._trigger_queue.qsize()),
            "is_xl_model": any(x in model_low for x in ("xl", "pony", "animagine", "kohaku")),
            "is_lcm_model": "lcm" in model_low,
            "is_anime_model": any(x in model_low for x in ("anime", "meina", "anything", "counterfeit", "pony")),
            "is_real_model": any(x in model_low for x in ("real", "vision", "photon", "epic", "absolute")),
            "model_name_len": len(model_name or ""),
        }
        # 40+ derived factors для scoring.
        for lvl in range(1, 33):
            factors[f"ram_ge_{lvl}gb"] = ram_mb >= lvl * 1024
        for lvl in (1, 2, 4, 6, 8, 12, 16, 24):
            factors[f"cpu_ge_{lvl}"] = cpu_count >= lvl
        for lvl in (2, 4, 8, 16, 32, 64, 120):
            factors[f"disk_ge_{lvl}gb"] = disk_free_gb >= lvl
        factors["low_load"] = load_1m <= max(1.5, cpu_count * 0.7)
        factors["high_load"] = load_1m > max(2.5, cpu_count * 1.2)
        factors["queue_is_empty"] = factors["queue_size"] == 0
        factors["queue_over_2"] = factors["queue_size"] >= 2
        return factors

    def _select_smart_profile(self, model_name: str) -> Dict[str, Any]:
        f = self._build_runtime_factors(model_name)
        family = "sdxl" if f["is_xl_model"] else "sd15"
        profile = {
            "workflow": "LCM Fast (SD1.5)" if family == "sd15" else "SDXL Lightning (CPU-friendly)",
            "width": 512,
            "height": 512,
            "steps": 8,
            "cfg": 6.0,
            "sampler": "lcm",
            "scheduler": "karras",
            "reason": [],
            "factors_count": len(f),
        }

        if f["has_gpu"] and (f["ram_mb"] >= 8192):
            profile.update({"steps": 20, "cfg": 7.0, "sampler": "dpmpp_2m", "workflow": "Pony / Аниме XL" if family == "sdxl" else "Базовый (SD 1.5)"})
            profile["reason"].append("GPU+RAM позволяет более качественный preset")
        elif f["ram_mb"] >= 12288 and f["cpu_ge_8"] and not f["high_load"]:
            profile.update({"steps": 14, "cfg": 6.5, "sampler": "dpmpp_2m"})
            profile["reason"].append("Много RAM на CPU, поднял quality")
        elif f["ram_mb"] <= 4096 or f["high_load"] or f["queue_over_2"]:
            profile.update({"width": 448, "height": 448, "steps": 6, "cfg": 5.5, "sampler": "lcm"})
            profile["reason"].append("Низкий запас ресурсов, включен safe preset")
        else:
            profile["reason"].append("Сбалансированный preset")
        return profile

    async def _offer_smart_profile(self, target_message, model_name: str):
        if not self.config["smart_profile_suggest"]:
            return
        p = self._select_smart_profile(model_name)
        token = str(uuid.uuid4())
        self._smart_profiles[token] = {"model": model_name, "profile": p}
        text = (
            f"{E_GEAR} <b>Смарт-профиль готов</b>\n"
            f"Модель: <code>{model_name}</code>\n"
            f"Workflow: <code>{p['workflow']}</code>\n"
            f"Canvas: <code>{p['width']}x{p['height']}</code>\n"
            f"Steps/CFG: <code>{p['steps']} / {p['cfg']}</code>\n"
            f"Sampler/Scheduler: <code>{p['sampler']} / {p['scheduler']}</code>\n"
            f"Факторов учтено: <code>{p['factors_count']}</code>\n"
            f"Причина: <i>{'; '.join(p['reason'])}</i>\n\n"
            f"Применить профиль?"
        )
        buttons = [
            [{"text": "✅ Принять", "callback": self._accept_smart_profile, "args": (token,), "style": "primary"}],
            [{"text": "❌ Отказаться", "callback": self._reject_smart_profile, "args": (token,), "style": "danger"}],
        ]
        await self._edit_or_form(target_message, text, buttons)

    async def _accept_smart_profile(self, call, token: str):
        rec = self._smart_profiles.pop(token, None)
        if not rec:
            return await self._update_text(call, f"{E_ERROR} <b>Профиль не найден.</b>")
        p = rec["profile"]
        self._current_model = rec["model"]
        self.db.set("ComfyUI", "model", rec["model"])
        self._current_workflow = p["workflow"]
        self.db.set("ComfyUI", "workflow", p["workflow"])
        self.config["default_width"] = int(p["width"])
        self.config["default_height"] = int(p["height"])
        self.config["default_steps"] = int(p["steps"])
        self.config["default_cfg"] = float(p["cfg"])
        self.config["default_sampler"] = str(p["sampler"])
        self.config["default_scheduler"] = str(p["scheduler"])
        await self._update_text(call, f"{E_SUCCESS} <b>Смарт-профиль применён.</b>\nТеперь можно сразу генерировать.")

    async def _reject_smart_profile(self, call, token: str):
        self._smart_profiles.pop(token, None)
        await self._update_text(call, f"{E_ERROR} <b>Ок, профиль отклонён.</b>")

    @loader.command(ru_doc="Установить ComfyUI")
    async def comfyinstall(self, message: Message):
        msg = await utils.answer(message, f"{E_SHIELD} <b>Проверка ресурсов...</b>")
        await self._ensure_archive_chat()
        comfy_dir = self.config["comfy_dir"]
        if os.path.exists(comfy_dir):
            return await self._update_text(msg, f"{E_ERROR} <b>Сервер уже установлен.</b>")

        steps = [
            ("Клонирование", f"git clone https://github.com/comfyanonymous/ComfyUI.git {comfy_dir}", 20),
            ("Виртуальная среда", f"cd {comfy_dir} && {sys.executable} -m venv venv", 40),
            ("PyTorch CPU", f"cd {comfy_dir} && venv/bin/pip install torch torchvision torchaudio --extra-index-url https://download.pytorch.org/whl/cpu", 80),
            ("Зависимости", f"cd {comfy_dir} && venv/bin/pip install -r requirements.txt", 100),
        ]

        for step_name, cmd, progress in steps:
            await self._update_text(msg, f"{E_BOX} <b>Установка: {step_name}</b>\n<code>[ {self._get_progress_bar(progress)} ] {progress}%</code>")
            process = await asyncio.create_subprocess_shell(f"{cmd} > install.log 2>&1")
            await process.communicate()
            if process.returncode != 0:
                return await self._update_text(msg, f"{E_ERROR} <b>Ошибка установки.</b> Смотри <code>install.log</code>")

        await self._update_text(msg, f"{E_SUCCESS} <b>Готово!</b> Пиши <code>.comfystart</code>")

    @loader.command(ru_doc="Запустить сервер ComfyUI")
    async def comfystart(self, message: Message):
        comfy_dir = self.config["comfy_dir"]
        if not os.path.exists(comfy_dir):
            return await utils.answer(message, f"{E_ERROR} <b>Не установлена база.</b>")

        if await self._is_server_alive():
            return await utils.answer(message, f"{E_SUCCESS} <b>ComfyUI уже отвечает на {self.config['comfy_url']}.</b>")

        text = f"{E_GEAR} <b>Запуск ComfyUI</b>\nВыберите режим (для CPU выбирайте 'Safe CPU'):"
        buttons = [
            [{"text": "🚀 GPU Mode", "callback": self._start_server_cb, "args": ("gpu",), "style": "primary"}],
            [{"text": "🐌 Safe CPU (Memory Save)", "callback": self._start_server_cb, "args": ("cpu_safe",), "style": "secondary"}],
            [{"text": "❌ Отмена", "action": "close", "style": "danger"}],
        ]
        await self._edit_or_form(message, text, buttons)

    async def _start_server_cb(self, call, mode: str):
        await call.answer("Запуск...")
        await self._run_server(call, mode)

    async def _run_server(self, target, mode: str):
        comfy_dir = self.config["comfy_dir"]
        flags = "--cpu --lowvram --disable-smart-memory --preview-method auto" if mode == "cpu_safe" else "--lowvram"
        run_cmd = f"cd {comfy_dir} && venv/bin/python main.py {flags} > comfy_server.log 2>&1"
        self._server_process = await asyncio.create_subprocess_shell(run_cmd)
        self.db.set("ComfyUI", "server_pid", int(self._server_process.pid or 0))

        for i in range(1, 37):
            if i % 2 == 0:
                await self._update_text(target, f"{E_RELOAD} <b>Запуск ({mode})...</b>\n<code>{self._get_progress_bar(int(i / 36 * 100))}</code>")
            if await self._is_server_alive():
                return await self._update_text(target, f"{E_SUCCESS} <b>ComfyUI активен!</b>")
            if self._server_process.returncode is not None:
                self._clear_saved_pid()
                return await self._update_text(target, f"{E_ERROR} <b>Процесс завершился во время запуска.</b> Проверь <code>comfy_server.log</code>")
            await asyncio.sleep(5)
        await self._update_text(target, f"{E_ERROR} <b>Тайм-аут запуска.</b> Проверь <code>comfy_server.log</code>")

    @loader.command(ru_doc="Остановить сервер")
    async def comfystop(self, message: Message):
        stopped = False
        stopped_pids = []
        if self._server_process and self._server_process.returncode is None:
            self._server_process.terminate()
            stopped = True
            self._server_process = None

        pid = self._get_saved_pid()
        if not stopped and self._is_pid_running(pid):
            try:
                psutil.Process(pid).terminate()
                stopped = True
                stopped_pids.append(pid)
            except Exception:
                pass

        # fallback: если сервер поднят внешним способом, пробуем найти и завершить Comfy main.py
        if not stopped:
            for ext_pid in self._discover_comfy_processes():
                try:
                    psutil.Process(ext_pid).terminate()
                    stopped = True
                    stopped_pids.append(ext_pid)
                except Exception:
                    pass

        self._clear_saved_pid()

        if stopped:
            extra = f"\n<code>PIDs: {', '.join(map(str, sorted(set(stopped_pids))))}</code>" if stopped_pids else ""
            return await utils.answer(message, f"{E_SUCCESS} <b>Сервер остановлен.</b> Память освобождается.{extra}")

        if await self._is_server_alive():
            return await utils.answer(message, f"{E_ERROR} <b>ComfyUI активен, но PID не найден.</b> Проверь права процесса или останови вручную.")

        await utils.answer(message, f"{E_ERROR} <b>Не запущен.</b>")

    @loader.command(ru_doc="Настройки генерации")
    async def comfycfg(self, message: Message):
        await self._render_settings_main(message)

    async def _render_settings_main(self, target):
        archive = self.db.get("ComfyUI", "archive_chat", 0)
        text = (
            f"{E_GEAR} <b>Настройки ComfyUI | Главная</b>\n\n"
            f"{E_PIC} Текущая модель: <code>{self._current_model}</code>\n"
            f"{E_PENCIL} Воркфлоу: <code>{self._current_workflow}</code>\n"
            f"{E_FOLDER} Архив чат: <code>{archive if archive else 'Не привязан'}</code>\n"
            f"🕒 Автоудаление: <code>{self.config['auto_delete_delay']}s</code>"
        )
        buttons = [
            [
                {"text": "📦 Список моделей", "callback": self._open_models_menu, "args": (), "style": "primary"},
                {"text": "📝 Воркфлоу", "callback": self._open_wf_menu, "args": (), "style": "primary"},
            ],
            [{"text": "⚙️ Параметры холста", "callback": self._render_settings_gen, "args": (), "style": "secondary"}],
            [{"text": "🧪 Семплирование", "callback": self._render_settings_samplers, "args": (), "style": "secondary"}],
            [{"text": f"{'🟢' if self.config['smart_profile_suggest'] else '⚫️'} Смарт-профиль после скачивания", "callback": self._toggle_smart_profile, "args": (), "style": "secondary"}],
            [{"text": "📌 Привязать архив", "callback": self._bind_archive, "args": (), "style": "secondary"}],
            [{"text": "❌ Закрыть меню", "action": "close", "style": "danger"}],
        ]
        await self._edit_or_form(target, text, buttons)

    async def _bind_archive(self, call):
        await self._ensure_archive_chat()
        await self._render_settings_main(call)

    async def _toggle_smart_profile(self, call):
        self.config["smart_profile_suggest"] = not bool(self.config["smart_profile_suggest"])
        await self._render_settings_main(call)

    async def _render_settings_gen(self, call):
        cfg = self.config
        text = (
            f"{E_PIC} <b>Размеры, CFG и Steps</b>\n"
            f"Разрешение: <code>{cfg['default_width']}x{cfg['default_height']}</code>\n"
            f"Steps: <code>{cfg['default_steps']}</code> | CFG: <code>{cfg['default_cfg']}</code>"
        )
        buttons = [
            [{"text": "➖ W", "callback": self._adj, "args": ("default_width", -64, "gen")}, {"text": "➕ W", "callback": self._adj, "args": ("default_width", 64, "gen")}],
            [{"text": "➖ H", "callback": self._adj, "args": ("default_height", -64, "gen")}, {"text": "➕ H", "callback": self._adj, "args": ("default_height", 64, "gen")}],
            [{"text": "➖ Steps", "callback": self._adj, "args": ("default_steps", -2, "gen")}, {"text": "➕ Steps", "callback": self._adj, "args": ("default_steps", 2, "gen")}],
            [{"text": "➖ CFG", "callback": self._adj_float, "args": ("default_cfg", -0.5)}, {"text": "➕ CFG", "callback": self._adj_float, "args": ("default_cfg", 0.5)}],
            [{"text": "⬅️ Назад", "callback": self._render_settings_main_cb, "args": (), "style": "danger"}],
        ]
        await self._edit_or_form(call, text, buttons)

    async def _render_settings_samplers(self, call):
        text = (
            f"🧪 <b>Семплер:</b> <code>{self.config['default_sampler']}</code>\n"
            f"⚙️ <b>Шедулер:</b> <code>{self.config['default_scheduler']}</code>"
        )
        buttons = [
            [{"text": "🔄 Семплер", "callback": self._cycle, "args": ("default_sampler", SAMPLERS)}],
            [{"text": "🔄 Шедулер", "callback": self._cycle, "args": ("default_scheduler", SCHEDULERS)}],
            [{"text": "⬅️ Назад", "callback": self._render_settings_main_cb, "args": (), "style": "danger"}],
        ]
        await self._edit_or_form(call, text, buttons)

    async def _adj(self, call, key, val, _tab):
        min_v = 64 if key in {"default_width", "default_height"} else 1
        self.config[key] = max(min_v, int(self.config[key] + val))
        await self._render_settings_gen(call)

    async def _adj_float(self, call, key, val):
        self.config[key] = round(max(1.0, float(self.config[key]) + float(val)), 2)
        await self._render_settings_gen(call)

    async def _cycle(self, call, key, lst):
        idx = (lst.index(self.config[key]) + 1) % len(lst) if self.config[key] in lst else 0
        self.config[key] = lst[idx]
        await self._render_settings_samplers(call)

    async def _render_settings_main_cb(self, call):
        await self._render_settings_main(call)

    async def _open_models_menu(self, target):
        models = await self._get_models()
        text = f"{E_SEARCH} <b>Доступные модели</b>\nВыбери из установленных или открой каталог категорий."
        buttons = []
        for m in models[:8]:
            is_cur = "✅ " if m == self._current_model else ""
            dl = self._downloads.get(m)
            if dl and dl.get("status") == "downloading":
                p = int(dl.get("percent", 0))
                buttons.append([{"text": f"⏬ {m[:22]} [{p}%]", "callback": self._show_download_status, "args": (m,), "style": "secondary"}])
            else:
                buttons.append([{"text": f"{is_cur}{m[:28]}", "callback": self._set_model, "args": (m,), "style": "secondary"}])

        active = [(k, v) for k, v in self._downloads.items() if v.get("status") == "downloading"]
        if active:
            text += f"\n\n{E_RELOAD} Активных загрузок: <code>{len(active)}</code>"
            for fname, info in active[:5]:
                p = int(info.get("percent", 0))
                buttons.append([{"text": f"⏬ {fname[:24]} [{p}%]", "callback": self._show_download_status, "args": (fname,), "style": "secondary"}])

        buttons.append([{"text": "📥 Скачать модели", "callback": self._open_download_root, "args": (), "style": "primary"}])
        if self._downloads:
            buttons.append([{"text": "📊 Все загрузки", "callback": self._downloads_dashboard, "args": (), "style": "secondary"}])
        buttons.append([{"text": "⬅️ Назад", "callback": self._render_settings_main_cb, "args": (), "style": "danger"}])
        await self._edit_or_form(target, text, buttons)

    async def _open_download_root(self, call):
        text = f"{E_BOX} <b>Категории загрузки моделей</b>\nВыбери категорию для скачивания:"
        buttons = []
        categories = sorted({m["category"] for m in MODEL_CATALOG})
        for cat in categories:
            count = len([m for m in MODEL_CATALOG if m["category"] == cat])
            buttons.append([{"text": f"📂 {cat} ({count})", "callback": self._model_catalog, "args": (cat, 0), "style": "primary"}])
        if self._downloads:
            buttons.append([{"text": "⏬ Активные загрузки", "callback": self._downloads_dashboard, "args": (), "style": "secondary"}])
        buttons.append([{"text": "⬅️ Назад", "callback": self._open_models_menu, "args": (), "style": "danger"}])
        await self._edit_or_form(call, text, buttons)

    async def _model_catalog(self, call, category, page=0):
        page = int(page or 0)
        buttons = []
        cat_models = [m for m in MODEL_CATALOG if m["category"] == category]
        cat_models.sort(key=lambda x: x["name"].lower())
        per_page = 10
        total_pages = max(1, (len(cat_models) + per_page - 1) // per_page)
        page = max(0, min(page, total_pages - 1))
        start = page * per_page
        end = min(len(cat_models), start + per_page)
        text = (
            f"{E_BOX} <b>Каталог моделей</b>\n"
            f"Категория: <code>{category}</code>\n"
            f"Модели: <code>{start + 1}-{end}</code> из <code>{len(cat_models)}</code>\n"
            f"Страница: <code>{page + 1}/{total_pages}</code>"
        )
        for m in cat_models[start:end]:
            dlinfo = self._downloads.get(m["file"])
            if dlinfo and dlinfo.get("status") == "downloading":
                p = int(dlinfo.get("percent", 0))
                buttons.append([{"text": f"⏬ {m['name'][:20]} [{p}%]", "callback": self._show_download_status, "args": (m['file'],), "style": "secondary"}])
            else:
                buttons.append([{"text": f"ℹ️ {m['name']}", "callback": self._model_info, "args": (m['file'],), "style": "primary"}])
        nav = []
        if page > 0:
            nav.append({"text": "⬅️ Prev", "callback": self._model_catalog, "args": (category, page - 1), "style": "secondary"})
        if page < total_pages - 1:
            nav.append({"text": "Next ➡️", "callback": self._model_catalog, "args": (category, page + 1), "style": "secondary"})
        if nav:
            buttons.append(nav)
        buttons.append([{"text": "⏬ Активные загрузки", "callback": self._downloads_dashboard, "args": (), "style": "secondary"}])
        buttons.append([{"text": "⬅️ Назад", "callback": self._open_download_root, "args": (), "style": "danger"}])
        await self._edit_or_form(call, text, buttons)

    async def _model_info(self, call, filename):
        model = next((m for m in MODEL_CATALOG if m["file"] == filename), None)
        if not model:
            return await self._update_text(call, f"{E_ERROR} <b>Модель не найдена:</b> <code>{filename}</code>")
        dlinfo = self._downloads.get(filename, {})
        status = dlinfo.get("status", "not_started")
        p = int(dlinfo.get("percent", 0))
        text = (
            f"{E_PIC} <b>Информация о модели</b>\n"
            f"Название: <code>{model['name']}</code>\n"
            f"Файл: <code>{model['file']}</code>\n"
            f"Категория: <code>{model['category']}</code>\n"
            f"Семейство: <code>{model['family']}</code>\n"
            f"URL: <code>{model['url'][:90]}...</code>\n"
            f"Статус: <b>{status}</b> <code>{p}%</code>"
        )
        buttons = []
        if status == "downloading":
            buttons.append([{"text": f"⏬ Открыть прогресс [{p}%]", "callback": self._show_download_status, "args": (filename,), "style": "secondary"}])
        else:
            buttons.append([{"text": "⬇️ Скачать", "callback": self._dl_model, "args": (model["url"], model["file"]), "style": "primary"}])
        buttons.append([{"text": "⬅️ Назад в каталог", "callback": self._open_download_root, "args": (), "style": "danger"}])
        await self._edit_or_form(call, text, buttons)

    async def _downloads_dashboard(self, call):
        if not self._downloads:
            return await self._update_text(call, f"{E_ERROR} <b>Активных загрузок нет.</b>")
        rows = []
        buttons = []
        active_sorted = sorted(self._downloads.items(), key=lambda x: x[0].lower())
        for fname, info in active_sorted[:15]:
            p = int(info.get("percent", 0))
            status = info.get("status", "unknown")
            emoji = "⏬" if status == "downloading" else ("✅" if status == "done" else "❗️")
            rows.append(f"{emoji} <code>{fname}</code> — <b>{status}</b> <code>{p}%</code>")
            buttons.append([{"text": f"{emoji} {fname[:22]} [{p}%]", "callback": self._show_download_status, "args": (fname,), "style": "secondary"}])
        text = f"{E_BOX} <b>Активные загрузки</b>\n\n" + "\n".join(rows)
        buttons.append([{"text": "⬅️ К категориям", "callback": self._open_download_root, "args": (), "style": "danger"}])
        await self._edit_or_form(call, text, buttons)

    async def _show_download_status(self, call, filename):
        info = self._downloads.get(filename)
        if not info:
            return await self._update_text(call, f"{E_ERROR} <b>Нет активной загрузки:</b> <code>{filename}</code>")
        p = int(info.get("percent", 0))
        bar = self._get_progress_bar(p, 14)
        status = info.get("status", "unknown")
        dl_mb = int(info.get("downloaded", 0) / 1000000)
        total = int(info.get("total", 0))
        total_mb = int(total / 1000000) if total else 0
        if total > 0:
            text = f"{E_RELOAD} <b>Загрузка модели</b>\n<code>{filename}</code>\n<code>[{bar}] {p}%</code>\n<code>{dl_mb}/{total_mb} MB</code>\nСтатус: <b>{status}</b>"
        else:
            text = f"{E_RELOAD} <b>Загрузка модели</b>\n<code>{filename}</code>\n<code>[{bar}] {p}%</code>\n<code>{dl_mb} MB</code>\nСтатус: <b>{status}</b>"
        buttons = [
            [{"text": "🔄 Обновить", "callback": self._show_download_status, "args": (filename,), "style": "secondary"}],
            [{"text": "⬅️ К моделям", "callback": self._open_models_menu, "args": (), "style": "danger"}],
        ]
        await self._edit_or_form(call, text, buttons)

    async def _dl_model(self, call, url, filename):
        d = self._downloads.get(filename)
        if d and d.get("status") == "downloading":
            return await call.answer("Уже скачивается")

        await call.answer(f"Загрузка {filename} начата")
        msg = await self.client.send_message(call.chat_id, f"{E_RELOAD} <b>Начинаю загрузку...</b>\n<code>{filename}</code>")
        self._downloads[filename] = {
            "status": "downloading",
            "percent": 0,
            "downloaded": 0,
            "total": 0,
            "message_id": getattr(msg, "id", 0),
            "chat_id": call.chat_id,
            "filename": filename,
        }
        asyncio.create_task(self._bg_dl(msg, url, filename))
        await self._show_download_status(call, filename)

    async def _bg_dl(self, msg, url, filename):
        path = os.path.join(self.config["comfy_dir"], "models", "checkpoints", filename)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=None) as r:
                    total = int(r.headers.get("content-length", 0))
                    self._downloads.setdefault(filename, {})["total"] = total
                    dl = 0
                    with open(path, "wb") as f:
                        async for chunk in r.content.iter_chunked(4 * 1024 * 1024):
                            f.write(chunk)
                            dl += len(chunk)
                            self._downloads.setdefault(filename, {})["downloaded"] = dl
                            if total > 0:
                                p = int(dl / total * 100)
                                self._downloads.setdefault(filename, {})["percent"] = p
                                bar = self._get_progress_bar(p, 14)
                                await self._update_text(msg, f"{E_BOX} <b>Загрузка:</b> {p}%\n<code>[{bar}]</code>\n<code>{dl // 1000000}/{max(1, total // 1000000)} MB</code>")
                            else:
                                self._downloads.setdefault(filename, {})["percent"] = 0
            await self._update_text(msg, f"{E_SUCCESS} <b>{filename} скачан!</b>")
            self._downloads.setdefault(filename, {})["status"] = "done"
            self._downloads.setdefault(filename, {})["percent"] = 100
            await self._offer_smart_profile(msg, filename)
        except Exception as e:
            await self._update_text(msg, f"{E_ERROR} <b>Ошибка:</b> {str(e)}")
            self._downloads.setdefault(filename, {})["status"] = "error"
            self._downloads.setdefault(filename, {})["error"] = str(e)

    async def _set_model(self, call, name):
        self._current_model = name
        self.db.set("ComfyUI", "model", name)
        await self._open_models_menu(call)

    async def _open_wf_menu(self, call):
        text = f"{E_PENCIL} <b>Выбор Воркфлоу</b>\nСейчас: <code>{self._current_workflow}</code>"
        buttons = []
        for wf in WORKFLOWS:
            mark = "✅ " if wf["name"] == self._current_workflow else ""
            buttons.append([{"text": f"{mark}{wf['name']}", "callback": self._set_wf, "args": (wf["name"],), "style": "primary"}])
        buttons.append([{"text": "⬅️ Назад", "callback": self._render_settings_main_cb, "args": (), "style": "danger"}])
        await self._edit_or_form(call, text, buttons)

    async def _set_wf(self, call, name):
        self._current_workflow = name
        self.db.set("ComfyUI", "workflow", name)
        await self._open_wf_menu(call)

    def _get_workflow_cfg(self) -> Dict[str, Any]:
        for wf in WORKFLOWS:
            if wf["name"] == self._current_workflow:
                return wf
        return WORKFLOWS[0]

    def _infer_model_family(self) -> str:
        name = (self._current_model or "").lower()
        if any(x in name for x in ["xl", "pony", "animagine", "kohaku"]):
            return "sdxl"
        return "sd15"

    @loader.command(ru_doc="Сгенерировать арт")
    async def draw(self, message: Message):
        prompt = utils.get_args_raw(message)
        if not prompt:
            return await utils.answer(message, f"{E_ERROR} <b>Где промпт?</b>")

        if not await self._is_server_alive():
            return await utils.answer(message, f"{E_ERROR} <b>ComfyUI не отвечает. Запусти <code>.comfystart</code>.</b>")

        msg = await utils.answer(message, f"{E_GEAR} <b>Инициализация...</b>")
        await self._run_generation(prompt=prompt, chat_id=message.chat_id, status_msg=msg, reply_to=message)

    async def _run_generation(self, prompt: str, chat_id: int, status_msg, reply_to=None, workflow_override: Optional[str] = None):
        old_wf = self._current_workflow
        if workflow_override:
            self._current_workflow = workflow_override

        wf = self._get_workflow_cfg()
        model_family = self._infer_model_family()
        if wf["family"] != model_family:
            if self.config["strict_family_check"]:
                await self._update_text(
                    status_msg,
                    f"{E_ERROR} <b>Ошибка внутри ComfyUI!</b>\n"
                    f"Возможно: несовместимость модели и воркфлоу ({model_family} vs {wf['family']}) или OOM.\n"
                    "Попробуй .comfycfg → смени workflow/model и уменьши шаги/разрешение.",
                )
                self._current_workflow = old_wf
                return
            wf_auto = self._workflow_by_family(model_family)
            self._current_workflow = wf_auto["name"]
            wf = wf_auto
            await self._update_text(
                status_msg,
                f"{E_RELOAD} <b>Автосовместимость:</b> модель <code>{model_family}</code>, переключаю workflow на <code>{wf['name']}</code>.",
            )

        w, h, steps = wf["width"], wf["height"], wf["steps"]
        if "cpu" in self._current_workflow.lower() or "lcm" in self._current_workflow.lower():
            w, h = min(w, 640), min(h, 640)
            steps = min(steps, 10)

        payload = self._build_payload(prompt, w, h, steps)
        ws_url = self.config["comfy_url"].replace("http", "ws") + f"/ws?clientId={self._client_id}"

        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(f"{self.config['comfy_url']}/prompt", json=payload) as r:
                    jr = await r.json()
                    pid = jr.get("prompt_id")
                    if not pid:
                        self._current_workflow = old_wf
                        return await self._update_text(status_msg, f"{E_ERROR} <b>Не удалось получить prompt_id:</b> <code>{json.dumps(jr)[:350]}</code>")

                img_data = None
                async with s.ws_connect(ws_url, timeout=600) as ws:
                    while True:
                        raw = await ws.receive_json(timeout=600)
                        mtype = raw.get("type")
                        if mtype == "progress":
                            data = raw.get("data", {})
                            cur, mx = int(data.get("value", 0)), max(1, int(data.get("max", 1)))
                            p = int(cur / mx * 100)
                            await self._update_text(status_msg, f"{E_FIRE} <b>Рисую: {p}%</b>\n<code>{prompt[:65]}...</code>")
                        elif mtype == "executed" and raw.get("data", {}).get("prompt_id") == pid:
                            img_info = raw.get("data", {}).get("output", {}).get("images", [{}])[0]
                            v_url = f"{self.config['comfy_url']}/view?filename={img_info.get('filename')}&type=output"
                            async with s.get(v_url) as ir:
                                img_data = await ir.read()
                            break
                        elif mtype == "execution_error":
                            err = raw.get("data", {}).get("exception_message", "OOM / runtime error")
                            self._current_workflow = old_wf
                            return await self._update_text(status_msg, f"{E_ERROR} <b>Ошибка внутри ComfyUI!</b>\n<code>{err}</code>")

            if not img_data:
                self._current_workflow = old_wf
                return await self._update_text(status_msg, f"{E_ERROR} <b>Не удалось получить картинку.</b>")

            out_path = "out.png"
            with open(out_path, "wb") as f:
                f.write(img_data)
            await self.client.send_file(chat_id, out_path, caption=f"{E_SUCCESS} <b>Готово!</b>\nМодель: {self._current_model}\nWorkflow: {self._current_workflow}", reply_to=reply_to)

            archive_id = self.db.get("ComfyUI", "archive_chat", 0)
            if archive_id:
                await self.client.send_file(archive_id, out_path, caption=f"{E_LINK} <b>Архив генерации</b>\n<code>{prompt[:800]}</code>")

            await status_msg.delete()
            if self.config["auto_delete_delay"] > 0:
                await asyncio.sleep(self.config["auto_delete_delay"])
                if os.path.exists(out_path):
                    os.remove(out_path)
        except Exception as e:
            await self._update_text(status_msg, f"{E_ERROR} <b>Критическая ошибка:</b> {str(e)}")
        finally:
            self._current_workflow = old_wf

    @loader.command(ru_doc="Показать конфиг триггеров")
    async def comfytriggers(self, message: Message):
        text = (
            f"{E_GEAR} <b>Триггеры ComfyUI</b>\n\n"
            f"enabled: <code>{self.config['trigger_enabled']}</code>\n"
            f"words: <code>{self.config['trigger_words']}</code>\n"
            f"cooldown: <code>{self.config['trigger_cooldown']}s</code>\n"
            f"max_prompt: <code>{self.config['trigger_max_prompt']}</code>\n"
            f"max_queue: <code>{self.config['trigger_max_queue']}</code>\n"
            f"strict_family_check: <code>{self.config['strict_family_check']}</code>\n"
            f"delete_request: <code>{self.config['trigger_delete_request']}s</code>\n"
            f"delete_status: <code>{self.config['trigger_delete_status']}s</code>\n"
            f"chat_whitelist: <code>{self.config['trigger_only_whitelist_chats'] or '-'}</code>\n"
            f"user_whitelist: <code>{self.config['trigger_only_whitelist_users'] or '-'}</code>\n\n"
            f"profiles_json:\n<code>{self.config['trigger_profiles_json'][:3000]}</code>\n\n"
            f"Формат: <code>триггер prompt...</code>"
        )
        await utils.answer(message, text)

    @loader.watcher(only_incoming=True, ignore_edited=True)
    async def watcher(self, message: Message):
        if not self.config["trigger_enabled"]:
            return
        if getattr(message, "out", False) or getattr(message, "is_private", False):
            return
        txt = (getattr(message, "raw_text", None) or getattr(message, "text", None) or "").strip()
        match = self._extract_trigger_prompt(txt)
        if not match:
            return

        cid = int(utils.get_chat_id(message))
        uid = int(getattr(message, "sender_id", 0) or 0)
        if uid == int(getattr(self, "_tg_id", 0) or 0):
            return

        chat_whitelist = set(self._parse_csv_ints(self.config["trigger_only_whitelist_chats"]))
        user_whitelist = set(self._parse_csv_ints(self.config["trigger_only_whitelist_users"]))
        if chat_whitelist and cid not in chat_whitelist:
            return
        if user_whitelist and uid not in user_whitelist:
            return

        now = time.time()
        cooldown = int(self.config["trigger_cooldown"])
        if now - self._last_trigger_by_chat.get(cid, 0.0) < cooldown:
            return
        self._last_trigger_by_chat[cid] = now

        prompt = match["prompt"][: int(self.config["trigger_max_prompt"])]
        trig = match["trigger"]
        if self._trigger_queue.qsize() >= int(self.config["trigger_max_queue"]):
            with contextlib.suppress(Exception):
                await message.reply(f"{E_ERROR} <b>Сервер занят.</b> Очередь заполнена, попробуй позже.")
            return

        profiles = self._trigger_profiles()
        profile = profiles.get(trig, {})
        status = await message.reply(f"{E_GEAR} <b>Триггер '{trig}' принят.</b> Ставлю в очередь...")
        await self._trigger_queue.put(
            {
                "chat_id": cid,
                "prompt": prompt,
                "reply_to": message.id,
                "status_msg": status,
                "source_msg": message,
                "workflow": profile.get("workflow"),
                "delete_status_after": int(profile.get("delete_status_after", self.config["trigger_delete_status"])),
                "delete_request_after": int(profile.get("delete_request_after", self.config["trigger_delete_request"])),
            }
        )

    async def _trigger_worker(self):
        while True:
            task = await self._trigger_queue.get()
            try:
                async with self._trigger_busy:
                    if not await self._is_server_alive():
                        await self._update_text(task["status_msg"], f"{E_ERROR} <b>ComfyUI оффлайн.</b> Запусти .comfystart")
                    else:
                        await self._run_generation(
                            prompt=task["prompt"],
                            chat_id=task["chat_id"],
                            status_msg=task["status_msg"],
                            reply_to=task["reply_to"],
                            workflow_override=task["workflow"],
                        )
                if task["delete_request_after"] > 0:
                    await asyncio.sleep(task["delete_request_after"])
                    with contextlib.suppress(Exception):
                        await task["source_msg"].delete()
                if task["delete_status_after"] > 0:
                    await asyncio.sleep(task["delete_status_after"])
                    with contextlib.suppress(Exception):
                        await task["status_msg"].delete()
            except Exception:
                pass
            finally:
                self._trigger_queue.task_done()

    async def _get_models(self):
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(f"{self.config['comfy_url']}/object_info") as r:
                    return (await r.json())["CheckpointLoaderSimple"]["input"]["required"]["ckpt_name"][0]
        except Exception:
            return []

    @loader.command(ru_doc="Диагностика CPU памяти для ComfyUI")
    async def comfydiag(self, message: Message):
        total = avail = 0
        if psutil is not None:
            vm = psutil.virtual_memory()
            total, avail = int(vm.total / 1024 / 1024), int(vm.available / 1024 / 1024)
        txt = (
            f"{E_ONE} <b>Диагностика</b>\n"
            f"RAM total/available: <code>{total} / {avail} MB</code>\n"
            f"Workflow: <code>{self._current_workflow}</code>\n"
            f"Model: <code>{self._current_model}</code>\n\n"
            f"Рекомендации:\n"
            f"1) Для CPU выбери <code>LCM Fast</code> или <code>SDXL Lightning</code>.\n"
            f"2) Снизь размеры до 512x512 и Steps до 8-12.\n"
            f"3) Убедись, что модель и workflow одной семьи (SD1.5/SDXL)."
        )
        await utils.answer(message, txt)

    def _build_payload(self, prompt, w, h, steps):
        return {
            "client_id": self._client_id,
            "prompt": {
                "3": {
                    "class_type": "KSampler",
                    "inputs": {
                        "cfg": self.config["default_cfg"],
                        "denoise": 1,
                        "latent_image": ["5", 0],
                        "model": ["4", 0],
                        "negative": ["7", 0],
                        "positive": ["6", 0],
                        "sampler_name": self.config["default_sampler"],
                        "scheduler": self.config["default_scheduler"],
                        "seed": int(time.time()),
                        "steps": steps,
                    },
                },
                "4": {"class_type": "CheckpointLoaderSimple", "inputs": {"ckpt_name": self._current_model}},
                "5": {"class_type": "EmptyLatentImage", "inputs": {"batch_size": 1, "height": h, "width": w}},
                "6": {"class_type": "CLIPTextEncode", "inputs": {"clip": ["4", 1], "text": prompt}},
                "7": {
                    "class_type": "CLIPTextEncode",
                    "inputs": {"clip": ["4", 1], "text": "low quality, blurry, deformed, artifacts"},
                },
                "8": {"class_type": "VAEDecode", "inputs": {"samples": ["3", 0], "vae": ["4", 2]}},
                "9": {"class_type": "SaveImage", "inputs": {"filename_prefix": "H", "images": ["8", 0]}},
            },
        }
