#!/usr/bin/env python3
"""GitHub module publisher bot for Telegram channels (aiogram 3)."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("module_publisher")


META_DESCRIPTION = re.compile(r"^\s*#\s*Description\s*:\s*(.+)$", re.I | re.M)
META_BANNER = re.compile(r"^\s*#\s*meta banner\s*:\s*(\S+)\s*$", re.I | re.M)
COMMAND_RE = re.compile(r"async\s+def\s+([a-zA-Z0-9_]+)cmd\s*\(")
VERSION_RE = re.compile(r"_module_version\s*=\s*[\"']([\w\-.]+)[\"']")
DEV_RE = re.compile(r"^\s*#\s*meta developer\s*:\s*(.+)$", re.I | re.M)


@dataclass
class TrackedModule:
    path: str
    sha: str
    version: str = "unknown"


@dataclass
class Settings:
    owner_id: int
    github_repo: str
    github_token: str = ""
    allowed_pm_users: list[int] = field(default_factory=list)
    bot_admins: list[int] = field(default_factory=list)
    publish_channels: list[int] = field(default_factory=list)
    include_patterns: list[str] = field(default_factory=lambda: ["*.py"])
    exclude_patterns: list[str] = field(default_factory=lambda: ["*__init__.py"])
    poll_interval_sec: int = 180


@dataclass
class State:
    modules: dict[str, TrackedModule] = field(default_factory=dict)


class Storage:
    def __init__(self, settings_file: Path, state_file: Path):
        self.settings_file = settings_file
        self.state_file = state_file

    def load_settings(self) -> Settings:
        if not self.settings_file.exists():
            raise FileNotFoundError(
                f"Create config first: {self.settings_file}. See module_publisher_config.example.json"
            )
        data = json.loads(self.settings_file.read_text("utf-8"))
        return Settings(**data)

    def load_state(self) -> State:
        if not self.state_file.exists():
            return State()
        data = json.loads(self.state_file.read_text("utf-8"))
        mods = {
            k: TrackedModule(**v)
            for k, v in data.get("modules", {}).items()
        }
        return State(modules=mods)

    def save_state(self, state: State) -> None:
        payload = {"modules": {k: asdict(v) for k, v in state.modules.items()}}
        self.state_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), "utf-8")


class GitHubClient:
    def __init__(self, repo: str, token: str = ""):
        self.repo = repo
        self.token = token
        self.base = f"https://api.github.com/repos/{repo}"

    async def _request(self, session: aiohttp.ClientSession, path: str) -> Any:
        headers = {"Accept": "application/vnd.github+json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        url = f"{self.base}{path}"
        async with session.get(url, headers=headers) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"GitHub API error {resp.status}: {text[:300]}")
            return json.loads(text)

    async def get_tree(self, session: aiohttp.ClientSession) -> list[dict[str, Any]]:
        head = await self._request(session, "/commits?per_page=1")
        sha = head[0]["sha"]
        tree = await self._request(session, f"/git/trees/{sha}?recursive=1")
        return tree.get("tree", [])

    async def get_file(self, session: aiohttp.ClientSession, path: str) -> dict[str, Any]:
        return await self._request(session, f"/contents/{path}")

    async def latest_commit_for_file(self, session: aiohttp.ClientSession, path: str) -> dict[str, Any] | None:
        data = await self._request(session, f"/commits?path={path}&per_page=1")
        return data[0] if data else None

    def raw_url(self, path: str) -> str:
        return f"https://raw.githubusercontent.com/{self.repo}/main/{path}"


class ModulePublisherBot:
    def __init__(self, bot: Bot, settings: Settings, storage: Storage):
        self.bot = bot
        self.settings = settings
        self.storage = storage
        self.state = storage.load_state()
        self.gh = GitHubClient(settings.github_repo, settings.github_token)
        self.router = Router(name="module-publisher")
        self.pending: dict[str, dict[str, Any]] = {}
        self._register_handlers()

    def _is_admin(self, user_id: int) -> bool:
        return user_id == self.settings.owner_id or user_id in self.settings.bot_admins

    def _pm_allowed(self, user_id: int) -> bool:
        return user_id == self.settings.owner_id or user_id in self.settings.allowed_pm_users

    def _register_handlers(self) -> None:
        @self.router.message(Command("start"))
        async def start_handler(message: Message) -> None:
            if not self._pm_allowed(message.from_user.id):
                await message.answer("⛔️ Доступ запрещён")
                return
            await message.answer("✅ Bot online. /help")

        @self.router.message(Command("help"))
        async def help_handler(message: Message) -> None:
            if not self._pm_allowed(message.from_user.id):
                return
            await message.answer(
                "/channels — список\n"
                "/addchannel <id>\n"
                "/rmchannel <id>\n"
                "/admins\n"
                "/addadmin <id>\n"
                "/rmadmin <id>\n"
                "/pmallow <id>\n"
                "/pmdel <id>\n"
                "/scan — ручная проверка GitHub"
            )

        @self.router.message(Command("channels"))
        async def channels_handler(message: Message) -> None:
            if not self._is_admin(message.from_user.id):
                return
            txt = "\n".join(str(x) for x in self.settings.publish_channels) or "(пусто)"
            await message.answer(f"📢 Каналы:\n{txt}")

        @self.router.message(Command("addchannel"))
        async def add_channel(message: Message, command: CommandObject) -> None:
            if not self._is_admin(message.from_user.id):
                return
            cid = int((command.args or "0").strip())
            if cid not in self.settings.publish_channels:
                self.settings.publish_channels.append(cid)
                self._save_settings()
            await message.answer(f"✅ Добавлен канал {cid}")

        @self.router.message(Command("rmchannel"))
        async def rm_channel(message: Message, command: CommandObject) -> None:
            if not self._is_admin(message.from_user.id):
                return
            cid = int((command.args or "0").strip())
            self.settings.publish_channels = [x for x in self.settings.publish_channels if x != cid]
            self._save_settings()
            await message.answer(f"✅ Удален канал {cid}")

        @self.router.message(Command("admins"))
        async def admins_list(message: Message) -> None:
            if not self._is_admin(message.from_user.id):
                return
            txt = "\n".join(str(x) for x in self.settings.bot_admins) or "(пусто)"
            await message.answer(f"👮 Админы:\n{txt}")

        @self.router.message(Command("addadmin"))
        async def add_admin(message: Message, command: CommandObject) -> None:
            if message.from_user.id != self.settings.owner_id:
                return
            uid = int((command.args or "0").strip())
            if uid not in self.settings.bot_admins:
                self.settings.bot_admins.append(uid)
                self._save_settings()
            await message.answer(f"✅ Админ добавлен {uid}")

        @self.router.message(Command("rmadmin"))
        async def rm_admin(message: Message, command: CommandObject) -> None:
            if message.from_user.id != self.settings.owner_id:
                return
            uid = int((command.args or "0").strip())
            self.settings.bot_admins = [x for x in self.settings.bot_admins if x != uid]
            self._save_settings()
            await message.answer(f"✅ Админ удален {uid}")

        @self.router.message(Command("pmallow"))
        async def pm_allow(message: Message, command: CommandObject) -> None:
            if message.from_user.id != self.settings.owner_id:
                return
            uid = int((command.args or "0").strip())
            if uid not in self.settings.allowed_pm_users:
                self.settings.allowed_pm_users.append(uid)
                self._save_settings()
            await message.answer(f"✅ PM доступ выдан {uid}")

        @self.router.message(Command("pmdel"))
        async def pm_del(message: Message, command: CommandObject) -> None:
            if message.from_user.id != self.settings.owner_id:
                return
            uid = int((command.args or "0").strip())
            self.settings.allowed_pm_users = [x for x in self.settings.allowed_pm_users if x != uid]
            self._save_settings()
            await message.answer(f"✅ PM доступ снят {uid}")

        @self.router.message(Command("scan"))
        async def scan_now(message: Message) -> None:
            if not self._is_admin(message.from_user.id):
                return
            await self.scan_and_notify(force=True)
            await message.answer("🔎 Проверка выполнена")

        @self.router.callback_query(F.data.startswith("publish:"))
        async def publish_cb(call: CallbackQuery) -> None:
            if call.from_user.id != self.settings.owner_id:
                await call.answer("Только owner", show_alert=True)
                return
            key = call.data.split(":", 1)[1]
            data = self.pending.get(key)
            if not data:
                await call.answer("Устарело")
                return
            await self.publish_update(data)
            await call.message.edit_reply_markup(reply_markup=None)
            await call.answer("Опубликовано")

        @self.router.callback_query(F.data.startswith("skip:"))
        async def skip_cb(call: CallbackQuery) -> None:
            if call.from_user.id != self.settings.owner_id:
                await call.answer("Только owner", show_alert=True)
                return
            await call.message.edit_reply_markup(reply_markup=None)
            await call.answer("Пропущено")

    def _save_settings(self) -> None:
        settings_path = self.storage.settings_file
        settings_path.write_text(json.dumps(asdict(self.settings), ensure_ascii=False, indent=2), "utf-8")

    async def bootstrap(self) -> None:
        if self.state.modules:
            return
        async with aiohttp.ClientSession() as session:
            tree = await self.gh.get_tree(session)
            for item in tree:
                if item.get("type") != "blob":
                    continue
                path = item.get("path", "")
                if not self._should_track(path):
                    continue
                details = await self.gh.get_file(session, path)
                content = self._decode_b64(details.get("content", ""))
                version = self._extract_version(content)
                self.state.modules[path] = TrackedModule(path=path, sha=item["sha"], version=version)
            self.storage.save_state(self.state)
            logger.info("Bootstrap done, tracked=%s", len(self.state.modules))

    def _should_track(self, path: str) -> bool:
        if not path.endswith(".py"):
            return False
        if path.startswith("."):
            return False
        if path.startswith("venv/"):
            return False
        return True

    @staticmethod
    def _decode_b64(text: str) -> str:
        import base64

        return base64.b64decode(text).decode("utf-8", errors="ignore") if text else ""

    def _extract_version(self, source: str) -> str:
        m = VERSION_RE.search(source)
        return m.group(1) if m else "unknown"

    def _parse_module(self, source: str, path: str) -> dict[str, Any]:
        description = (META_DESCRIPTION.search(source).group(1).strip() if META_DESCRIPTION.search(source) else "No description")
        banner = META_BANNER.search(source).group(1).strip() if META_BANNER.search(source) else ""
        developer = DEV_RE.search(source).group(1).strip() if DEV_RE.search(source) else "unknown"
        commands = sorted({f".{m.group(1)}" for m in COMMAND_RE.finditer(source)})
        version = self._extract_version(source)
        module_name = Path(path).stem
        return {
            "name": module_name,
            "version": version,
            "description": description,
            "banner": banner,
            "developer": developer,
            "commands": commands,
        }

    def _build_text(self, kind: str, old_v: str, new_v: str, m: dict[str, Any], commit_url: str) -> str:
        title_map = {
            "added": f"🆕 Module {m['name']} (v{new_v}) Added!",
            "updated": f"🆙 Module {m['name']} (v{old_v} -> v{new_v}) Updated!",
            "deleted": f"🗑️ Module {m['name']} (v{old_v}) Deleted!",
        }
        commands = "\n".join(f"• <code>{c}</code>" for c in m.get("commands", [])) or "• <i>No commands found</i>"
        top = f"<blockquote expandable><b>{title_map[kind]}</b></blockquote>"
        desc = f"📝 <b>Description:</b>\n{m.get('description', '—')}"
        cmd = f"\n\n⚙️ <b>Commands:</b>\n{commands}"
        commit = f"\n\n🔗 <a href=\"{commit_url}\">Open commit on GitHub</a>"
        footer = f"\n👨‍💻 <b>Developer:</b> {m.get('developer','unknown')}"
        return top + "\n\n" + desc + cmd + commit + footer

    async def scan_and_notify(self, force: bool = False) -> None:
        async with aiohttp.ClientSession() as session:
            tree = await self.gh.get_tree(session)
            current = {i["path"]: i for i in tree if i.get("type") == "blob" and self._should_track(i.get("path", ""))}

            tracked_paths = set(self.state.modules.keys())
            current_paths = set(current.keys())

            added = sorted(current_paths - tracked_paths)
            deleted = sorted(tracked_paths - current_paths)
            maybe_updated = sorted(current_paths & tracked_paths)

            updates: list[dict[str, Any]] = []

            for path in added:
                info = await self.gh.get_file(session, path)
                source = self._decode_b64(info.get("content", ""))
                meta = self._parse_module(source, path)
                commit = await self.gh.latest_commit_for_file(session, path)
                updates.append({
                    "kind": "added",
                    "path": path,
                    "old_v": "-",
                    "new_v": meta["version"],
                    "meta": meta,
                    "sha": current[path]["sha"],
                    "commit_url": commit.get("html_url") if commit else self.gh.raw_url(path),
                })

            for path in maybe_updated:
                old = self.state.modules[path]
                new_sha = current[path]["sha"]
                if old.sha == new_sha and not force:
                    continue
                info = await self.gh.get_file(session, path)
                source = self._decode_b64(info.get("content", ""))
                meta = self._parse_module(source, path)
                commit = await self.gh.latest_commit_for_file(session, path)
                updates.append({
                    "kind": "updated",
                    "path": path,
                    "old_v": old.version,
                    "new_v": meta["version"],
                    "meta": meta,
                    "sha": new_sha,
                    "commit_url": commit.get("html_url") if commit else self.gh.raw_url(path),
                })

            for path in deleted:
                old = self.state.modules[path]
                commit = await self.gh.latest_commit_for_file(session, path)
                updates.append({
                    "kind": "deleted",
                    "path": path,
                    "old_v": old.version,
                    "new_v": "-",
                    "meta": {"name": Path(path).stem, "description": "Module removed", "commands": []},
                    "sha": "",
                    "commit_url": commit.get("html_url") if commit else f"https://github.com/{self.settings.github_repo}",
                })

            updates.sort(key=lambda x: x["path"])
            for upd in updates:
                await self._notify_owner(upd)
                self._update_state(upd)

            if updates:
                self.storage.save_state(self.state)

    def _update_state(self, upd: dict[str, Any]) -> None:
        kind = upd["kind"]
        path = upd["path"]
        if kind == "deleted":
            self.state.modules.pop(path, None)
            return
        self.state.modules[path] = TrackedModule(path=path, sha=upd["sha"], version=upd["new_v"])

    async def _notify_owner(self, upd: dict[str, Any]) -> None:
        key = f"{upd['path']}:{upd.get('sha','x')}:{int(datetime.now(tz=timezone.utc).timestamp())}"
        self.pending[key] = upd

        msg = self._build_text(upd["kind"], upd["old_v"], upd["new_v"], upd["meta"], upd["commit_url"])
        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Залить в каналы", callback_data=f"publish:{key}")
        kb.button(text="⏭ Пропустить", callback_data=f"skip:{key}")
        kb.adjust(2)

        await self.bot.send_message(
            self.settings.owner_id,
            msg,
            reply_markup=kb.as_markup(),
            disable_web_page_preview=False,
        )

    async def publish_update(self, upd: dict[str, Any]) -> None:
        text = self._build_text(upd["kind"], upd["old_v"], upd["new_v"], upd["meta"], upd["commit_url"])
        install_kb = InlineKeyboardBuilder()
        raw = self.gh.raw_url(upd["path"])
        install_kb.button(text="📦 Install Module", url=raw)
        install_kb.button(text="ℹ️ Как установить", url="https://github.com/hikariatama/Hikka/wiki")
        install_kb.adjust(1)

        for channel_id in self.settings.publish_channels:
            try:
                banner = upd["meta"].get("banner")
                if banner and upd["kind"] != "deleted":
                    await self.bot.send_photo(channel_id, banner, caption=text, reply_markup=install_kb.as_markup())
                else:
                    await self.bot.send_message(channel_id, text, reply_markup=install_kb.as_markup(), disable_web_page_preview=False)
            except Exception as e:
                logger.exception("Publish failed for %s: %s", channel_id, e)

    async def worker(self) -> None:
        await self.bootstrap()
        while True:
            try:
                await self.scan_and_notify()
            except Exception as e:
                logger.exception("scan error: %s", e)
            await asyncio.sleep(max(60, self.settings.poll_interval_sec))


async def main() -> None:
    settings_file = Path(os.getenv("SETTINGS_FILE", "module_publisher_config.json"))
    state_file = Path(os.getenv("STATE_FILE", "module_publisher_state.json"))
    storage = Storage(settings_file, state_file)
    settings = storage.load_settings()

    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is required")

    bot = Bot(token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()

    app = ModulePublisherBot(bot, settings, storage)
    dp.include_router(app.router)

    asyncio.create_task(app.worker())
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())
