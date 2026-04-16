#!/usr/bin/env python3
"""Monolithic GitHub module publisher bot (aiogram 3)."""

from __future__ import annotations

import asyncio
import base64
import fnmatch
import json
import logging
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

# =========================
# MONOLITHIC CONFIG (EDIT HERE)
# =========================
BOT_TOKEN = "PUT_YOUR_BOT_TOKEN_HERE"
OWNER_ID = 8304142242
GITHUB_REPO = "sepiol026-wq/goypulse"
GITHUB_TOKEN = ""
POLL_INTERVAL_SEC = 180
TRACK_INCLUDE_PATTERNS = ["*.py"]
TRACK_EXCLUDE_PATTERNS = ["*__init__.py", "venv/*", ".*", "assets/*"]
STATE_PATH = Path("module_publisher_state.json")

# Lists are persisted in state json and can be managed via inline UI.
DEFAULT_CHANNELS: list[int] = []
DEFAULT_BOT_ADMINS: list[int] = []
DEFAULT_ALLOWED_PM_USERS: list[int] = []

# =========================
# PARSERS
# =========================
META_DESCRIPTION = re.compile(r"^\s*#\s*Description\s*:\s*(.+)$", re.I | re.M)
META_BANNER = re.compile(r"^\s*#\s*meta banner\s*:\s*(\S+)\s*$", re.I | re.M)
DEV_RE = re.compile(r"^\s*#\s*meta developer\s*:\s*(.+)$", re.I | re.M)
CMD_RE = re.compile(r"async\s+def\s+([a-zA-Z0-9_]+)cmd\s*\(")
VERSION_RE = re.compile(r"_module_version\s*=\s*[\"']([\w\-.]+)[\"']")
STRINGS_NAME_RE = re.compile(r"strings\s*=\s*\{[\s\S]{0,3000}?['\"]name['\"]\s*:\s*['\"]([^'\"]+)['\"]", re.M)
DOCSTRING_RE = re.compile(r"class\s+\w+\([^)]*\):\s*\n\s*[ruRU]*([\"\']{3})([\s\S]{1,2000}?)\1", re.M)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("module_publisher")


@dataclass
class TrackedModule:
    path: str
    sha: str
    version: str = "unknown"


@dataclass
class RuntimeState:
    modules: dict[str, TrackedModule] = field(default_factory=dict)
    publish_channels: list[int] = field(default_factory=lambda: DEFAULT_CHANNELS.copy())
    bot_admins: list[int] = field(default_factory=lambda: DEFAULT_BOT_ADMINS.copy())
    allowed_pm_users: list[int] = field(default_factory=lambda: DEFAULT_ALLOWED_PM_USERS.copy())


class Storage:
    def load(self) -> RuntimeState:
        if not STATE_PATH.exists():
            return RuntimeState()
        data = json.loads(STATE_PATH.read_text("utf-8"))
        modules = {k: TrackedModule(**v) for k, v in data.get("modules", {}).items()}
        return RuntimeState(
            modules=modules,
            publish_channels=data.get("publish_channels", DEFAULT_CHANNELS.copy()),
            bot_admins=data.get("bot_admins", DEFAULT_BOT_ADMINS.copy()),
            allowed_pm_users=data.get("allowed_pm_users", DEFAULT_ALLOWED_PM_USERS.copy()),
        )

    def save(self, state: RuntimeState) -> None:
        payload = {
            "modules": {k: asdict(v) for k, v in state.modules.items()},
            "publish_channels": state.publish_channels,
            "bot_admins": state.bot_admins,
            "allowed_pm_users": state.allowed_pm_users,
        }
        STATE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), "utf-8")


class GitHubClient:
    def __init__(self):
        self.base = f"https://api.github.com/repos/{GITHUB_REPO}"

    async def _request(self, session: aiohttp.ClientSession, path: str) -> Any:
        headers = {"Accept": "application/vnd.github+json"}
        if GITHUB_TOKEN:
            headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
        async with session.get(f"{self.base}{path}", headers=headers) as resp:
            raw = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"GitHub API {resp.status}: {raw[:300]}")
            return json.loads(raw)

    async def get_tree(self, session: aiohttp.ClientSession) -> list[dict[str, Any]]:
        head = await self._request(session, "/commits?per_page=1")
        sha = head[0]["sha"]
        tree = await self._request(session, f"/git/trees/{sha}?recursive=1")
        return tree.get("tree", [])

    async def get_file(self, session: aiohttp.ClientSession, path: str) -> dict[str, Any]:
        return await self._request(session, f"/contents/{path}")

    async def latest_commit_for_file(self, session: aiohttp.ClientSession, path: str) -> dict[str, Any] | None:
        commits = await self._request(session, f"/commits?path={path}&per_page=1")
        return commits[0] if commits else None

    @staticmethod
    def raw_url(path: str) -> str:
        return f"https://raw.githubusercontent.com/{GITHUB_REPO}/main/{path}"


class App:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.dp = Dispatcher()
        self.router = Router(name="main")
        self.dp.include_router(self.router)

        self.storage = Storage()
        self.state = self.storage.load()
        self.gh = GitHubClient()

        self.pending_updates: dict[str, dict[str, Any]] = {}
        self.await_input: dict[int, str] = {}
        self.register_handlers()

    # ---------- Permissions ----------
    def is_owner(self, user_id: int) -> bool:
        return user_id == OWNER_ID

    def is_admin(self, user_id: int) -> bool:
        return self.is_owner(user_id) or user_id in self.state.bot_admins

    def pm_allowed(self, user_id: int) -> bool:
        return self.is_owner(user_id) or user_id in self.state.allowed_pm_users

    # ---------- UI ----------
    def kb_main(self) -> InlineKeyboardBuilder:
        kb = InlineKeyboardBuilder()
        kb.button(text="🔎 Scan now", callback_data="ui:scan")
        kb.button(text="📢 Channels", callback_data="ui:channels")
        kb.button(text="👮 Admins", callback_data="ui:admins")
        kb.button(text="💬 PM Access", callback_data="ui:pm")
        kb.button(text="📊 Status", callback_data="ui:status")
        kb.adjust(2, 2, 1)
        return kb

    @staticmethod
    def _list_to_text(values: list[int]) -> str:
        return "\n".join(f"• <code>{v}</code>" for v in values) if values else "<i>(empty)</i>"

    def kb_entity(self, kind: str) -> InlineKeyboardBuilder:
        kb = InlineKeyboardBuilder()
        kb.button(text="➕ Add", callback_data=f"ui:add:{kind}")
        kb.button(text="➖ Remove", callback_data=f"ui:rm:{kind}")
        kb.button(text="⬅️ Back", callback_data="ui:main")
        kb.adjust(2, 1)
        return kb

    async def render_main(self, target: Message | CallbackQuery, edit: bool = False) -> None:
        text = (
            "<b>Module Publisher Panel</b>\n"
            f"Repo: <code>{GITHUB_REPO}</code>\n"
            f"Channels: <code>{len(self.state.publish_channels)}</code> | "
            f"Admins: <code>{len(self.state.bot_admins)}</code> | "
            f"PM: <code>{len(self.state.allowed_pm_users)}</code>"
        )
        markup = self.kb_main().as_markup()
        if isinstance(target, CallbackQuery):
            if edit:
                await target.message.edit_text(text, reply_markup=markup)
            else:
                await target.message.answer(text, reply_markup=markup)
            await target.answer("OK")
        else:
            await target.answer(text, reply_markup=markup)

    # ---------- Handlers ----------
    def register_handlers(self) -> None:
        @self.router.message(Command("start"))
        async def start_cmd(message: Message) -> None:
            uid = message.from_user.id
            if not self.pm_allowed(uid):
                await message.answer("⛔️ access denied")
                return
            await self.render_main(message)

        @self.router.message(Command("panel"))
        async def panel_cmd(message: Message) -> None:
            uid = message.from_user.id
            if not self.pm_allowed(uid):
                return
            await self.render_main(message)

        @self.router.callback_query(F.data == "ui:main")
        async def ui_main(call: CallbackQuery) -> None:
            if not self.pm_allowed(call.from_user.id):
                await call.answer("Denied", show_alert=True)
                return
            await self.render_main(call, edit=True)

        @self.router.callback_query(F.data == "ui:status")
        async def ui_status(call: CallbackQuery) -> None:
            if not self.pm_allowed(call.from_user.id):
                await call.answer("Denied", show_alert=True)
                return
            txt = (
                "<b>Status</b>\n"
                f"Tracked modules: <code>{len(self.state.modules)}</code>\n"
                f"Poll interval: <code>{POLL_INTERVAL_SEC}s</code>"
            )
            kb = InlineKeyboardBuilder()
            kb.button(text="⬅️ Back", callback_data="ui:main")
            await call.message.edit_text(txt, reply_markup=kb.as_markup())
            await call.answer("Updated")

        @self.router.callback_query(F.data == "ui:channels")
        async def ui_channels(call: CallbackQuery) -> None:
            if not self.is_admin(call.from_user.id):
                await call.answer("Admin only", show_alert=True)
                return
            txt = "<b>Channels</b>\n" + self._list_to_text(self.state.publish_channels)
            await call.message.edit_text(txt, reply_markup=self.kb_entity("channels").as_markup())
            await call.answer("Channels")

        @self.router.callback_query(F.data == "ui:admins")
        async def ui_admins(call: CallbackQuery) -> None:
            if not self.is_owner(call.from_user.id):
                await call.answer("Owner only", show_alert=True)
                return
            txt = "<b>Bot admins</b>\n" + self._list_to_text(self.state.bot_admins)
            await call.message.edit_text(txt, reply_markup=self.kb_entity("admins").as_markup())
            await call.answer("Admins")

        @self.router.callback_query(F.data == "ui:pm")
        async def ui_pm(call: CallbackQuery) -> None:
            if not self.is_owner(call.from_user.id):
                await call.answer("Owner only", show_alert=True)
                return
            txt = "<b>PM allowed users</b>\n" + self._list_to_text(self.state.allowed_pm_users)
            await call.message.edit_text(txt, reply_markup=self.kb_entity("pm").as_markup())
            await call.answer("PM access")

        @self.router.callback_query(F.data.startswith("ui:add:"))
        async def ui_add(call: CallbackQuery) -> None:
            uid = call.from_user.id
            kind = call.data.split(":", 2)[2]
            if kind == "channels" and not self.is_admin(uid):
                await call.answer("Admin only", show_alert=True)
                return
            if kind in {"admins", "pm"} and not self.is_owner(uid):
                await call.answer("Owner only", show_alert=True)
                return

            self.await_input[uid] = f"add:{kind}"
            await call.answer("Жду ID")
            await call.message.answer(f"Введите ID для <b>add {kind}</b> одним числом.")

        @self.router.callback_query(F.data.startswith("ui:rm:"))
        async def ui_rm(call: CallbackQuery) -> None:
            uid = call.from_user.id
            kind = call.data.split(":", 2)[2]
            if kind == "channels" and not self.is_admin(uid):
                await call.answer("Admin only", show_alert=True)
                return
            if kind in {"admins", "pm"} and not self.is_owner(uid):
                await call.answer("Owner only", show_alert=True)
                return

            self.await_input[uid] = f"rm:{kind}"
            await call.answer("Жду ID")
            await call.message.answer(f"Введите ID для <b>remove {kind}</b> одним числом.")

        @self.router.callback_query(F.data == "ui:scan")
        async def ui_scan(call: CallbackQuery) -> None:
            if not self.is_admin(call.from_user.id):
                await call.answer("Admin only", show_alert=True)
                return
            await call.answer("Scanning...", show_alert=False)
            await self.scan_and_notify(force=True)
            await call.message.answer("✅ Scan done")

        @self.router.message()
        async def catch_input(message: Message) -> None:
            uid = message.from_user.id
            if uid not in self.await_input:
                return
            op = self.await_input.pop(uid)
            try:
                target_id = int((message.text or "").strip())
            except Exception:
                await message.answer("❌ Нужен числовой ID")
                return

            action, kind = op.split(":", 1)
            arr = self.state.publish_channels if kind == "channels" else self.state.bot_admins if kind == "admins" else self.state.allowed_pm_users

            if action == "add":
                if target_id not in arr:
                    arr.append(target_id)
            else:
                arr[:] = [x for x in arr if x != target_id]

            self.storage.save(self.state)
            await message.answer("✅ Saved. /panel")

        @self.router.callback_query(F.data.startswith("publish:"))
        async def publish(call: CallbackQuery) -> None:
            if not self.is_owner(call.from_user.id):
                await call.answer("Owner only", show_alert=True)
                return
            key = call.data.split(":", 1)[1]
            upd = self.pending_updates.get(key)
            if not upd:
                await call.answer("Expired", show_alert=True)
                return
            await self.publish_update(upd)
            await call.message.edit_reply_markup(reply_markup=None)
            await call.answer("Published")

        @self.router.callback_query(F.data.startswith("skip:"))
        async def skip(call: CallbackQuery) -> None:
            if not self.is_owner(call.from_user.id):
                await call.answer("Owner only", show_alert=True)
                return
            await call.message.edit_reply_markup(reply_markup=None)
            await call.answer("Skipped")

    # ---------- Scanner ----------
    @staticmethod
    def should_track(path: str) -> bool:
        inc = any(fnmatch.fnmatch(path, p) for p in TRACK_INCLUDE_PATTERNS)
        exc = any(fnmatch.fnmatch(path, p) for p in TRACK_EXCLUDE_PATTERNS)
        return inc and not exc

    @staticmethod
    def decode_b64(payload: str) -> str:
        return base64.b64decode(payload).decode("utf-8", errors="ignore") if payload else ""

    @staticmethod
    def extract_version(source: str) -> str:
        m = VERSION_RE.search(source)
        return m.group(1) if m else "unknown"

    @staticmethod
    def extract_docstring(source: str) -> str:
        m = DOCSTRING_RE.search(source)
        if not m:
            return ""
        return " ".join(x.strip() for x in m.group(2).strip().splitlines() if x.strip())

    def parse_module_meta(self, source: str, path: str) -> dict[str, Any]:
        name_m = STRINGS_NAME_RE.search(source)
        name = name_m.group(1).strip() if name_m else Path(path).stem

        desc = ""
        meta_desc = META_DESCRIPTION.search(source)
        if meta_desc:
            desc = meta_desc.group(1).strip()
        if not desc:
            desc = self.extract_docstring(source)
        if not desc:
            desc = f"Module: {name}"

        banner = META_BANNER.search(source).group(1).strip() if META_BANNER.search(source) else ""
        developer = DEV_RE.search(source).group(1).strip() if DEV_RE.search(source) else "unknown"
        cmds = sorted({f".{m.group(1)}" for m in CMD_RE.finditer(source)})

        return {
            "name": name,
            "description": desc,
            "banner": banner,
            "developer": developer,
            "version": self.extract_version(source),
            "commands": cmds,
        }

    @staticmethod
    def build_text(kind: str, old_v: str, new_v: str, meta: dict[str, Any], commit_url: str) -> str:
        title = {
            "added": f"🆕 Module {meta['name']} (v{new_v}) Added!",
            "updated": f"🆙 Module {meta['name']} (v{old_v} -> v{new_v}) Updated!",
            "deleted": f"🗑️ Module {meta['name']} (v{old_v}) Deleted!",
        }[kind]
        commands = "\n".join(f"• <code>{c}</code>" for c in meta.get("commands", [])) or "• <i>No commands</i>"
        return (
            f"<blockquote expandable><b>{title}</b></blockquote>\n\n"
            f"📝 <b>Description:</b>\n{meta.get('description', '—')}\n\n"
            f"⚙️ <b>Commands:</b>\n{commands}\n\n"
            f"🔗 <a href=\"{commit_url}\">Open commit on GitHub</a>\n"
            f"👨‍💻 <b>Developer:</b> {meta.get('developer', 'unknown')}"
        )

    async def bootstrap(self) -> None:
        if self.state.modules:
            return
        async with aiohttp.ClientSession() as session:
            tree = await self.gh.get_tree(session)
            for item in tree:
                if item.get("type") != "blob":
                    continue
                path = item.get("path", "")
                if not self.should_track(path):
                    continue
                info = await self.gh.get_file(session, path)
                src = self.decode_b64(info.get("content", ""))
                self.state.modules[path] = TrackedModule(path=path, sha=item["sha"], version=self.extract_version(src))
            self.storage.save(self.state)
            logger.info("bootstrap tracked=%s", len(self.state.modules))

    async def scan_and_notify(self, force: bool = False) -> None:
        async with aiohttp.ClientSession() as session:
            tree = await self.gh.get_tree(session)
            current = {i["path"]: i for i in tree if i.get("type") == "blob" and self.should_track(i.get("path", ""))}

            old_set, new_set = set(self.state.modules.keys()), set(current.keys())
            added, deleted, common = sorted(new_set - old_set), sorted(old_set - new_set), sorted(old_set & new_set)

            updates: list[dict[str, Any]] = []

            for path in added:
                info = await self.gh.get_file(session, path)
                src = self.decode_b64(info.get("content", ""))
                meta = self.parse_module_meta(src, path)
                commit = await self.gh.latest_commit_for_file(session, path)
                updates.append({
                    "kind": "added", "path": path, "old_v": "-", "new_v": meta["version"],
                    "sha": current[path]["sha"], "meta": meta,
                    "commit_url": commit.get("html_url") if commit else self.gh.raw_url(path),
                })

            for path in common:
                old = self.state.modules[path]
                new_sha = current[path]["sha"]
                if old.sha == new_sha and not force:
                    continue
                info = await self.gh.get_file(session, path)
                src = self.decode_b64(info.get("content", ""))
                meta = self.parse_module_meta(src, path)
                commit = await self.gh.latest_commit_for_file(session, path)
                updates.append({
                    "kind": "updated", "path": path, "old_v": old.version, "new_v": meta["version"],
                    "sha": new_sha, "meta": meta,
                    "commit_url": commit.get("html_url") if commit else self.gh.raw_url(path),
                })

            for path in deleted:
                old = self.state.modules[path]
                commit = await self.gh.latest_commit_for_file(session, path)
                updates.append({
                    "kind": "deleted", "path": path, "old_v": old.version, "new_v": "-", "sha": "",
                    "meta": {"name": Path(path).stem, "description": "Module removed", "commands": []},
                    "commit_url": commit.get("html_url") if commit else f"https://github.com/{GITHUB_REPO}",
                })

            for upd in sorted(updates, key=lambda x: x["path"]):
                await self.notify_owner(upd)
                self.apply_state(upd)

            if updates:
                self.storage.save(self.state)

    async def notify_owner(self, upd: dict[str, Any]) -> None:
        stamp = int(datetime.now(tz=timezone.utc).timestamp())
        key = f"{upd['path']}:{upd.get('sha', 'x')}:{stamp}"
        self.pending_updates[key] = upd

        text = self.build_text(upd["kind"], upd["old_v"], upd["new_v"], upd["meta"], upd["commit_url"])
        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Залить", callback_data=f"publish:{key}")
        kb.button(text="⏭ Пропустить", callback_data=f"skip:{key}")
        kb.adjust(2)
        await self.bot.send_message(OWNER_ID, text, reply_markup=kb.as_markup(), disable_web_page_preview=False)

    async def publish_update(self, upd: dict[str, Any]) -> None:
        text = self.build_text(upd["kind"], upd["old_v"], upd["new_v"], upd["meta"], upd["commit_url"])
        kb = InlineKeyboardBuilder()
        kb.button(text="📦 Install Module", url=self.gh.raw_url(upd["path"]))
        kb.button(text="ℹ️ Manual install", url="https://github.com/hikariatama/Hikka/wiki")
        kb.adjust(1)

        for channel_id in self.state.publish_channels:
            try:
                banner = upd["meta"].get("banner")
                if banner and upd["kind"] != "deleted":
                    await self.bot.send_photo(channel_id, banner, caption=text, reply_markup=kb.as_markup())
                else:
                    await self.bot.send_message(channel_id, text, reply_markup=kb.as_markup(), disable_web_page_preview=False)
            except Exception as exc:
                logger.exception("publish failed channel=%s err=%s", channel_id, exc)

    def apply_state(self, upd: dict[str, Any]) -> None:
        if upd["kind"] == "deleted":
            self.state.modules.pop(upd["path"], None)
            return
        self.state.modules[upd["path"]] = TrackedModule(path=upd["path"], sha=upd["sha"], version=upd["new_v"])

    async def worker(self) -> None:
        await self.bootstrap()
        while True:
            try:
                await self.scan_and_notify()
            except Exception as exc:
                logger.exception("worker error: %s", exc)
            await asyncio.sleep(max(60, POLL_INTERVAL_SEC))

    async def run(self) -> None:
        if BOT_TOKEN == "PUT_YOUR_BOT_TOKEN_HERE":
            raise RuntimeError("Set BOT_TOKEN constant in aiogram_module_publisher_bot.py")
        asyncio.create_task(self.worker())
        await self.dp.start_polling(self.bot, allowed_updates=self.dp.resolve_used_update_types())


async def main() -> None:
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    app = App(bot)
    await app.run()


if __name__ == "__main__":
    asyncio.run(main())
