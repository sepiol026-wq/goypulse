# meta developer: @mtproto_architect
# Description: BurpSuite companion panel for Heroku Userbot (process control + quick setup)

import os
import signal
import shlex
import asyncio
from telethon.tl.types import Message

from .. import loader, utils


@loader.tds
class BurpSuiteMod(loader.Module):
    """BurpSuite companion for Heroku Userbot (launcher + inline control panel)."""

    strings = {
        "name": "burpsuite",
        "panel": (
            "🧩 <b>BurpSuite Companion</b>\n"
            "<i>Это модуль-оркестратор: управляет Burp process, но не заменяет полный GUI BurpSuite.</i>\n\n"
            "• Status: <code>{status}</code>\n"
            "• Host/Port: <code>{host}:{port}</code>\n"
            "• Project: <code>{project}</code>\n"
            "• Jar: <code>{jar}</code>"
        ),
        "started": "✅ Burp process запущен. PID: <code>{}</code>",
        "stopped": "🛑 Burp process остановлен.",
        "not_running": "ℹ️ Burp process не запущен.",
        "already_running": "⚠️ Burp process уже запущен. PID: <code>{}</code>",
        "cfg_updated": "✅ Настройка <code>{}</code> обновлена: <code>{}</code>",
        "unknown_key": "❌ Неизвестный ключ. Доступно: jar, java, host, port, project, jvm",
        "usage_set": "💡 Использование: <code>.burpset &lt;key&gt; &lt;value&gt;</code>",
        "status_text": (
            "📊 <b>Burp status</b>\n"
            "• Running: <code>{running}</code>\n"
            "• PID: <code>{pid}</code>\n"
            "• Host/Port: <code>{host}:{port}</code>\n"
            "• Project: <code>{project}</code>\n"
            "• Jar exists: <code>{jar_exists}</code>"
        ),
        "guide": (
            "📘 <b>Quick setup (Heroku Userbot)</b>\n\n"
            "1) Загрузи Burp JAR в доступный путь (например, <code>/app/burp/burpsuite.jar</code>).\n"
            "2) Настрой модуль: <code>.burpset jar /app/burp/burpsuite.jar</code>\n"
            "3) Запуск: <code>.burpstart</code>\n"
            "4) Остановка: <code>.burpstop</code>\n"
            "5) Логи: <code>.burplog</code>\n\n"
            "⚠️ На Heroku полноценный desktop GUI обычно недоступен. Этот модуль делает управление процессом и конфигами через Telegram."
        ),
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue("jar", "/app/burp/burpsuite.jar", "Path to burpsuite jar", validator=loader.validators.String()),
            loader.ConfigValue("java", "java", "Java executable", validator=loader.validators.String()),
            loader.ConfigValue("host", "127.0.0.1", "Proxy bind host", validator=loader.validators.String()),
            loader.ConfigValue("port", 8080, "Proxy bind port", validator=loader.validators.Integer(minimum=1, maximum=65535)),
            loader.ConfigValue("project", "/app/burp/project.burp", "Burp project file", validator=loader.validators.String()),
            loader.ConfigValue("jvm", "-Xmx1024m", "JVM flags", validator=loader.validators.String()),
        )
        self._log_file = "/tmp/burpsuite.log"

    def _pid(self):
        return int(self.get("pid", 0) or 0)

    def _set_pid(self, pid: int):
        self.set("pid", int(pid or 0))

    def _running(self):
        pid = self._pid()
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def _build_cmd(self):
        java = shlex.quote(str(self.config["java"]))
        jvm = str(self.config["jvm"]).strip()
        jar = shlex.quote(str(self.config["jar"]))
        host = shlex.quote(str(self.config["host"]))
        port = int(self.config["port"])
        project = shlex.quote(str(self.config["project"]))
        jvm_part = f" {jvm}" if jvm else ""
        return (
            f"nohup {java}{jvm_part} -jar {jar} "
            f"--headless.mode=true --project-file={project} --proxy-listen-address={host} --proxy-listen-port={port} "
            f">> {shlex.quote(self._log_file)} 2>&1 & echo $!"
        )

    async def _render_panel(self, target):
        status = "running" if self._running() else "stopped"
        text = self.strings("panel").format(
            status=status,
            host=self.config["host"],
            port=self.config["port"],
            project=self.config["project"],
            jar=self.config["jar"],
        )
        kb = [
            [
                {"text": "📊 Status", "callback": self._cb_status},
                {"text": "▶️ Start", "callback": self._cb_start},
                {"text": "⏹ Stop", "callback": self._cb_stop},
            ],
            [
                {"text": "🧾 Log tail", "callback": self._cb_log},
                {"text": "📘 Setup guide", "callback": self._cb_guide},
            ],
            [{"text": "🔄 Refresh", "callback": self._cb_refresh}],
        ]
        if getattr(self, "inline", None) and hasattr(self.inline, "form"):
            await self.inline.form(text=text, message=target, reply_markup=kb)
            return
        await utils.answer(target, text)

    @loader.command()
    async def burp(self, message: Message):
        """Open BurpSuite inline panel."""
        await self._render_panel(message)

    @loader.command()
    async def burpset(self, message: Message):
        """.burpset <jar|java|host|port|project|jvm> <value>"""
        args = utils.get_args_raw(message).split(maxsplit=1)
        if len(args) != 2:
            return await utils.answer(message, self.strings("usage_set"))
        key, value = args[0].strip().lower(), args[1].strip()
        if key not in {"jar", "java", "host", "port", "project", "jvm"}:
            return await utils.answer(message, self.strings("unknown_key"))
        if key == "port":
            value = int(value)
        self.config[key] = value
        await utils.answer(message, self.strings("cfg_updated").format(key, value))

    @loader.command()
    async def burpstart(self, message: Message):
        """Start Burp process."""
        if self._running():
            return await utils.answer(message, self.strings("already_running").format(self._pid()))
        cmd = self._build_cmd()
        proc = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, _ = await proc.communicate()
        pid_text = (out.decode(errors="ignore").strip() or "0").splitlines()[-1]
        pid = int(pid_text) if pid_text.isdigit() else 0
        self._set_pid(pid)
        if pid > 0:
            await utils.answer(message, self.strings("started").format(pid))
            return
        await utils.answer(message, "❌ Не удалось запустить Burp process. Проверь .burplog")

    @loader.command()
    async def burpstop(self, message: Message):
        """Stop Burp process."""
        pid = self._pid()
        if not self._running():
            self._set_pid(0)
            return await utils.answer(message, self.strings("not_running"))
        os.kill(pid, signal.SIGTERM)
        self._set_pid(0)
        await utils.answer(message, self.strings("stopped"))

    @loader.command()
    async def burpstatus(self, message: Message):
        """Show Burp process status."""
        await utils.answer(
            message,
            self.strings("status_text").format(
                running=self._running(),
                pid=self._pid() or "-",
                host=self.config["host"],
                port=self.config["port"],
                project=self.config["project"],
                jar_exists=os.path.exists(self.config["jar"]),
            ),
        )

    @loader.command()
    async def burplog(self, message: Message):
        """Show last 30 log lines."""
        if not os.path.exists(self._log_file):
            return await utils.answer(message, "ℹ️ Лог пока пуст: <code>/tmp/burpsuite.log</code>")
        with open(self._log_file, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()[-30:]
        txt = "".join(lines).strip() or "(empty)"
        await utils.answer(message, f"<b>Burp log tail</b>\n<code>{utils.escape_html(txt)[:3500]}</code>")

    async def _cb_refresh(self, call):
        await self._render_panel(call)

    async def _cb_status(self, call):
        await call.answer("Status refreshed", show_alert=False)
        await call.edit(
            self.strings("status_text").format(
                running=self._running(),
                pid=self._pid() or "-",
                host=self.config["host"],
                port=self.config["port"],
                project=self.config["project"],
                jar_exists=os.path.exists(self.config["jar"]),
            )
        )

    async def _cb_start(self, call):
        if self._running():
            await call.answer(f"Already running PID {self._pid()}", show_alert=True)
            return
        cmd = self._build_cmd()
        proc = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, _ = await proc.communicate()
        pid_text = (out.decode(errors="ignore").strip() or "0").splitlines()[-1]
        pid = int(pid_text) if pid_text.isdigit() else 0
        self._set_pid(pid)
        if pid > 0:
            await call.answer(f"Started PID {pid}", show_alert=True)
        else:
            await call.answer("Start failed", show_alert=True)

    async def _cb_stop(self, call):
        pid = self._pid()
        if not self._running():
            self._set_pid(0)
            await call.answer("Not running", show_alert=True)
            return
        os.kill(pid, signal.SIGTERM)
        self._set_pid(0)
        await call.answer("Stopped", show_alert=True)

    async def _cb_log(self, call):
        if not os.path.exists(self._log_file):
            await call.answer("Лог пуст", show_alert=True)
            return
        with open(self._log_file, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()[-20:]
        txt = "".join(lines).strip() or "(empty)"
        await call.edit(f"<b>Burp log tail</b>\n<code>{utils.escape_html(txt)[:3500]}</code>")

    async def _cb_guide(self, call):
        await call.edit(self.strings("guide"))
