# QwenCLI — README EN

[![Telegram](https://img.shields.io/badge/Telegram-@goymodules-2CA5E0?logo=telegram&logoColor=white)](https://t.me/goymodules)

![QwenCLI Banner](../assets/QwenCLI.png)

## What this module is
QwenCLI is a multifunctional AI module for Hikka and Heroku. It provides a unified command layer for productivity tasks, chat-side automation, and assistant-style workflows.

## How it works
The module exposes an internal command suite and relies on dependencies that should be bootstrapped once. After bootstrap, you manage everything from your userbot prefix.

## Module file
- `QwenCLI.py`

## Installation
```text
.dlm https://raw.githubusercontent.com/sepiol026-wq/goypulse/main/QwenCLI.py
```

## First run (important)
To initialize runtime dependencies, run:
```text
.qwinstall
```
If you use a custom prefix, replace `.` with your own (for example `!qwinstall`). This command installs the required packages into your environment and prepares the module for full operation.

## Core commands
- `.qwinstall` — install/reinstall dependencies.
- `.qwhelp` — show the command map.
- `.qw*` — QwenCLI working command group (see in-module help for the exact set).

## Typical workflow
1. Install with `.dlm`.
2. Execute `.qwinstall`.
3. Review features via `.qwhelp`.
4. Use the `.qw*` command family in your target chat.

## Navigation
- [Back to English index](./readme_en.md)
- [Русская версия](./readme_qwencli_ru.md)

## License
This README and module are protected under **GNU AGPLv3**. Details: [LICENSE](../LICENSE).
