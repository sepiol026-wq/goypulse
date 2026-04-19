# OmniCLI — README EN

[![Telegram](https://img.shields.io/badge/Telegram-@goymodules-2CA5E0?logo=telegram&logoColor=white)](https://t.me/goymodules)

![QwenCLI Banner](../assets/QwenCLI.png)

## What this module is
OmniCLI is a unified AI CLI hub for Hikka/Heroku. It lets you switch providers in one place: Qwen CLI, OpenAI Codex CLI, Gemini CLI, and Claude Code.

## Core concept
One command namespace (`.om*`), one button-driven UX, one chat memory layer, and a pluggable backend provider.

## Module file
- `OmniCLI.py`

## Installation
```text
.dlm https://raw.githubusercontent.com/sepiol026-wq/GoyModules/main/OmniCLI.py
```

## Core commands
- `.om <query>` — run a request via currently selected CLI.
- `.omprovider [qwen|codex|gemini|claude]` — set provider.
- `.omproviders` — show available providers.
- `.ommodel [model]` — set/show model.
- `.omprompt [text|-c]` — system prompt.
- `.omclear` — clear chat memory.
- `.omauth` — verify binary/auth.
- `.ompatch <fix>` — patch last answer.

## CLI requirements
- Qwen: `qwen` binary + `QWEN_API_KEY`.
- Codex: `codex` binary + `OPENAI_API_KEY` (or `CODEX_API_KEY`).
- Gemini: `gemini` binary + `GEMINI_API_KEY`.
- Claude: `claude` binary + `ANTHROPIC_API_KEY`.

## Navigation
- [Back to English index](./readme_en.md)
- [Русская версия](./readme_omnicli_ru.md)
