# OmniCLI — README RU

[![Telegram](https://img.shields.io/badge/Telegram-@goymodules-2CA5E0?logo=telegram&logoColor=white)](https://t.me/goymodules)

![QwenCLI Banner](../assets/QwenCLI.png)

## Что это за модуль
OmniCLI — единый AI CLI-хаб для Hikka/Heroku: в одном модуле можно переключать движок между Qwen CLI, OpenAI Codex CLI, Gemini CLI и Claude Code.

## Ключевая идея
Один набор команд (`.om*`), один интерфейс с кнопками, одна память диалога и переключаемый backend-провайдер.

## Файл модуля
- `OmniCLI.py`

## Установка
```text
.dlm https://raw.githubusercontent.com/sepiol026-wq/GoyModules/main/OmniCLI.py
```

## Базовые команды
- `.om <запрос>` — выполнить запрос через выбранный CLI.
- `.omprovider [qwen|codex|gemini|claude]` — выбрать провайдера.
- `.omproviders` — показать все доступные провайдеры.
- `.ommodel [model]` — установить/показать модель.
- `.omprompt [text|-c]` — системный промпт.
- `.omclear` — очистить память чата.
- `.omauth` — проверить бинарник/авторизацию.
- `.ompatch <fix>` — доработать прошлый ответ.

## Требования по CLI
- Qwen: бинарник `qwen` + `QWEN_API_KEY`.
- Codex: бинарник `codex` + `OPENAI_API_KEY` (или `CODEX_API_KEY`).
- Gemini: бинарник `gemini` + `GEMINI_API_KEY`.
- Claude: бинарник `claude` + `ANTHROPIC_API_KEY`.

## Навигация
- [Назад в русский индекс](./readme_ru.md)
- [English version](./readme_omnicli_en.md)
