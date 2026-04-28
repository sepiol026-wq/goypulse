# GoyPulse — README EN

[![Root](https://img.shields.io/badge/Root-111111?style=for-the-badge&logo=github&logoColor=white)](../README.md)
[![DOCS RU](https://img.shields.io/badge/DOCS-RU-2CA5E0?style=for-the-badge&logo=readthedocs&logoColor=white)](./readme_ru.md)
[![DOCS EN](https://img.shields.io/badge/DOCS-EN-6f42c1?style=for-the-badge&logo=readthedocs&logoColor=white)](./readme_en.md)
[![Telegram](https://img.shields.io/badge/Telegram-@goymodules-26A5E4?style=for-the-badge&logo=telegram&logoColor=white)](https://t.me/goymodules)
[![Stars](https://img.shields.io/github/stars/sepiol026-wq/GoyModules?style=for-the-badge&logo=github&label=stars)](https://github.com/sepiol026-wq/GoyModules/stargazers)
[![Forks](https://img.shields.io/github/forks/sepiol026-wq/GoyModules?style=for-the-badge&logo=github&label=forks)](https://github.com/sepiol026-wq/GoyModules/forks)
[![Issues](https://img.shields.io/github/issues/sepiol026-wq/GoyModules?style=for-the-badge&logo=github&label=issues)](https://github.com/sepiol026-wq/GoyModules/issues)
[![Pull requests](https://img.shields.io/github/issues-pr/sepiol026-wq/GoyModules?style=for-the-badge&logo=github&label=pull%20requests)](https://github.com/sepiol026-wq/GoyModules/pulls)
[![Commits](https://img.shields.io/github/commit-activity/t/sepiol026-wq/GoyModules?style=for-the-badge&logo=git&label=commits)](https://github.com/sepiol026-wq/GoyModules/commits/main)
[![Last commit](https://img.shields.io/github/last-commit/sepiol026-wq/GoyModules?style=for-the-badge&logo=git&label=last%20commit)](https://github.com/sepiol026-wq/GoyModules/commits/main)
[![License](https://img.shields.io/github/license/sepiol026-wq/GoyModules?style=for-the-badge&label=license)](https://github.com/sepiol026-wq/GoyModules/blob/main/LICENSE)

<p align='center'>[![Docs RU](https://img.shields.io/badge/RU-blue?logo=book)](./readme_ru.md) [![Docs EN](https://img.shields.io/badge/EN-blue?logo=book)](./readme_en.md) [![Src](https://img.shields.io/badge/SRC-blue?logo=book)](https://github.com/sepiol026-wq/GoyModules/blob/main/goypulse.py) [![Raw](https://img.shields.io/badge/RAW-blue?logo=book)](https://raw.githubusercontent.com/sepiol026-wq/GoyModules/main/goypulse.py)</p>


![GoyPulse Banner](../assets/goypulse.png)

## What this module is
GoyPulse is an advanced Markov-based autoreply engine. It learns from chat messages, adapts response behavior to context, supports stealth workflows and flexible reply tuning.

## How it works
1. Reads chat history and incoming messages.
2. Updates internal memory/model state.
3. Generates replies based on mode and constraints.
4. Runs as a single autoreply engine without update/backup subsystems.

## Module file
- `goypulse.py`

## Installation
```text
.dlm https://raw.githubusercontent.com/sepiol026-wq/GoyModules/main/goypulse.py
```

## Core commands
- `.gpulse on/off` — enable or disable engine.
- `.gpref` — refresh/learn from data.
- `.gpstat` — runtime metrics.
- `.gpinfo` — chat diagnostics.

## Recommended onboarding
1. Load module.
2. Enable with `.gpulse on` in target chat.
3. Trigger `.gpref` after initial message collection.
4. Tune behavior with `.gpset` if needed.

## Navigation
- [Root](../README.md)
- [Back to English index](./readme_en.md)
- [Русская версия](./readme_goypulse_ru.md)

## License
This README and module are protected under **GNU AGPLv3**. Details: [LICENSE](../LICENSE).
