# GoyPulse — README EN

[![Telegram](https://img.shields.io/badge/Telegram-@goymodules-2CA5E0?logo=telegram&logoColor=white)](https://t.me/goymodules)

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
- [Back to English index](./readme_en.md)
- [Русская версия](./readme_goypulse_ru.md)

## License
This README and module are protected under **GNU AGPLv3**. Details: [LICENSE](../LICENSE).
