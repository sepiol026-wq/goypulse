# GoySecurity — README RU

[![Root](https://img.shields.io/badge/Root-111111?style=for-the-badge&logo=github&logoColor=white)](../README.md)
[![DOCS RU](https://img.shields.io/badge/DOCS-RU-2CA5E0?style=for-the-badge&logo=readthedocs&logoColor=white)](./readme_ru.md)
[![DOCS EN](https://img.shields.io/badge/DOCS-EN-6f42c1?style=for-the-badge&logo=readthedocs&logoColor=white)](./readme_en.md)
[![Telegram](https://img.shields.io/badge/Telegram-@goymodules-26A5E4?style=for-the-badge&logo=telegram&logoColor=white)](https://t.me/goymodules)
[![Stars](https://img.shields.io/github/stars/sepiol026-wq/GoyModules?style=for-the-badge&logo=github&label=stars)](https://github.com/sepiol026-wq/GoyModules/stargazers)
[![Forks](https://img.shields.io/github/forks/sepiol026-wq/GoyModules?style=for-the-badge&logo=github&label=forks)](https://github.com/sepiol026-wq/GoyModules/forks)
[![Watchers](https://img.shields.io/github/watchers/sepiol026-wq/GoyModules?style=for-the-badge&logo=github&label=watchers)](https://github.com/sepiol026-wq/GoyModules/watchers)
[![Issues](https://img.shields.io/github/issues/sepiol026-wq/GoyModules?style=for-the-badge&logo=github&label=issues)](https://github.com/sepiol026-wq/GoyModules/issues)
[![Pull requests](https://img.shields.io/github/issues-pr/sepiol026-wq/GoyModules?style=for-the-badge&logo=github&label=pull%20requests)](https://github.com/sepiol026-wq/GoyModules/pulls)
[![Commits](https://img.shields.io/github/commit-activity/t/sepiol026-wq/GoyModules?style=for-the-badge&logo=git&label=commits)](https://github.com/sepiol026-wq/GoyModules/commits/main)
[![Last commit](https://img.shields.io/github/last-commit/sepiol026-wq/GoyModules?style=for-the-badge&logo=git&label=last%20commit)](https://github.com/sepiol026-wq/GoyModules/commits/main)
[![Repo size](https://img.shields.io/github/repo-size/sepiol026-wq/GoyModules?style=for-the-badge&logo=github&label=repo%20size)](https://github.com/sepiol026-wq/GoyModules)
[![Code size](https://img.shields.io/github/languages/code-size/sepiol026-wq/GoyModules?style=for-the-badge&logo=github&label=code%20size)](https://github.com/sepiol026-wq/GoyModules)
[![Lines](https://img.shields.io/tokei/lines/github/sepiol026-wq/GoyModules?style=for-the-badge&logo=files&label=lines)](https://github.com/sepiol026-wq/GoyModules)
[![Top language](https://img.shields.io/github/languages/top/sepiol026-wq/GoyModules?style=for-the-badge&logo=python&label=top%20lang)](https://github.com/sepiol026-wq/GoyModules)
[![License](https://img.shields.io/github/license/sepiol026-wq/GoyModules?style=for-the-badge&label=license)](https://github.com/sepiol026-wq/GoyModules/blob/main/LICENSE)

<p align='center'>[![Docs RU](https://img.shields.io/badge/RU-blue?logo=book)](./readme_ru.md) [![Docs EN](https://img.shields.io/badge/EN-blue?logo=book)](./readme_en.md) [![Src](https://img.shields.io/badge/SRC-blue?logo=book)](https://github.com/sepiol026-wq/GoyModules/blob/main/goysec.py) [![Raw](https://img.shields.io/badge/RAW-blue?logo=book)](https://raw.githubusercontent.com/sepiol026-wq/GoyModules/main/goysec.py)</p>


![GoySecurity Banner](../assets/goysec.png)

## Что это за модуль
GoySecurity — модуль предварительной проверки кода перед установкой сторонних модулей. Он снижает риск запуска подозрительных скриптов и помогает контролировать доверенные источники.

## Как это работает
- Выполняет скан кода по правилам риска.
- Поддерживает режимы строгости.
- Ведёт историю проверок.
- Даёт объяснение «почему риск» и поддерживает whitelist.

## Файл модуля
- `goysec.py`

## Установка
```text
.dlm https://raw.githubusercontent.com/sepiol026-wq/GoyModules/main/goysec.py
```

## Основные команды
- `.gscan` — скан текущего модуля/ссылки.
- `.gautoscan` — автоскан перед установкой.
- `.gmode` — уровень строгости.
- `.gwl` / `.gunwl` — управление whitelist.
- `.ghist` / `.gwhy` — история и причины срабатывания.
- `.gai` / `.gaicustom` — AI-пояснения.

## Навигация
- [Root](../README.md)
- [Назад в русский индекс](./readme_ru.md)
- [English version](./readme_goysec_en.md)

## Лицензия
Этот README и модуль защищены лицензией **GNU AGPLv3**. Подробности: [LICENSE](../LICENSE).