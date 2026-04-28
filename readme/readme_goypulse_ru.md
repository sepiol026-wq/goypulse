# GoyPulse — README RU

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

## Что это за модуль
GoyPulse — продвинутый нейро-автоответчик на основе цепей Маркова. Модуль учится на сообщениях, учитывает контекст чата, поддерживает скрытый режим и гибкую настройку ответов.

## Как это работает
1. Считывает историю и новые сообщения в чатах.
2. Обновляет языковую модель по локальной памяти.
3. Генерирует ответ с учётом настроек, ограничений и режима работы.
4. Работает как единый автоответчик без подсистем обновлений/бэкапов.

## Файл модуля
- `goypulse.py`

## Установка
```text
.dlm https://raw.githubusercontent.com/sepiol026-wq/GoyModules/main/goypulse.py
```

## Основные команды
- `.gpulse on/off` — включение и отключение.
- `.gpref` — обновить память/рефреш.
- `.gpstat` — статус и статистика.
- `.gpinfo` — диагностика текущего чата.

## Рекомендованный старт
1. Загрузите модуль.
2. Включите `.gpulse on` в нужном чате.
3. Через время выполните `.gpref` для улучшения ответов.
4. При необходимости докрутите параметры через `.gpset`.

## Навигация
- [Root](../README.md)
- [Назад в русский индекс](./readme_ru.md)
- [English version](./readme_goypulse_en.md)

## Лицензия
Этот README и модуль защищены лицензией **GNU AGPLv3**. Подробности: [LICENSE](../LICENSE).
