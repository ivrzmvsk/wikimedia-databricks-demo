# Wikimedia Databricks Lakehouse Demo

[![Wikimedia CI Pipeline](https://github.com/ivrzmvsk/wikimedia-databricks-demo/actions/workflows/ci.yml/badge.svg)](https://github.com/ivrzmvsk/wikimedia-databricks-demo/actions/workflows/ci.yml)

## Опис проєкту
Пайплайн потокової та пакетної обробки даних Wikimedia на базі Azure Databricks, Medallion Architecture та Delta Lake.

## CI/CD & Автоматизація
- **CI Pipeline:** GitHub Actions автоматично перевіряє код лінтером `flake8` та проганяє 20 юніт-тестів на базі `pytest`, `chispa` та локального PySpark.
- **Branch Protection:** Прямі пуші в гілку `main` заблоковані; злиття коду дозволене лише після проходження всіх тестів.
