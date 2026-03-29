"""CLI-точка входа для анализатора рынка IT-вакансий.

Пример запуска:
    python main.py --keywords "Python разработчик" "Data Scientist" --pages 5
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from analyzer import (
    publications_over_time,
    salary_distribution,
    top_cities,
    top_skills,
)
from data_processor import (
    DEFAULT_SKILLS,
    deduplicate,
    extract_skills,
    normalize_salaries,
    save_dataset,
)
from hh_parser import fetch_vacancies, resolve_area_id
from visualizer import (
    plot_publications_over_time,
    plot_salary_distribution,
    plot_top_cities,
    plot_top_skills,
)


logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Анализатор рынка вакансий (HH)")
    parser.add_argument(
        "--keywords",
        nargs="+",
        required=True,
        help="Список поисковых запросов",
    )
    parser.add_argument("--pages", type=int, default=1, help="Количество страниц HH (по 50 вакансий)")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/vacancies.json"),
        help="Путь для сохранения сырых данных (json/csv)",
    )
    parser.add_argument(
        "--skills",
        nargs="*",
        default=DEFAULT_SKILLS,
        help="Список навыков для поиска в описании",
    )
    parser.add_argument(
        "--details",
        action="store_true",
        help="Запрашивать полное описание вакансии (медленнее)",
    )
    parser.add_argument("--area", type=str, default=None, help="Город/регион HH (название или ID)")
    parser.add_argument(
        "--experience",
        type=str,
        default=None,
        help="Уровень опыта (noExperience, between1And3, between3And6, moreThan6)",
    )
    parser.add_argument(
        "--with-salary",
        action="store_true",
        help="Фильтровать только вакансии с указанной зарплатой",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    logging.info("Запуск парсинга HH")

    area_id = resolve_area_id(args.area) if args.area else None
    if args.area and not area_id:
        logging.warning("Не удалось определить регион по названию '%s', поиск без фильтра", args.area)

    records = fetch_vacancies(
        keywords=args.keywords,
        pages=args.pages,
        per_page=100,
        fetch_details=True,  # нужно для key_skills
        area=area_id,
        experience=args.experience,
        only_with_salary=args.with_salary,
    )
    df = pd.DataFrame(records)
    logging.info("Получено сырых записей: %s", len(df))

    df = deduplicate(df)
    logging.info("После удаления дублей: %s", len(df))
    df = normalize_salaries(df)
    df = extract_skills(df, skills=args.skills)
    logging.info(
        "С зарплатой в рублях: %s, без зарплаты/другая валюта: %s",
        df["mid_salary"].notna().sum(),
        df["mid_salary"].isna().sum(),
    )

    save_dataset(df, args.output)
    logging.info("Данные сохранены в %s", args.output)

    # Аналитика
    salary_series = salary_distribution(df)
    skills_series = top_skills(df)
    cities_series = top_cities(df)
    timeline_series = publications_over_time(df)

    out_dir = Path("output")
    plot_salary_distribution(salary_series, out_dir / "salary_hist.png")
    plot_top_skills(skills_series, out_dir / "top_skills.png")
    plot_top_cities(cities_series, out_dir / "top_cities.png")
    plot_publications_over_time(timeline_series, out_dir / "timeline.png")
    logging.info("Графики сохранены в %s", out_dir)


if __name__ == "__main__":
    main()
