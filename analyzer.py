"""Аналитические функции для агрегирования вакансий."""

from __future__ import annotations

import pandas as pd


def salary_distribution(df: pd.DataFrame, bins: int = 20):
    return df["mid_salary"].dropna()


def top_skills(df: pd.DataFrame, top_n: int = 10):
    skills_series = df["skills"].explode().dropna()
    return skills_series.value_counts().head(top_n)


def skills_frequency(df: pd.DataFrame):
    """Полное распределение навыков."""
    return df["skills"].explode().dropna().value_counts()


def top_skills_with_salary(df: pd.DataFrame, top_n: int = 10):
    """ТОП навыков по частоте + средняя/медиана зарплаты для них (RUB)."""

    exploded = df.explode("skills")
    exploded = exploded[exploded["skills"].notna()]
    exploded["skills"] = exploded["skills"].astype(str).str.strip().str.lower()
    exploded = exploded[exploded["skills"] != ""]
    freq = exploded["skills"].value_counts()

    salary_stats_raw = (
        exploded[exploded["mid_salary"].notna()]
        .groupby("skills")["mid_salary"]
        .agg(total_salary="sum", salary_count="count", med_salary="median")
    )
    salary_stats_raw["avg_salary"] = salary_stats_raw["total_salary"] / salary_stats_raw["salary_count"]
    salary_stats = salary_stats_raw.drop(columns=["total_salary"])

    merged = (
        freq.to_frame("freq")
        .join(salary_stats, how="left")
        .dropna(subset=["avg_salary"])
        .sort_values("freq", ascending=False)
        .head(top_n)
    )
    return merged


def skills_salary(df: pd.DataFrame, min_count: int = 3):
    """Средняя зарплата по навыкам (RUB) с порогом по количеству вхождений."""

    exploded = df.explode("skills")
    filtered = exploded[exploded["mid_salary"].notna() & exploded["skills"].notna()]
    grp = filtered.groupby("skills")["mid_salary"]
    stats = grp.agg(["count", "mean", "median", "std"]).rename(
        columns={"count": "freq", "mean": "avg_salary", "median": "med_salary", "std": "std_salary"}
    )
    stats["ci95_low"] = stats["avg_salary"] - 1.96 * (stats["std_salary"] / (stats["freq"] ** 0.5))
    stats["ci95_high"] = stats["avg_salary"] + 1.96 * (stats["std_salary"] / (stats["freq"] ** 0.5))
    return stats[stats["freq"] >= min_count].sort_values("med_salary", ascending=False)


def companies_salary(df: pd.DataFrame, min_count: int = 1):
    """Топ компаний по средней зарплате (учитываются все вакансии с указанной зарплатой)."""

    subset = df[df["mid_salary"].notna() & df["employer"].notna()]
    grp = subset.groupby("employer")["mid_salary"]
    stats = grp.agg(["count", "mean", "median"]).rename(
        columns={"count": "vacancies", "mean": "avg_salary", "median": "med_salary"}
    )
    stats = stats[stats["vacancies"] >= min_count].sort_values("avg_salary", ascending=False)
    return stats


def top_cities(df: pd.DataFrame, top_n: int = 10):
    return df["city"].value_counts().head(top_n)


def publications_over_time(df: pd.DataFrame):
    timeline = pd.to_datetime(df["published_at"]).dt.date
    return timeline.value_counts().sort_index()


__all__ = [
    "salary_distribution",
    "top_skills",
    "top_cities",
    "publications_over_time",
    "skills_frequency",
    "skills_salary",
    "top_skills_with_salary",
    "companies_salary",
]
