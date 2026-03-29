"""Инструменты для получения вакансий с HeadHunter (и опционально Habr Career).

Модуль решает узкий набор задач:
- формирует и отправляет запросы с троттлингом 1 rps;
- повторяет попытки при временных ошибках;
- нормализует базовые поля для дальнейшей обработки.

Сетевое использование максимально щадящее, чтобы не нарушать лимиты HH.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

import requests

API_URL = "https://api.hh.ru/vacancies"
USER_AGENT = "IT-Market-Analyzer/1.0 (github.com/Perlovas)"

_AREAS_CACHE: Optional[List[Dict]] = None


def _load_areas(timeout: int = 10) -> List[Dict]:
    global _AREAS_CACHE
    if _AREAS_CACHE is not None:
        return _AREAS_CACHE
    resp = requests.get("https://api.hh.ru/areas", headers={"User-Agent": USER_AGENT}, timeout=timeout)
    resp.raise_for_status()
    _AREAS_CACHE = resp.json()
    return _AREAS_CACHE


def resolve_area_id(area_name: str, timeout: int = 10) -> Optional[str]:
    """Найти id региона HH по названию (регистр не важен)."""

    name = area_name.strip().lower()
    if not name:
        return None
    if name.isdigit():
        return name

    def walk(nodes):
        for node in nodes:
            if node.get("name", "").lower() == name:
                return node.get("id")
            if "areas" in node:
                found = walk(node["areas"])
                if found:
                    return found
        return None

    try:
        areas = _load_areas(timeout=timeout)
    except requests.RequestException:
        return None
    return walk(areas)


class CaptchaRequired(Exception):
    """Исключение при требовании капчи HH."""

    def __init__(self, url: str):
        super().__init__("captcha required")
        self.url = url


@dataclass
class Vacancy:
    """Нормализованное представление вакансии, возвращаемое парсером."""

    id: str
    name: str
    employer: str
    city: str
    experience: str
    published_at: str
    alternate_url: str
    description: str
    salary_from: Optional[float]
    salary_to: Optional[float]
    currency: Optional[str]
    keyword: str
    key_skills: Optional[List[str]]
    source: str = "hh"

    def to_dict(self) -> Dict[str, Optional[str]]:
        return self.__dict__


class HHParser:
    """Клиент HH API с учётом лимитов и повторных попыток."""

    def __init__(
        self,
        rate_limit_per_sec: float = 1.0,
        max_retries: int = 3,
        timeout: int = 10,
        fetch_details: bool = False,
        area: Optional[str] = None,
        experience: Optional[str] = None,
        only_with_salary: bool = False,
        max_items: Optional[int] = None,
    ) -> None:
        self.delay = 1 / rate_limit_per_sec
        self.max_retries = max_retries
        self.timeout = timeout
        self.fetch_details = fetch_details
        self.area = area
        self.experience = experience
        self.only_with_salary = only_with_salary
        self.max_items = max_items

    def _request(self, url: str, params: Optional[Dict] = None) -> Dict:
        """Выполнить GET-запрос с повторами и бэкоффом."""

        for attempt in range(1, self.max_retries + 1):
            try:
                resp = requests.get(
                    url,
                    params=params,
                    headers={"User-Agent": USER_AGENT},
                    timeout=self.timeout,
                )
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 403 and "captcha_required" in resp.text:
                    try:
                        data = resp.json()
                        captcha_url = data["errors"][0].get("captcha_url")
                    except Exception:
                        captcha_url = None
                    raise CaptchaRequired(captcha_url or "https://hh.ru/account/captcha")
                logging.warning("HH API non-200 (%s): %s", resp.status_code, resp.text)
            except requests.RequestException as exc:
                logging.warning("HH API request error: %s", exc)

            sleep_for = self.delay * attempt
            logging.info("Retry %s/%s in %.1fs", attempt, self.max_retries, sleep_for)
            time.sleep(sleep_for)

        raise RuntimeError(f"Failed to fetch {url} after {self.max_retries} attempts")

    def _extract_description(self, item: Dict) -> str:
        """Использовать краткое описание (responsibility/requirement) как текст."""

        snippet = item.get("snippet", {})
        return (snippet.get("responsibility") or snippet.get("requirement") or "").strip()

    def _fetch_detail(self, vacancy_id: str) -> Dict:
        """Получить только key_skills (минимальный набор)."""

        if not self.fetch_details:
            return {}
        url = f"{API_URL}/{vacancy_id}"
        try:
            data = self._request(url)
            time.sleep(self.delay)
            return {"key_skills": data.get("key_skills", [])}
        except Exception as exc:
            logging.warning("Detail fetch failed for %s: %s", vacancy_id, exc)
            return {}

    def fetch(
        self,
        keywords: Iterable[str],
        pages: int = 1,
        per_page: int = 100,
    ) -> List[Vacancy]:
        """Загрузить вакансии по всем ключевым словам и страницам.

        HH ограничивает per_page до 100 и общий объём примерно 2000 вакансий.
        """

        vacancies: List[Vacancy] = []
        for keyword in keywords:
            logging.info("Fetching vacancies for keyword='%s' pages=%s", keyword, pages)
            for page in range(pages):
                params = {
                    "text": keyword,
                    "page": page,
                    "per_page": per_page,
                }
                if self.area:
                    params["area"] = self.area
                if self.experience:
                    params["experience"] = self.experience
                if self.only_with_salary:
                    params["only_with_salary"] = "true"
                payload = self._request(API_URL, params=params)
                items = payload.get("items", [])
                logging.info("Page %s/%s received %s items", page + 1, pages, len(items))
                if not items:
                    logging.info("Пустая страница %s — останавливаю запросы по ключу '%s'", page + 1, keyword)
                    break
                for raw in items:
                    detail = {}
                    try:
                        detail = self._fetch_detail(raw.get("id")) if self.fetch_details else {}
                    except CaptchaRequired:
                        logging.warning("Капча при детальном запросе; прекращаем детализацию.")
                        self.fetch_details = False
                    description = ""  # не тянем полное описание
                    key_skills = detail.get("key_skills") or []
                    ks = [s.get("name", "") for s in key_skills]
                    salary = raw.get("salary") or {}
                    area = raw.get("area") or {}
                    experience = raw.get("experience") or {}
                    employer = (raw.get("employer") or {}).get("name", "")
                    vacancies.append(
                        Vacancy(
                            id=str(raw.get("id")),
                            name=raw.get("name", ""),
                            employer=employer,
                            city=area.get("name", ""),
                            experience=experience.get("name", ""),
                            published_at=raw.get("published_at", ""),
                            alternate_url=raw.get("alternate_url", ""),
                            description=description,
                            salary_from=salary.get("from"),
                            salary_to=salary.get("to"),
                            currency=salary.get("currency"),
                            keyword=keyword,
                            key_skills=ks,
                        )
                    )
                time.sleep(self.delay)

                if self.max_items and len(vacancies) >= self.max_items:
                    logging.info("Достигнут лимит %s вакансий, останавливаю сбор", self.max_items)
                    return vacancies

        return vacancies


def fetch_vacancies(
    keywords: Iterable[str],
    pages: int = 1,
    per_page: int = 50,
    fetch_details: bool = False,
    area: Optional[str] = None,
    experience: Optional[str] = None,
    only_with_salary: bool = False,
    max_items: Optional[int] = None,
    rate_limit_per_sec: float = 1.0,
    max_retries: int = 3,
    timeout: int = 10,
) -> List[Dict]:
    """Удобная обёртка, возвращающая словари."""

    parser = HHParser(
        rate_limit_per_sec=rate_limit_per_sec,
        max_retries=max_retries,
        timeout=timeout,
        fetch_details=fetch_details,
        area=area,
        experience=experience,
        only_with_salary=only_with_salary,
        max_items=max_items,
    )
    return [v.to_dict() for v in parser.fetch(keywords, pages=pages, per_page=per_page)]


__all__ = ["HHParser", "Vacancy", "fetch_vacancies"]


def check_hh_available(timeout: int = 5) -> bool:
    """Проверка доступности API HH (возвращает True при коде 200)."""

    try:
        resp = requests.get("https://api.hh.ru/status", headers={"User-Agent": USER_AGENT}, timeout=timeout)
        return resp.status_code == 200
    except requests.RequestException:
        return False
