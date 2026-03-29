"""Простой веб-интерфейс для Анализатора рынка IT-вакансий."""

from __future__ import annotations

import io
from pathlib import Path

from flask import Flask, render_template_string, request, send_file, flash
import pandas as pd

from analyzer import (
    publications_over_time,
    salary_distribution,
    top_cities,
    top_skills,
    skills_frequency,
    skills_salary,
    top_skills_with_salary,
    companies_salary,  
)
from data_processor import DEFAULT_SKILLS, deduplicate, extract_skills, normalize_salaries, save_dataset
from hh_parser import CaptchaRequired, check_hh_available, fetch_vacancies, resolve_area_id
from visualizer import (
    plot_publications_over_time,
    plot_salary_distribution,
    plot_top_cities,
    plot_top_skills,
)

app = Flask(__name__)
app.secret_key = "hh-market-analyzer"

TEMPLATE = """
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <title>Анализатор IT-вакансий</title>
  <style>
    :root {
      --bg: #0f172a;
      --card: #111827;
      --accent: #22c55e;
      --text: #e5e7eb;
      --muted: #9ca3af;
      --border: #1f2937;
    }
    * { box-sizing:border-box; }
    body { font-family: "Inter", system-ui, -apple-system, sans-serif; background: radial-gradient(120% 120% at 20% 20%, #1e293b, #0b1020); color: var(--text); margin:0; min-height:100vh; }
    main { max-width: 1100px; margin: 32px auto 56px; padding: 0 16px; }
    h1 { margin: 0 0 12px; font-size: 28px; letter-spacing:0.2px; }
    p.lead { margin: 0 0 18px; color: var(--muted); }
    label { display:block; margin-top:12px; font-weight:600; color: var(--text); }
    input, textarea { width:100%; padding:10px 12px; border-radius:10px; border:1px solid var(--border); background: #0b1222; color: var(--text); }
    textarea { min-height: 90px; }
    .row { display:flex; gap:12px; flex-wrap:wrap; }
    .row > div { flex:1; min-width:220px; }
    .status { padding:10px 12px; margin:12px 0; border-radius:10px; border:1px solid var(--border); background:#0b1222; display:flex; align-items:center; gap:10px; }
    .dot { width:10px; height:10px; border-radius:50%; background: var(--muted); }
    .ok .dot { background: var(--accent); box-shadow:0 0 12px rgba(34,197,94,0.8); }
    .fail .dot { background: #ef4444; box-shadow:0 0 12px rgba(239,68,68,0.8); }
    button { margin-top:18px; padding:12px 18px; border-radius:12px; border:none; background: linear-gradient(135deg, #22c55e, #16a34a); color:#0b1020; font-weight:700; cursor:pointer; }
    button:hover { filter: brightness(1.05); }
    .cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(240px,1fr)); gap:16px; margin-top:24px; }
    .card { padding:14px; border:1px solid var(--border); border-radius:14px; background: var(--card); box-shadow:0 10px 30px rgba(0,0,0,0.25); }
    .card h3 { margin:0 0 10px; font-size:16px; color:#f8fafc; }
    .card a { display:block; overflow:hidden; border-radius:10px; border:1px solid var(--border); }
    .card img { width:100%; display:block; }
    .flash { color:#fbbf24; margin: 8px 0 0 0; padding-left: 0; }
    .flash li { margin-bottom:4px; }
    table { width:100%; border-collapse:collapse; margin-top:12px; }
    th, td { padding:8px 10px; border:1px solid var(--border); }
    th { background:#0b1222; text-align:left; }
    .loader-backdrop { position:fixed; inset:0; background:rgba(0,0,0,0.55); display:none; align-items:center; justify-content:center; z-index:999; }
    .loader { width:64px; height:64px; border:6px solid #1f2937; border-top-color:#22c55e; border-radius:50%; animation: spin 1s linear infinite; }
    @keyframes spin { to { transform: rotate(360deg); } }
    .lightbox { position:fixed; inset:0; background:rgba(0,0,0,0.7); display:none; align-items:center; justify-content:center; z-index:1000; }
    .lightbox.show { display:flex; }
    .lightbox img { max-width:90vw; max-height:90vh; border:1px solid var(--border); border-radius:14px; box-shadow:0 20px 50px rgba(0,0,0,0.6); }
    .lightbox .close { position:absolute; top:20px; right:26px; font-size:28px; color:#fff; cursor:pointer; }
  </style>
</head>
<body>
<div class="loader-backdrop" id="loader"><div class="loader"></div></div>
<main>
  <h1>Анализатор рынка вакансий HH.ru</h1>
  <p class="lead">Сбор, навыки, зарплаты, города и динамика по HeadHunter</p>

  <div class="status {{ 'ok' if hh_ok else 'fail' }}">
    <span class="dot"></span>
    HH API: {{ 'доступен' if hh_ok else 'недоступен' }}
  </div>

  {% with messages = get_flashed_messages() %}
    {% if messages %}
      <ul class="flash">
      {% for m in messages %}
        <li>{{ m }}</li>
      {% endfor %}
      </ul>
    {% endif %}
  {% endwith %}

  <form method="post" action="/" enctype="application/x-www-form-urlencoded" onsubmit="showLoader()">
    <label>Ключевые слова (по одному в строке)</label>
    <textarea name="keywords" rows="3" required>{{ keywords }}</textarea>

    <div class="row">
      <div>
        <label>Страниц HH (по 50 вакансий)</label>
        <input type="number" name="pages" min="1" max="20" value="{{ pages }}" required>
      </div>
      <div>
        <label>Город/регион</label>
        <input type="text" name="area" value="{{ area }}" placeholder="Москва, Санкт-Петербург, Новосибирск">
      </div>
    </div>

    <button type="submit" style="margin-top:16px;padding:10px 20px;">Запустить сбор и анализ</button>
  </form>

  {% if graphs %}
  <h2>Результаты</h2>
  <div class="cards">
    {% for title, img_path in graphs %}
    <div class="card">
      <h3>{{ title }}</h3>
      <a href="#" onclick="openLightbox('/file/{{ img_path }}'); return false;" title="Открыть в полном размере">
        <img src="/file/{{ img_path }}" alt="{{ title }}">
      </a>
    </div>
    {% endfor %}
  </div>
  {% if salary_mean %}
    <p><strong>Указанная зарплата:</strong> средняя {{ salary_mean }} ₽, медиана {{ salary_median }} ₽ ({{ salary_count }} вакансий с зарплатой)</p>
  {% endif %}

  {% if skills_full_items %}
    <h3>Самые востребованные навыки (частота + средняя/медиана зарплаты)</h3>
    <table border="1" cellpadding="6" cellspacing="0">
      <tr><th>Навык</th><th>Количество</th><th>Средняя (RUB)</th><th>Медиана</th></tr>
      {% for row in skills_full_items %}
        <tr>
          <td>{{ row.skill }}</td>
          <td>{{ row.freq }}</td>
          <td>{{ row.avg_salary and "%.0f"|format(row.avg_salary) or "—" }}</td>
          <td>{{ row.med_salary and "%.0f"|format(row.med_salary) or "—" }}</td>
        </tr>
      {% endfor %}
    </table>
  {% endif %}

  {% if skills_by_salary_rows %}
    <h3>Навыки с наивысшей средней зарплатой</h3>
    <table border="1" cellpadding="6" cellspacing="0">
      <tr><th>Навык</th><th>Средняя (RUB)</th><th>Медиана</th><th>95% Доверительный Интервал</th><th>Вакансий</th></tr>
      {% for row in skills_by_salary_rows %}
        <tr>
          <td>{{ row.skills }}</td>
          <td>{{ "%.0f"|format(row.avg_salary) }}</td>
          <td>{{ "%.0f"|format(row.med_salary) }}</td>
          <td>{{ "%.0f"|format(row.ci95_low) }} – {{ "%.0f"|format(row.ci95_high) }}</td>
          <td>{{ row.freq }}</td>
        </tr>
      {% endfor %}
    </table>
    <p>Порог по количеству вакансий: 3</p>
  {% endif %}

  {% if companies_rows %}
    <h3>Топ платёжеспособных компаний</h3>
    <table border="1" cellpadding="6" cellspacing="0">
      <tr><th>Компания</th><th>Вакансий</th><th>Средняя (RUB)</th><th>Медиана</th></tr>
      {% for row in companies_rows %}
        <tr>
          <td>{{ row.employer }}</td>
          <td>{{ row.vacancies }}</td>
          <td>{{ "%.0f"|format(row.avg_salary) }}</td>
          <td>{{ "%.0f"|format(row.med_salary) }}</td>
        </tr>
      {% endfor %}
    </table>
  {% endif %}
  {% endif %}
  </main>
  <div class="lightbox" id="lightbox" onclick="closeLightbox(event)">
    <span class="close" id="lightbox-close">&times;</span>
    <img id="lightbox-img" src="" alt="preview">
  </div>
  <script>
    function showLoader() {
      const loader = document.getElementById('loader');
      if (loader) loader.style.display = 'flex';
    }
    function openLightbox(src) {
      const lb = document.getElementById('lightbox');
      const img = document.getElementById('lightbox-img');
      img.src = src;
      lb.classList.add('show');
    }
    function closeLightbox(ev) {
      if (ev && ev.target && ev.target.id !== 'lightbox' && ev.target.id !== 'lightbox-close') return;
      const lb = document.getElementById('lightbox');
      lb.classList.remove('show');
    }
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') closeLightbox({target:{id:'lightbox'}});
    });
  </script>
</body>
</html>
"""


@app.route("/", methods=["GET", "POST"])
def index():
    hh_ok = check_hh_available()
    graphs = []
    defaults = {
        "keywords": "Java developer\nPython developer",
        "pages": 1,
        "details": False,
        "area": "",
    }

    if request.method == "POST":
        keywords_raw = request.form.get("keywords", "").strip()
        pages = int(request.form.get("pages", 1))
        details = request.form.get("details") == "on"
        area_input = request.form.get("area") or None

        keywords = [k.strip() for k in keywords_raw.splitlines() if k.strip()]

        if not keywords:
            flash("Укажите хотя бы одно ключевое слово.")
        elif not hh_ok:
            flash("HH недоступен, попробуйте позже.")
        else:
            area_id = resolve_area_id(area_input) if area_input else None
            if area_input and not area_id:
                flash(f"Не удалось найти регион/город «{area_input}». Поиск без фильтра.")
            try:
                records = fetch_vacancies(
                    keywords=keywords,
                    pages=pages,
                    per_page=50,
                    fetch_details=True,  # тянем только нужное: key_skills, город, зарплата, название, компания
                    area=area_id,
                    experience=None,
                    only_with_salary=True,  # чтобы средняя зарплата не была пустой
                    max_retries=3,
                    timeout=7,
                    max_items=50,  # ограничиваем для скорости UI, если выдачи нет — завершится быстрее
                )
            except CaptchaRequired as e:
                flash(f"HH вернул капчу. Откройте и пройдите её: {e.url}")
                records = []

            if records:
                df = pd.DataFrame(records)
                df = deduplicate(df)
                df = normalize_salaries(df)
                df = extract_skills(df, skills=DEFAULT_SKILLS)
                flash(
                    f"Сырых записей: {len(records)}, после дублей: {len(df)}, зарплат в RUB: {df['mid_salary'].notna().sum()}"
                )

                save_dataset(df, Path("data/web_vacancies.json"))

                salary_series = salary_distribution(df)
                skills_series = top_skills(df)
                skills_full = top_skills_with_salary(df, top_n=15)
                skills_by_salary = skills_salary(df)
                companies = companies_salary(df)
                cities_series = top_cities(df)
                timeline_series = publications_over_time(df)
                salary_count = df["mid_salary"].notna().sum()
                salary_mean = round(df["mid_salary"].mean(), 0) if salary_count else None
                salary_median = round(df["mid_salary"].median(), 0) if salary_count else None

                out_dir = Path("output")
                plot_salary_distribution(salary_series, out_dir / "salary_hist.png")
                plot_top_skills(skills_series, out_dir / "top_skills.png")
                plot_top_cities(cities_series, out_dir / "top_cities.png")
                plot_publications_over_time(timeline_series, out_dir / "timeline.png")

                graphs = [
                    ("Распределение зарплат", "salary_hist.png"),
                    ("ТОП навыков", "top_skills.png"),
                    ("ТОП городов", "top_cities.png"),
                    ("Динамика публикаций", "timeline.png"),
                ]

        defaults.update(
            {
                "keywords": keywords_raw,
                "pages": pages,
                "details": details,
                "area": area_input or "",
            }
        )

    return render_template_string(
        TEMPLATE,
        hh_ok=hh_ok,
        graphs=graphs,
        salary_mean=locals().get("salary_mean"),
        salary_median=locals().get("salary_median"),
        salary_count=locals().get("salary_count", 0),
        skills_full_items=list(
            locals().get("skills_full", pd.DataFrame())
            .reset_index()
            .rename(columns={"index": "skill", "skills": "skill"})
            .to_dict("records")
        )
        if "skills_full" in locals()
        else [],
        skills_by_salary_rows=locals()
        .get("skills_by_salary", pd.DataFrame())
        .head(10)
        .reset_index()
        .to_dict("records")
        if "skills_by_salary" in locals()
        else [],
        companies_rows=locals()
        .get("companies", pd.DataFrame())
        .head(10)
        .reset_index()
        .to_dict("records")
        if "companies" in locals()
        else [],
        **defaults,
    )


@app.route("/file/<path:filename>")
def send_output(filename: str):
    path = Path("output") / filename
    if not path.exists():
        return "Не найдено", 404
    return send_file(path)


@app.route("/health")
def health():
    return {"hh_available": check_hh_available()}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
