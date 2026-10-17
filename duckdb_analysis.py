import duckdb
import time

from config import ENDPOINT, ACCESS_KEY, SECRET_KEY, BUCKET, OBJECT_KEY

endpoint = ENDPOINT.replace("http://", "").replace("https://", "")

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

con.execute(f"""
CREATE SECRET rustfs (
    TYPE s3,
    KEY_ID '{ACCESS_KEY}',
    SECRET '{SECRET_KEY}',
    ENDPOINT '{endpoint}',
    URL_STYLE 'path',
    USE_SSL false
);
""")

print("Секрет создан")

t0 = time.perf_counter()

# Читаем вакансии и приводим навыки к единому формату
con.execute(f"""
CREATE TEMP TABLE vacancies AS
SELECT
    row_number() OVER () AS vacancy_id,
    key_skills
FROM read_csv_auto(
    's3://{BUCKET}/{OBJECT_KEY}',
    delim = '\\t',
    header = true
)
WHERE key_skills IS NOT NULL;
""")


# Разбиваем список навыков и удаляем дубликаты
# внутри каждой вакансии
# + во избежание тавтологии уберем из скиллов основную специализацию - сами ЯП (аналогично mapreduce)
con.execute("""
CREATE TEMP TABLE vacancy_skills AS
SELECT DISTINCT
    v.vacancy_id,
    LOWER(TRIM(s.skill)) AS skill
FROM vacancies AS v
CROSS JOIN UNNEST(string_split(v.key_skills, ';')) AS s(skill)
WHERE TRIM(s.skill) <> ''
  AND LOWER(TRIM(s.skill)) NOT IN ('c', 'c++', 'c/c++', 'cpp', 'си', 'си++')
""")


# Одиночные навыки
con.execute("""
CREATE TEMP TABLE skill_counts AS
SELECT
    'skill:' || skill AS feature,
    COUNT(*) AS cnt
FROM vacancy_skills
GROUP BY skill;
""")

# Пары навыков внутри каждой вакансии
con.execute("""
CREATE TEMP TABLE pair_counts AS
SELECT
    'pair:' || a.skill || '&' || b.skill AS feature,
    COUNT(*) AS cnt
FROM vacancy_skills AS a
JOIN vacancy_skills AS b
    ON a.vacancy_id = b.vacancy_id
   AND a.skill < b.skill
GROUP BY a.skill, b.skill;
""")

# Тройки навыков внутри каждой вакансии
con.execute("""
CREATE TEMP TABLE triple_counts AS
SELECT
    'triple:' || a.skill || '&' || b.skill || '&' || c.skill AS feature,
    COUNT(*) AS cnt
FROM vacancy_skills AS a
JOIN vacancy_skills AS b
    ON a.vacancy_id = b.vacancy_id
   AND a.skill < b.skill
JOIN vacancy_skills AS c
    ON b.vacancy_id = c.vacancy_id
   AND b.skill < c.skill
GROUP BY a.skill, b.skill, c.skill;
""")

# Объединяем результаты
con.execute("""
CREATE TEMP TABLE all_counts AS
SELECT * FROM skill_counts
UNION ALL
SELECT * FROM pair_counts
UNION ALL
SELECT * FROM triple_counts;
""")

# Выводим топ-10 для каждого типа
for prefix, title in [
    ("skill:", "ОДИНОЧНЫЕ НАВЫКИ"),
    ("pair:", "ПАРЫ НАВЫКОВ"),
    ("triple:", "ТРОЙКИ НАВЫКОВ"),
]:
    rows = con.execute("""
        SELECT
            SUBSTR(feature, LENGTH(?) + 1) AS name,
            cnt
        FROM all_counts
        WHERE STARTS_WITH(feature, ?)
        ORDER BY cnt DESC, name
        LIMIT 10
    """, [prefix, prefix]).fetchall()

    print(f"\nТоп-10: {title}")
    for name, cnt in rows:
        print(f"  {name}: {cnt}")

# Сохраняем полный результат в формате MapReduce
result_path = "duckdb_result.tsv"

con.execute(f"""
COPY (
    SELECT feature, cnt
    FROM all_counts
    ORDER BY feature
)
TO '{result_path}'
(HEADER false, DELIMITER '\\t');
""")

print(f"\nПолный результат сохранён: {result_path}")
print(f"Время DuckDB: {time.perf_counter() - t0:.2f} сек")

con.close()
