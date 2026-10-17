"""Этап 5. Влияние навыков на зарплату (пары и тройки)."""
import os
import time
import duckdb
from config import ENDPOINT, ACCESS_KEY, SECRET_KEY, BUCKET, OBJECT_KEY

HERE = os.path.dirname(os.path.abspath(__file__))
MIN_PAIRS, MIN_TRIPLES, TOP_N = 30, 20, 10

SOURCE = f"read_csv_auto('s3://{BUCKET}/{OBJECT_KEY}', delim='\\t', header=true)"


def setup():
    endpoint = ENDPOINT.replace("http://", "")
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""CREATE SECRET r (TYPE s3, KEY_ID '{ACCESS_KEY}',
        SECRET '{SECRET_KEY}', ENDPOINT '{endpoint}',
        URL_STYLE 'path', USE_SSL false);""")
    return con

# аналогично предыдущим этапам добавим условие предотвращающее тавтологию
# уберем специальность из рассматриваемых скилов 
def base_cte():
    """Общий CTE: одна строка = (vacancy_id, salary, skill)."""
    return f"""
    WITH skills AS (
        SELECT DISTINCT
            CAST(ROW_NUMBER() OVER () AS INTEGER) AS vid,
            salary_from AS salary,
            LOWER(TRIM(unnest(string_split(key_skills, ';')))) AS skill
        FROM {SOURCE}
        WHERE salary_from IS NOT NULL 
          AND currency = 'RUR' 
          AND key_skills IS NOT NULL
    ),
    clean_skills AS (
        SELECT * FROM skills
        WHERE skill NOT IN ('c', 'c++', 'c/c++', 'cpp', 'си', 'си++')  -- <--- ДОБАВИЛИ
    )"""


def query_pairs(con, min_count):
    return con.execute(base_cte() + f"""
        SELECT a.skill, b.skill, COUNT(*), median(a.salary)
        FROM clean_skills a JOIN skills b ON a.vid = b.vid AND a.skill < b.skill
        GROUP BY 1, 2 HAVING COUNT(*) >= {min_count}
    """).fetchall()


def query_triples(con, min_count):
    return con.execute(base_cte() + f"""
        SELECT a.skill, b.skill, c.skill, COUNT(*), median(a.salary)
        FROM clean_skills a
        JOIN clean_skills b ON a.vid = b.vid AND a.skill < b.skill
        JOIN clean_skills c ON a.vid = c.vid AND b.skill < c.skill
        GROUP BY 1, 2, 3 HAVING COUNT(*) >= {min_count}
    """).fetchall()


def show(items, title, base_med, top=TOP_N, asc=False):
    print(f"\n  {title}\n")
    for *skills, cnt, med in sorted(items, key=lambda x: x[-1], reverse=not asc)[:top]:
        diff = med - base_med
        print(f"  {' & '.join(skills):<50} n={cnt:<5} med={med:>9.0f} ({diff:+.0f}, {100*diff/base_med:+.1f}%)")


def save(items, path, base_med):
    with open(path, "w", encoding="utf-8") as f:
        f.write("skills,count,median,diff,pct\n")
        for *skills, cnt, med in sorted(items, key=lambda x: -x[-1]):
            diff = med - base_med
            f.write(f'"{" & ".join(skills)}",{cnt},{med:.0f},{diff:.0f},{100*diff/base_med:.1f}\n')


def main():
    t0 = time.time()
    con = setup()

    n, base_med = con.execute(f"""
        SELECT COUNT(*), median(salary_from) FROM {SOURCE}
        WHERE salary_from IS NOT NULL AND currency = 'RUR' AND key_skills IS NOT NULL
    """).fetchone()
    print(f"С/С++ разработчиков с зарплатой RUR: {n}\n Общая медиана: {base_med:.0f} руб")

    print("\n ПАРЫ:")
    pairs = query_pairs(con, MIN_PAIRS)
    print(f"   Найдено: {len(pairs)}")

    print(" ТРОЙКИ")
    triples = query_triples(con, MIN_TRIPLES)
    print(f"   Найдено: {len(triples)}")

    show(pairs, "ТОП-10 ПАР: ПОВЫШАЮТ", base_med)
    show(pairs, "ТОП-10 ПАР: ПОНИЖАЮТ", base_med, asc=True)
    show(triples, "ТОП-10 ТРОЕК: ПОВЫШАЮТ", base_med)
    show(triples, "ТОП-10 ТРОЕК: ПОНИЖАЮТ", base_med, asc=True)

    save(pairs, os.path.join(HERE, "salary_pairs.csv"), base_med)
    save(triples, os.path.join(HERE, "salary_triples.csv"), base_med)
    print(f"\n Время: {time.time() - t0:.1f} сек")


if __name__ == "__main__":
    main()