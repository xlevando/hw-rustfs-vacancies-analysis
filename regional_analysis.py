"""Этап 6. Сравнение МСК/СПб/НН"""
import os
import time

import duckdb
from config import ACCESS_KEY, BUCKET, ENDPOINT, OBJECT_KEY, SECRET_KEY

HERE = os.path.dirname(os.path.abspath(__file__))
REGIONS = ["Москва", "Санкт-Петербург", "Нижний Новгород"]
MIN_PAIR_COUNT = 20


# Аналогично: исключаем сам язык из списка навыков
STOP_SKILLS = "('c', 'c++', 'c/c++', 'cpp', 'си', 'си++')"


SOURCE = f"read_csv_auto('s3://{BUCKET}/{OBJECT_KEY}', delim='\\t', header=true)"


def setup():
    ep = ENDPOINT.replace("http://", "")
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""CREATE SECRET r (TYPE s3, KEY_ID '{ACCESS_KEY}',
        SECRET '{SECRET_KEY}', ENDPOINT '{ep}',
        URL_STYLE 'path', USE_SSL false);""")
    return con


def get_stats(con):
    regions_sql = ", ".join(f"'{r}'" for r in REGIONS)
    return con.execute(f"""
        SELECT area, COUNT(*) AS total,
               COUNT(salary_from) FILTER (WHERE currency='RUR') AS ws,
               median(salary_from) FILTER (WHERE currency='RUR') AS med
        FROM {SOURCE}
        WHERE area IN ({regions_sql})
        GROUP BY area ORDER BY total DESC
    """).fetchall()


def get_skills(con, region, n=5):
    return con.execute(f"""
        SELECT LOWER(TRIM(skill)), COUNT(*) FROM (
            SELECT unnest(string_split(key_skills, ';')) AS skill
            FROM {SOURCE}
            WHERE area = '{region}' AND key_skills IS NOT NULL
        ) 
        WHERE LOWER(TRIM(skill)) NOT IN {STOP_SKILLS}
        GROUP BY 1 ORDER BY 2 DESC LIMIT {n}
    """).fetchall()


def get_pairs(con, region, n=3):
    return con.execute(f"""
        WITH s AS (
            SELECT DISTINCT CAST(ROW_NUMBER() OVER () AS INTEGER) AS vid,
                            LOWER(TRIM(unnest(string_split(key_skills, ';')))) AS skill
            FROM {SOURCE}
            WHERE area = '{region}' AND key_skills IS NOT NULL
        )
        SELECT a.skill, b.skill, COUNT(*) FROM s a
        JOIN s b ON a.vid = b.vid AND a.skill < b.skill
        WHERE a.skill NOT IN {STOP_SKILLS} AND b.skill NOT IN {STOP_SKILLS}
        GROUP BY 1, 2 HAVING COUNT(*) >= {MIN_PAIR_COUNT}
        ORDER BY 3 DESC LIMIT {n}
    """).fetchall()


def main():
    t0 = time.time()
    con = setup()

    print(f"{'Регион':<20} {'Всего':>8} {'С зп':>8} {'Медиана':>10}\n")
    for area, total, ws, med in get_stats(con):
        med_str = f"{med:.0f}" if med else "—"
        print(f"{area:<20} {total:>8} {ws:>8} {med_str:>10}")

    for region in REGIONS:
        print(f"\n  {region.upper()}\n")
        print("  Топ-5 навыков:")
        for s, c in get_skills(con, region):
            print(f"    {s:<40} {c}")
        print("  Топ-3 пары:")
        for s1, s2, c in get_pairs(con, region):
            print(f"    {s1} & {s2:<35} {c}")

    print(f"\n Время: {time.time() - t0:.1f} сек")


if __name__ == "__main__":
    main()