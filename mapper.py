import sys
import csv
from itertools import combinations

from config import CSV_SEP, CSV_ENCODING

# во избежание тавтологии уберем из скиллов основную специализацию - сами ЯП
STOP_SKILLS = {"c", "c++", "c/c++", "cpp", "си", "си++"}

def parse_skills(s):
    if not s or not s.strip():
        return []
    # Разбиваем, чистим, приводим к нижнему регистру
    raw_skills = [x.strip().lower() for x in s.split(";") if x.strip()]
    # Отбрасываем стоп-слова
    return [x for x in raw_skills if x not in STOP_SKILLS]


def emit(key):
    sys.stdout.write(f"{key}\t1\n")


def main():
    # Корректно читаем utf 8
    sys.stdin.reconfigure(encoding=CSV_ENCODING, newline="")

    reader = csv.DictReader(
        sys.stdin,
        delimiter=CSV_SEP,
    )

    if not reader.fieldnames or "key_skills" not in reader.fieldnames:
        raise ValueError(
            "В CSV не найдена колонка 'key_skills'. "
            f"Полученные колонки: {reader.fieldnames}"
        )

    for row in reader:
        skills = sorted(set(parse_skills(row.get("key_skills", ""))))

        if not skills:
            continue

        for skill in skills:
            emit(f"skill:{skill}")

        for combo in combinations(skills, 2):
            emit(f"pair:{'&'.join(combo)}")

        for combo in combinations(skills, 3):
            emit(f"triple:{'&'.join(combo)}")


if __name__ == "__main__":
    main()