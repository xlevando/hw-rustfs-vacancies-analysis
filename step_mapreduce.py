import glob
import heapq
import multiprocessing
import os
import shutil
import subprocess
import sys
import time
import zlib
from collections import defaultdict

import boto3
from botocore.config import Config
from config import (
    ACCESS_KEY,
    BUCKET,
    ENDPOINT,
    OBJECT_KEY,
    REDUCERS,
    SECRET_KEY,
    SPLIT,
)

HERE = os.path.dirname(os.path.abspath(__file__))
WORK = os.path.join(HERE, "mr_work")

LOCAL_CSV = os.path.join(HERE, OBJECT_KEY)

MAPPER = [sys.executable, os.path.join(HERE, "mapper.py")]
REDUCER = [sys.executable, os.path.join(HERE, "reducer_stats.py")]

WORKERS = os.cpu_count() or 4


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
        region_name="us-east-1",
    )


def download_from_rustfs(bucket, key, local_path):
    # Не используем старую локальную копию:
    # каждый запуск получает актуальный объект из RustFS.
    print(f"Скачиваю s3://{bucket}/{key}...")

    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    s3 = get_s3_client()
    s3.download_file(bucket, key, local_path)

    print(
        f"Файл загружен: {local_path} "
        f"({os.path.getsize(local_path) / 1024**2:.1f} МБ)"
    )

    return local_path


def read_header(path):
    # Заголовок добавляем каждому чанку, чтобы DictReader всегда видел правильные имена столбцов
    with open(path, "rb") as f:
        header = f.readline()

    if not header:
        raise ValueError("CSV-файл пустой: заголовок отсутствует")

    return header


def read_split(path, start, end):
    """Читает диапазон файла, расширяя его до границ строк"""
    with open(path, "rb") as f:
        f.seek(start)

        if start > 0:
            # пропускаем неполную строку на границе чанка
            f.readline()

        position = f.tell()

        if position >= end:
            return b""

        # читаем до end и дочитываем последнюю строку целиком
        return f.read(end - position) + f.readline()


def map_task(task):
    index, path, start, end, header = task

    data = read_split(path, start, end)

    if not data:
        return 0

    # 1й чанк уже содержит исходный заголовок, для остальных - добавляем вручную
    if start > 0:
        data = header + data

    mapper = subprocess.run(
        MAPPER,
        input=data,
        capture_output=True,
        env=dict(os.environ),
    )

    if mapper.returncode != 0:
        raise RuntimeError(
            f"Mapper {index} завершился с ошибкой:\n"
            f"{mapper.stderr.decode('utf-8', 'replace')}"
        )

    # распределяем записи по редьюсерам
    buckets = [[] for _ in range(REDUCERS)]

    for line in mapper.stdout.splitlines(keepends=True):
        key, separator, _ = line.partition(b"\t")

        if not separator:
            raise ValueError(
                f"Mapper создал некорректную запись: {line!r}"
            )

        reducer_id = zlib.crc32(key) % REDUCERS
        buckets[reducer_id].append(line)

    emitted_records = 0

    for reducer_id, bucket in enumerate(buckets):
        if not bucket:
            continue

        # сортировка нужна для корректной работы редьюсера
        bucket.sort()

        output_path = os.path.join(
            WORK,
            f"map{index:04d}-r{reducer_id:02d}",
        )

        with open(output_path, "wb") as f:
            f.writelines(bucket)

        emitted_records += len(bucket)

    return emitted_records


def reduce_task(reducer_id):
    pattern = os.path.join(
        WORK,
        f"map*-r{reducer_id:02d}",
    )

    paths = sorted(glob.glob(pattern))
    output_path = os.path.join(
        WORK,
        f"part-r{reducer_id:05d}",
    )

    # каждый входной файл уже отсортирован
    with open(output_path, "wb") as out: 
        with __import__("contextlib").ExitStack() as stack:
            runs = [
                stack.enter_context(open(path, "rb"))
                for path in paths
            ]

            reducer = subprocess.Popen(
                REDUCER,
                stdin=subprocess.PIPE,
                stdout=out,
            )

            try:
                for line in heapq.merge(*runs):
                    reducer.stdin.write(line)

                reducer.stdin.close()
                return_code = reducer.wait()

            except Exception:
                reducer.kill()
                reducer.wait()
                raise

            if return_code != 0:
                raise RuntimeError(
                    f"Reducer {reducer_id} завершился с кодом "
                    f"{return_code}"
                )


def main():
    download_from_rustfs(BUCKET, OBJECT_KEY, LOCAL_CSV)

    size = os.path.getsize(LOCAL_CSV)

    if size == 0:
        raise ValueError("Исходный файл пустой")

    header = read_header(LOCAL_CSV)

    shutil.rmtree(WORK, ignore_errors=True)
    os.makedirs(WORK)

    tasks = [
        (i, LOCAL_CSV, start, min(start + SPLIT, size), header)
        for i, start in enumerate(range(0, size, SPLIT))
    ]

    workers = min(WORKERS, max(1, len(tasks)))

    print(
        f"Чанков: {len(tasks)}, "
        f"редьюсеров: {REDUCERS}, "
        f"воркеров: {workers}"
    )

    start_time = time.perf_counter()

    with multiprocessing.Pool(workers) as pool:
        produced = pool.map(map_task, tasks, chunksize=1)
        pool.map(reduce_task, range(REDUCERS), chunksize=1)

    elapsed = time.perf_counter() - start_time

    print(f"Время MapReduce: {elapsed:.2f} сек")
    print(f"Промежуточных записей: {sum(produced):,}")

    results = defaultdict(int)

    for path in sorted(glob.glob(os.path.join(WORK, "part-*"))):
        with open(path, encoding="utf-8") as f:
            for line in f:
                key, separator, value = line.rstrip("\n").partition("\t")

                if not separator:
                    raise ValueError(
                        f"Некорректная итоговая запись: {line!r}"
                    )

                results[key] += int(value)

    for prefix, title in [
        ("skill:", "ОДИНОЧНЫЕ НАВЫКИ"),
        ("pair:", "ПАРЫ НАВЫКОВ"),
        ("triple:", "ТРОЙКИ НАВЫКОВ"),
    ]:
        items = sorted(
            (
                (key[len(prefix):], count)
                for key, count in results.items()
                if key.startswith(prefix)
            ),
            key=lambda item: (-item[1], item[0]),
        )

        print(f"\nТоп-10: {title}")

        for key, count in items[:10]:
            print(f"  {key}: {count}")

    result_path = os.path.join(HERE, "mapreduce_result.tsv")

    with open(result_path, "w", encoding="utf-8", newline="") as f:
        for key, value in sorted(results.items()):
            f.write(f"{key}\t{value}\n")

    print(f"\nРезультат сохранён: {result_path}")


if __name__ == "__main__":
    main()
