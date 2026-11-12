import argparse
import glob
import heapq
import json
import os
import subprocess
import sys
import time
import zlib
from collections import defaultdict
from datetime import datetime

from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError, NodeExistsError, NoNodeError

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
WORK = os.path.join(HERE, "mr_work_coord")
LOCAL_CSV = os.path.join(PROJECT_ROOT, "cpp_vacancies.csv")
MAPPER = [sys.executable, os.path.join(PROJECT_ROOT, "mapper.py")]
REDUCER = [sys.executable, os.path.join(PROJECT_ROOT, "reducer_stats.py")]
REDUCERS = 8              # совпадает с config.py из ДЗ №3
SPLIT = 4 * 1024 * 1024   # 4 МБ на чанк, как в config.py


def log(msg, name):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"{ts} [{name}] {msg}", flush=True)


def read_header(path):
    with open(path, "rb") as f:
        return f.readline()


def read_split(path, start, end):
    with open(path, "rb") as f:
        f.seek(start)
        if start > 0:
            f.readline()
        position = f.tell()
        if position >= end:
            return b""
        return f.read(end - position) + f.readline()


def process_chunk(chunk_index, path, start, end, header):
    """Обрабатывает один чанк: mapper → shuffle по редьюсерам."""
    data = read_split(path, start, end)
    if not data:
        return 0
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
            f"Mapper {chunk_index} failed: "
            f"{mapper.stderr.decode('utf-8', 'replace')}"
        )

    buckets = [[] for _ in range(REDUCERS)]
    for line in mapper.stdout.splitlines(keepends=True):
        key, sep, _ = line.partition(b"\t")
        if not sep:
            raise ValueError(f"bad mapper line: {line!r}")
        buckets[zlib.crc32(key) % REDUCERS].append(line)

    emitted = 0
    for rid, bucket in enumerate(buckets):
        if not bucket:
            continue
        bucket.sort()
        out_path = os.path.join(WORK, f"map{chunk_index:05d}-r{rid:02d}")
        with open(out_path, "wb") as f:
            f.writelines(bucket)
        emitted += len(bucket)
    return emitted


def reduce_all():
    """Финальный reduce по всем файлам WORK."""
    for rid in range(REDUCERS):
        pattern = os.path.join(WORK, f"map*-r{rid:02d}")
        paths = sorted(glob.glob(pattern))
        out_path = os.path.join(WORK, f"part-r{rid:05d}")
        with open(out_path, "wb") as out:
            with __import__("contextlib").ExitStack() as stack:
                runs = [stack.enter_context(open(p, "rb")) for p in paths]
                reducer = subprocess.Popen(
                    REDUCER,
                    stdin=subprocess.PIPE,
                    stdout=out,
                )
                try:
                    for line in heapq.merge(*runs):
                        reducer.stdin.write(line)
                    reducer.stdin.close()
                    rc = reducer.wait()
                except Exception:
                    reducer.kill()
                    reducer.wait()
                    raise
                if rc != 0:
                    raise RuntimeError(f"Reducer {rid} failed with {rc}")


def collect_results():
    results = defaultdict(int)
    for path in sorted(glob.glob(os.path.join(WORK, "part-*"))):
        with open(path, encoding="utf-8") as f:
            for line in f:
                key, sep, val = line.rstrip("\n").partition("\t")
                if not sep:
                    raise ValueError(f"bad reduce line: {line!r}")
                results[key] += int(val)

    out_tsv = os.path.join(HERE, "logs", "coordinator_result.tsv")
    os.makedirs(os.path.dirname(out_tsv), exist_ok=True)
    with open(out_tsv, "w", encoding="utf-8", newline="") as f:
        for key in sorted(results):
            f.write(f"{key}\t{results[key]}\n")

    return dict(results)


def save_checkpoint(zk, node, value, name):
    """Сохраняет чекпоинт , при потере сессии только логирует, но не падает."""
    try:
        try:
            zk.set(node, json.dumps(value).encode())
        except NoNodeError:
            zk.create(node, json.dumps(value).encode())
        return True
    except Exception as e:
        log(f"Чекпоинт не сохранён (сессия потеряна?): {e}", name)
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--zk", default="127.0.0.1:2181")
    ap.add_argument("--fencing", type=int, default=0, choices=[0, 1])
    ap.add_argument("--name", default="coord")
    ap.add_argument("--delay", type=float, default=0.0,
                    help="Задержка после каждого чанка, сек (для демонстрации STOP/CONT)")
    args = ap.parse_args()

    if not os.path.exists(LOCAL_CSV):
        print(f"CSV не найден: {LOCAL_CSV}", file=sys.stderr)
        sys.exit(1)

    zk = KazooClient(hosts=args.zk, timeout=10.0)
    zk.start()
    log(f"Подключён к {args.zk}, session_timeout=10s", args.name)

    zk.ensure_path("/coordinator")
    leader_path = "/coordinator/leader"
    result_node = "/coordinator/result"
    checkpoint_node = "/coordinator/checkpoint"

    # борьба за лидерство
    is_leader = False
    try:
        zk.create(leader_path, value=args.name.encode(), ephemeral=True)
        is_leader = True
        log("Стал ЛИДЕРОМ (создан эфемерный узел)", args.name)
    except NodeExistsError:
        log("Лидер уже есть, я запасной", args.name)

    leader_lost = {"flag": False}
    if not is_leader:
        @zk.DataWatch(leader_path)
        def watch(data, stat, event):
            if event is not None and event.type == "DELETED":
                leader_lost["flag"] = True
                log("Лидер пропал — пытаюсь стать лидером", args.name)

    while not is_leader:
        if leader_lost["flag"]:
            try:
                zk.create(leader_path, value=args.name.encode(), ephemeral=True)
                is_leader = True
                log("Стал ЛИДЕРОМ после падения предыдущего", args.name)
            except NodeExistsError:
                leader_lost["flag"] = False
        time.sleep(0.3)

    # лидер: читаем версию узла результата
    fencing_version = -1
    try:
        _data, stat = zk.get(result_node)
        fencing_version = stat.version
        log(f"Версия узла результата на старте: {fencing_version}", args.name)
    except NoNodeError:
        log("Узла результата нет, fencing_version = -1", args.name)
    except Exception as e:
        log(f"Не удалось прочитать версию на старте: {e}", args.name)

    # расчёт с чекпоинтом 
    os.makedirs(WORK, exist_ok=True)

    start_chunk = 0
    try:
        cp_data, _ = zk.get(checkpoint_node)
        start_chunk = int(json.loads(cp_data).get("last_chunk", 0))
        log(f"Чекпоинт из ZooKeeper: последний чанк = {start_chunk}", args.name)
    except NoNodeError:
        log("Чекпоинта нет, начинаем с чанка 0", args.name)
    except Exception as e:
        log(f"Не удалось прочитать чекпоинт: {e}", args.name)

    size = os.path.getsize(LOCAL_CSV)
    header = read_header(LOCAL_CSV)
    all_starts = list(range(0, size, SPLIT))
    total = len(all_starts)
    log(f"Всего чанков: {total}, начиная с {start_chunk}", args.name)

    for i in range(start_chunk, total):
        start = all_starts[i]
        end = min(start + SPLIT, size)
        emitted = process_chunk(i, LOCAL_CSV, start, end, header)

        saved = save_checkpoint(
            zk, checkpoint_node,
            {"last_chunk": i + 1, "by": args.name},
            args.name,
        )
        if saved:
            log(
                f"Чанк {i + 1}/{total} обработан ({emitted} записей), "
                f"чекпоинт сохранён",
                args.name,
            )
        else:
            log(
                f"Чанк {i + 1}/{total} обработан ({emitted} записей), "
                f"чекпоинт НЕ сохранён",
                args.name,
            )

        if args.delay > 0:
            time.sleep(args.delay)

    # reduce и результат
    log("Все чанки обработаны, запускаю reduce...", args.name)
    reduce_all()
    results = collect_results()

    pairs = sum(v for k, v in results.items() if k.startswith("pair:"))
    triples = sum(v for k, v in results.items() if k.startswith("triple:"))
    payload = json.dumps({
        "pairs_total": pairs,
        "triples_total": triples,
        "keys": len(results),
        "by": args.name,
    }).encode()

    # запись результата
    try:
        if args.fencing == 1:
            # Используем версию, прочитанную В НАЧАЛЕ работы лидера.
            if fencing_version == -1:
                # Узла не было на старте — пытаемся создать.
                try:
                    zk.create(result_node, payload)
                    log("Записал результат (fencing=1, создан узел)", args.name)
                except NodeExistsError:
                    log("BadVersionError: узел создан другим лидером (fencing=1)", args.name)
            else:
                zk.set(result_node, payload, version=fencing_version)
                log(
                    f"Записал результат (fencing=1, version={fencing_version})",
                    args.name,
                )
        else:
            # Без fencing — просто перезаписываем без проверки версии.
            try:
                zk.set(result_node, payload)
                log("Записал результат (fencing=0, без проверки версии)", args.name)
            except NoNodeError:
                zk.create(result_node, payload)
                log("Записал результат (fencing=0, создан узел)", args.name)
    except BadVersionError:
        log("BadVersionError: запись отклонена (устаревшая версия)", args.name)
    except Exception as e:
        log(f"Ошибка записи результата: {e}", args.name)

    log("Ожидаю завершения (Ctrl+C)...", args.name)
    try:
        while zk.connected:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        if zk.connected:
            log("Завершение по Ctrl+C", args.name)
        else:
            log("Сессия потеряна — завершаюсь", args.name)
        zk.stop()


if __name__ == "__main__":
    main()