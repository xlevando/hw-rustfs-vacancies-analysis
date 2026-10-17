import tracemalloc
import time

def measure(func, name):
    tracemalloc.start()
    t0 = time.time()
    func()
    elapsed = time.time() - t0
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f"{name}: {elapsed:.2f} сек, пик памяти {peak / 1024**2:.1f} МБ")

# Замер для MapReduce
from step_mapreduce import main as mr_main
measure(mr_main, "MapReduce")

# Замер для DuckDB
from duckdb_analysis import main as ddb_main
measure(ddb_main, "DuckDB")