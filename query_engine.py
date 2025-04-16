import sys
import time
from concurrent.futures import ThreadPoolExecutor
from json_reader import simple_json_parser

DATA_FILE = "taxi-trips-data.json"
CHUNK_SIZE = 50000
MAX_WORKERS = 8  # adjust based on CPU

# ---------------- QUERY 1 ----------------

def process_chunk_query1(lines):
    count = 0
    for line in lines:
        if not line.strip():
            continue
        try:
            simple_json_parser(line)
            count += 1
        except:
            continue
    return count

def query1():
    total = 0
    with open(DATA_FILE, 'r', encoding='utf-8') as f, ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        chunk = []

        for line in f:
            chunk.append(line)
            if len(chunk) >= CHUNK_SIZE:
                futures.append(executor.submit(process_chunk_query1, chunk))
                chunk = []
        if chunk:
            futures.append(executor.submit(process_chunk_query1, chunk))

        for future in futures:
            total += future.result()

    print("Query 1: Total Number of Trips")
    print(f"Total Trips: {total}")
    print(f"Approximate: {total / 1_000_000:.2f} million")

# ---------------- QUERY 2 ----------------

def process_chunk_query2(lines):
    partial_stats = {}
    for line in lines:
        if not line.strip():
            continue
        try:
            row = simple_json_parser(line.strip())
            if float(row.get("trip_distance", 0)) <= 5:
                continue

            ptype = int(row.get("payment_type", -1))
            fare = float(row.get("fare_amount", 0))
            tip = float(row.get("tip_amount", 0))

            if ptype not in partial_stats:
                partial_stats[ptype] = [0, 0.0, 0.0]

            partial_stats[ptype][0] += 1
            partial_stats[ptype][1] += fare
            partial_stats[ptype][2] += tip

        except:
            continue
    return partial_stats

def merge_stats(global_stats, partial_stats):
    for ptype, values in partial_stats.items():
        if ptype not in global_stats:
            global_stats[ptype] = [0, 0.0, 0.0]
        global_stats[ptype][0] += values[0]
        global_stats[ptype][1] += values[1]
        global_stats[ptype][2] += values[2]

def query2():
    stats = {}
    with open(DATA_FILE, 'r', encoding='utf-8') as f, ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        chunk = []

        for line in f:
            chunk.append(line)
            if len(chunk) >= CHUNK_SIZE:
                futures.append(executor.submit(process_chunk_query2, chunk))
                chunk = []
        if chunk:
            futures.append(executor.submit(process_chunk_query2, chunk))

        for future in futures:
            merge_stats(stats, future.result())

    print("payment_type,num_trips,avg_fare,total_tip")
    for ptype in sorted(stats):
        count, fare_sum, tip_sum = stats[ptype]
        avg_fare = fare_sum / count if count else 0
        print(f"{ptype},{count},{avg_fare:.2f},{tip_sum:.2f}")

# ---------------- Runner ----------------

def run_query(query_name):
    start_time = time.time()

    if query_name == "query1":
        query1()
    elif query_name == "query2":
        query2()
    else:
        print(f"Unknown query: {query_name}")

    elapsed = time.time() - start_time
    print(f"\nExecution Time: {elapsed:.2f} seconds")

def main():
    if len(sys.argv) < 2:
        print("Usage: python query_engine.py <query1 | query2>")
        sys.exit(1)

    query_name = sys.argv[1]
    run_query(query_name)

if __name__ == "__main__":
    main()
