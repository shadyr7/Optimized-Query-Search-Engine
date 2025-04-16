import sys
import time
from concurrent.futures import ThreadPoolExecutor

DATA_FILE = "taxi-trips-data.json"
CHUNK_SIZE = 50000
MAX_WORKERS = 12  

# ---------------- JSON LINE PARSER ----------------
def simple_json_parser(line):
    data = {}
    key = ''
    value = ''
    in_key = False
    in_value = False
    is_string = False
    i = 0

    while i < len(line):
        char = line[i]

        if char == '"':
            if not in_key and not in_value:
                in_key = True
                key = ''
            elif in_key and not in_value:
                in_key = False
                i += 1
                while i < len(line) and line[i] in [' ', ':']:
                    i += 1
                if i < len(line) and line[i] == '"':
                    is_string = True
                    in_value = True
                    value = ''
                else:
                    is_string = False
                    in_value = True
                    value = ''
                    continue
            elif in_value and is_string:
                in_value = False
                data[key] = value
        elif in_key:
            key += char
        elif in_value:
            if is_string:
                value += char
            else:
                if char in [',', '}']:
                    try:
                        if '.' in value:
                            data[key] = float(value)
                        else:
                            data[key] = int(value)
                    except:
                        data[key] = value
                    in_value = False
                else:
                    value += char
        i += 1
    return data

# ---------------- QUERY 1 ----------------
def process_chunk_query1(lines):
    return sum(1 for line in lines if line.strip())

def query1():
    with open(DATA_FILE, 'r', encoding='utf-8') as f, ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures, chunk = [], []
        for line in f:
            chunk.append(line)
            if len(chunk) >= CHUNK_SIZE:
                futures.append(executor.submit(process_chunk_query1, chunk))
                chunk = []
        if chunk:
            futures.append(executor.submit(process_chunk_query1, chunk))
        total = sum(f.result() for f in futures)

    print("Query 1: Total Number of Trips")
    print(f"Total Trips: {total}\nApproximate: {total / 1_000_000:.2f} million")

# ---------------- QUERY 2 ----------------
def process_chunk_query2(lines):
    stats = {}
    for line in lines:
        try:
            row = simple_json_parser(line)
            if float(row.get("trip_distance", 0)) <= 5:
                continue
            ptype = int(row.get("payment_type", -1))
            fare = float(row.get("fare_amount", 0))
            tip = float(row.get("tip_amount", 0))
            if ptype not in stats:
                stats[ptype] = [0, 0.0, 0.0]
            stats[ptype][0] += 1
            stats[ptype][1] += fare
            stats[ptype][2] += tip
        except:
            continue
    return stats

def merge_stats(global_stats, partial):
    for k, v in partial.items():
        if k not in global_stats:
            global_stats[k] = [0, 0.0, 0.0]
        global_stats[k][0] += v[0]
        global_stats[k][1] += v[1]
        global_stats[k][2] += v[2]

def query2():
    stats = {}
    with open(DATA_FILE, 'r', encoding='utf-8') as f, ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures, chunk = [], []
        for line in f:
            chunk.append(line)
            if len(chunk) >= CHUNK_SIZE:
                futures.append(executor.submit(process_chunk_query2, chunk))
                chunk = []
        if chunk:
            futures.append(executor.submit(process_chunk_query2, chunk))
        for f in futures:
            merge_stats(stats, f.result())

    print("payment_type,num_trips,avg_fare,total_tip")
    for p, v in sorted(stats.items()):
        avg = v[1]/v[0] if v[0] else 0
        print(f"{p},{v[0]},{avg:.2f},{v[2]:.2f}")

# ---------------- QUERY 3 ----------------
def process_chunk_query3(lines):
    stats = {}
    for line in lines:
        try:
            row = simple_json_parser(line)
            if row.get("store_and_fwd_flag") != 'Y': continue
            dt = row.get("tpep_pickup_datetime", "")[:10]
            if not dt.startswith("2024-01"): continue
            vendor = int(row.get("VendorID", -1))
            passengers = float(row.get("passenger_count", 0))
            if vendor not in stats:
                stats[vendor] = [0, 0.0]
            stats[vendor][0] += 1
            stats[vendor][1] += passengers
        except:
            continue
    return stats

def query3():
    stats = {}
    with open(DATA_FILE, 'r', encoding='utf-8') as f, ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures, chunk = [], []
        for line in f:
            chunk.append(line)
            if len(chunk) >= CHUNK_SIZE:
                futures.append(executor.submit(process_chunk_query3, chunk))
                chunk = []
        if chunk:
            futures.append(executor.submit(process_chunk_query3, chunk))
        for f in futures:
            merge = f.result()
            for k, v in merge.items():
                if k not in stats:
                    stats[k] = [0, 0.0]
                stats[k][0] += v[0]
                stats[k][1] += v[1]

    print("VendorID,trips,avg_passengers")
    for v, s in sorted(stats.items()):
        avg = s[1]/s[0] if s[0] else 0
        print(f"{v},{s[0]},{avg:.6f}")

# ---------------- QUERY 4 ----------------
def process_chunk_query4(lines):
    stats = {}
    for line in lines:
        try:
            row = simple_json_parser(line)
            dt = row.get("tpep_pickup_datetime", "")[:10]
            if not dt.startswith("2024-01"): continue
            p = float(row.get("passenger_count", 0))
            d = float(row.get("trip_distance", 0))
            f = float(row.get("fare_amount", 0))
            t = float(row.get("tip_amount", 0))
            if dt not in stats:
                stats[dt] = [0, 0.0, 0.0, 0.0, 0.0]
            stats[dt][0] += 1
            stats[dt][1] += p
            stats[dt][2] += d
            stats[dt][3] += f
            stats[dt][4] += t
        except:
            continue
    return stats

def query4():
    stats = {}
    with open(DATA_FILE, 'r', encoding='utf-8') as f, ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures, chunk = [], []
        for line in f:
            chunk.append(line)
            if len(chunk) >= CHUNK_SIZE:
                futures.append(executor.submit(process_chunk_query4, chunk))
                chunk = []
        if chunk:
            futures.append(executor.submit(process_chunk_query4, chunk))
        for f in futures:
            merge = f.result()
            for k, v in merge.items():
                if k not in stats:
                    stats[k] = [0, 0.0, 0.0, 0.0, 0.0]
                for i in range(5):
                    stats[k][i] += v[i]

    print("trip_date,total_trips,avg_passengers,avg_distance,avg_fare,total_tip")
    for d in sorted(stats):
        s = stats[d]
        n = s[0]
        print(f"{d},{n},{s[1]/n:.2f},{s[2]/n:.2f},{s[3]/n:.2f},{s[4]:.2f}")

def run_query(query_name):
    start_time = time.time()

    if query_name == "query1":
        query1()
    elif query_name == "query2":
        query2()
    elif query_name == "query3":
        query3()
    elif query_name == "query4":
        query4()
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
