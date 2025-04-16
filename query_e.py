import sys
import time
from multiprocessing import Pool, cpu_count

DATA_FILE = "taxi-trips-data.json"
CHUNK_SIZE = 50000
MAX_WORKERS = cpu_count()

# schema specific json parser
def fast_json_parser(line):
    parts = line.strip().strip('{}').split(',')
    data = {}
    for part in parts:
        if ':' not in part:
            continue
        k, v = part.split(':', 1)
        key = k.strip().strip('"')
        val = v.strip().strip('"')

        if key in ["VendorID", "passenger_count", "payment_type"]:
            try:
                data[key] = int(val)
            except:
                data[key] = 0
        elif key in ["trip_distance", "fare_amount", "tip_amount"]:
            try:
                data[key] = float(val)
            except:
                data[key] = 0.0
        else:
            data[key] = val
    return data

#chunk loader
def load_chunks():
    with open(DATA_FILE, 'r', encoding='utf-8') as f:
        chunk = []
        for line in f:
            chunk.append(line)
            if len(chunk) >= CHUNK_SIZE:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

#query1
def process_chunk_query1(lines):
    return sum(1 for line in lines if line.strip())

def query1():
    with Pool(processes=MAX_WORKERS) as pool:
        results = pool.map(process_chunk_query1, load_chunks())
    total = sum(results)
    print("Query 1: Total Number of Trips")
    print(f"Total Trips: {total}\nApproximate: {total / 1_000_000:.2f} million")

#query2
def process_chunk_query2(lines):
    stats = {}
    for line in lines:
        try:
            row = fast_json_parser(line)
            if row.get("trip_distance", 0) <= 5:
                continue
            ptype = row.get("payment_type", -1)
            fare = row.get("fare_amount", 0.0)
            tip = row.get("tip_amount", 0.0)
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
    with Pool(processes=MAX_WORKERS) as pool:
        results = pool.map(process_chunk_query2, load_chunks())
    stats = {}
    for r in results:
        merge_stats(stats, r)
    print("payment_type,num_trips,avg_fare,total_tip")
    for p, v in sorted(stats.items()):
        avg = v[1]/v[0] if v[0] else 0
        print(f"{p},{v[0]},{avg:.2f},{v[2]:.2f}")

#query3
def process_chunk_query3(lines):
    stats = {}
    for line in lines:
        try:
            row = fast_json_parser(line)
            if row.get("store_and_fwd_flag") != 'Y': continue
            dt = row.get("tpep_pickup_datetime", "")[:10]
            if not dt.startswith("2024-01"): continue
            vendor = row.get("VendorID", -1)
            passengers = row.get("passenger_count", 0.0)
            if vendor not in stats:
                stats[vendor] = [0, 0.0]
            stats[vendor][0] += 1
            stats[vendor][1] += passengers
        except:
            continue
    return stats

def query3():
    with Pool(processes=MAX_WORKERS) as pool:
        results = pool.map(process_chunk_query3, load_chunks())
    stats = {}
    for r in results:
        for k, v in r.items():
            if k not in stats:
                stats[k] = [0, 0.0]
            stats[k][0] += v[0]
            stats[k][1] += v[1]
    print("VendorID,trips,avg_passengers")
    for v, s in sorted(stats.items()):
        avg = s[1]/s[0] if s[0] else 0
        print(f"{v},{s[0]},{avg:.6f}")

#query4
def process_chunk_query4(lines):
    stats = {}
    for line in lines:
        try:
            row = fast_json_parser(line)
            dt = row.get("tpep_pickup_datetime", "")[:10]
            if not dt.startswith("2024-01"): continue
            p = row.get("passenger_count", 0.0)
            d = row.get("trip_distance", 0.0)
            f = row.get("fare_amount", 0.0)
            t = row.get("tip_amount", 0.0)
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
    with Pool(processes=MAX_WORKERS) as pool:
        results = pool.map(process_chunk_query4, load_chunks())
    stats = {}
    for r in results:
        for k, v in r.items():
            if k not in stats:
                stats[k] = [0, 0.0, 0.0, 0.0, 0.0]
            for i in range(5):
                stats[k][i] += v[i]
    print("trip_date,total_trips,avg_passengers,avg_distance,avg_fare,total_tip")
    for d in sorted(stats):
        s = stats[d]
        n = s[0]
        print(f"{d},{n},{s[1]/n:.2f},{s[2]/n:.2f},{s[3]/n:.2f},{s[4]:.2f}")

# CLI
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
        return
    print(f"\nExecution Time: {time.time() - start_time:.2f} seconds")

def main():
    if len(sys.argv) < 2:
        print("Usage: python query_engine.py <query1|query2|query3|query4>")
        sys.exit(1)
    run_query(sys.argv[1])

if __name__ == "__main__":
    main()
