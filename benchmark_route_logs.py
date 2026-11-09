import csv
import os 
import random 
import time 
from datetime import datetime 
from pathlib import Path

import psycopg 

DSN = os.getenv(
    "PG_DSN",
    "dbname=aerologistics_benchmark host=localhost port=5432"
)

ROW_COUNT = 500_000
UPDATE_COUNT = 100_000
RANDOM_SEED = 42

RESULTS_FILE = Path("benchmark_results.csv")

STATUSES = [
    "received",
    "in_transit",
    "delayed",
    "out_for_delivery",
    "delivered",
]

LOCATIONS = [
    "Seattle Alpha",
    "Dallas Central",
    "New York East ",
    "Chicago Prime",
    "Los Angeles Hub",
]

INDEX_NAMES = [
    "idx_route_logs_tracking",
    "idx_route_logs_route",
    "idx_route_logs_delivery",
    "idx_route_logs_vehicle",
    "idx_route_logs_driver",
    "idx_route_logs_status",
    "idx_route_logs_tracking_cover",
]

def generate_route_logs():
    rng = random.Random(RANDOM_SEED)
    rows = []
    initial_time = datetime(2026, 1, 1, 12, 0, 0)

    for route_id in range(1, ROW_COUNT + 1):
        rows.append((
            route_id,
            route_id,
            rng.randint(1, 5),
            rng.randint(1, 500),
            rng.randint(1, 4),
            round(rng.uniform(10.00, 500.00), 2),
            rng.randint(0, 120),
            rng.choice(STATUSES),
            f"TRK-{route_id:08d}",
            rng.choice(LOCATIONS),
            initial_time,
        ))

    return rows

def generate_update_targets():
    rng = random.Random(RANDOM_SEED)
    selected_route_ids = rng.sample(
        range(1, ROW_COUNT + 1),
        UPDATE_COUNT,
    )

    targets = []
    for route_id in selected_route_ids:
        targets.append((
            route_id,
            rng.choice(["in_transit", "delayed", "out_for_delivery"]),
            rng.choice(LOCATIONS),
        ))

    return targets


def drop_benchmark_indexes(cur):
    """
    Remove all indexes created by this benchmark.
    """
    for index_name in INDEX_NAMES:
        cur.execute( 
            f"DROP INDEX IF EXISTS {index_name};"

        )

def create_state_indexes(cur, state):
    if state == "A":
        return

    indexes = {
        "B": [
            "CREATE INDEX idx_route_logs_tracking ON route_logs (tracking_number)",
            "CREATE INDEX idx_route_logs_route ON route_logs (route_id)",
            "CREATE INDEX idx_route_logs_delivery ON route_logs (delivery_id)",
            "CREATE INDEX idx_route_logs_vehicle ON route_logs (vehicle_id)",
            "CREATE INDEX idx_route_logs_status ON route_logs (status)",
        ],
        "C": [
            """
            CREATE INDEX idx_route_logs_tracking_cover
            ON route_logs (tracking_number, route_id)
            INCLUDE (status, current_location, last_updated)
            """,
        ],
    }

    if state not in indexes:
        raise ValueError(f"Unknown benchmark state: {state}")

    for statement in indexes[state]:
        cur.execute(statement)



def clear_route_logs(cur):
    drop_benchmark_indexes(cur)
    cur.execute("TRUNCATE TABLE route_logs;")


def insert_route_logs(cur, rows):
    with cur.copy("""
        COPY route_logs (
            route_id, delivery_id, vehicle_id, driver_id, weather_id,
            route_distance, delay_minutes, status, tracking_number,
            current_location, last_updated
        ) FROM STDIN
    """) as copy:
        for row in rows:
            copy.write_row(row)
            
def create_update_targets(cur, targets):
    cur.execute("""
        CREATE TEMP TABLE update_targets (
            route_id BIGINT NOT NULL,
            new_status VARCHAR(30) NOT NULL,
            new_location VARCHAR(80) NOT NULL
        );
    """)

    with cur.copy("""
        COPY update_targets (route_id, new_status, new_location)
        FROM STDIN
    """) as copy:
        for target in targets:
            copy.write_row(target)

    cur.execute("""
        CREATE INDEX update_targets_route_id_idx
        ON update_targets (route_id);
    """)
    cur.execute("ANALYZE update_targets;")

def run_update_storm(cur):
    """Update the selected route records."""
    cur.execute("""
        UPDATE route_logs AS r
        SET
            status = u.new_status,
            current_location = u.new_location,
            last_updated = CURRENT_TIMESTAMP
        FROM update_targets AS u
        WHERE r.route_id = u.route_id;
    """)

    if cur.rowcount != UPDATE_COUNT:
        raise RuntimeError(
            f"Expected to update {UPDATE_COUNT} rows; updated {cur.rowcount}."
        )

def get_relation_size_mb(cur):
    """Measure the table and index size before VACUUM."""
    cur.execute("SELECT pg_total_relation_size('route_logs');")
    size_bytes = cur.fetchone()[0]
    return size_bytes / (1024 * 1024)

def vacuum_and_analyze(conn):
    """
    VACUUM updates the Visibility Map and removes reclaimable
    dead tuples before the read test.

    VACUUM must run outside a normal transaction.
    """
    conn.commit()
    conn.autocommit = True

    with conn.cursor() as cur: 
        cur.execute("""
        VACUUM (ANALYZE) route_logs;
        """)

        conn.autocommit = False
    
def collect_plan_nodes(plan):
    """
    Recursively collect all EXPLAIN plan nodes.
    """

    nodes = [plan]

    for child in plan.get("Plans", []):
        nodes.extend(
            collect_plan_nodes(child)
        )

    return nodes

def run_tracking_query(cur):
    """
    Run the primary tracking query using EXPLAIN ANALYZE BUFFERS.
    """

    tracking_number = "TRK-00000001"

    cur.execute("""
    EXPLAIN( 
    ANALYZE,
    BUFFERS,
    FORMAT JSON
    )
    SELECT 
    route_id, 
    tracking_number, 
    status,
    current_location,
    last_updated
    FROM route_logs
    WHERE tracking_number = %s;
    """, (tracking_number,))

    explain_output = cur.fetchone()[0][0]
    root_plan = explain_output["Plan"]

    nodes = collect_plan_nodes(root_plan)

    node_types = [
        node.get("Node Type", "")
        for node in nodes
    ]
    shared_hits = sum(
        node.get("Shared Hit Blocks", 0)
        for node in nodes 
    )

    shared_reads = sum(
        node.get("Shared Read Blocks", 0)
        for node in nodes 
    )

    heap_fetches = sum(
        node.get("Heap Fetches", 0)
        for node in nodes
        if node.get("Node Type") == "Index Only Scan"
    )

    return {
        "read_latency_ms": explain_output["Execution Time"],
        "plan": " -> ".join(node_types),
        "index_only_scan": "Index Only Scan" in node_types,
        "heap_fetches": heap_fetches,
        "shared_hit_blocks": root_plan.get("Shared Hit Blocks", 0),
        "shared_read_blocks": root_plan.get("Shared Read Blocks", 0),
    }

def run_benchmark_state(state, rows, update_targets):
    """
    Run one complete benchmark state.
    """
    print(f"\nRunning State {state}...")

    with psycopg.connect(DSN) as conn:

        with conn.cursor() as cur: 
            clear_route_logs(cur)
            create_state_indexes(cur, state)
            conn.commit()

            create_update_targets(
                cur,
                update_targets
            )
            conn.commit()

            insert_start = time.perf_counter()

            insert_route_logs(cur,rows)
            conn.commit()

            insert_ms = (
                time.perf_counter() - insert_start
            ) * 1000

            update_start = time.perf_counter()

            run_update_storm(cur)
            conn.commit()

            update_ms = (
                time.perf_counter() - update_start
            ) * 1000

            total_write_ms = insert_ms + update_ms

            size_after_update_mb = (
                get_relation_size_mb(cur)
            )

            vacuum_and_analyze(conn)

            tracking_result = run_tracking_query(cur)

            result = { 
                "state": state, 
                "insert_ms": round(insert_ms, 2),
                "update_ms": round(update_ms, 2),
                "total_write_ms": round( 
                    total_write_ms,
                    2
                ),
                "size_after_update_mb": round(
                    size_after_update_mb,
                    2
                ),
                "read_latency_ms": round(
                    tracking_result[
                        "read_latency_ms"
                    ],
                    2
                ),
                "plan": tracking_result["plan"],
                "index_only_scan": tracking_result[
                    "index_only_scan"
                ],
                "heap_fetches": tracking_result[
                    "heap_fetches"
                ],
                "shared_hit_blocks": tracking_result[
                    "shared_hit_blocks"
                ],
                "shared_read_blocks": tracking_result[
                    "shared_read_blocks"
                ],
            }

            print(result)
            return result 
        
def save_results(results):
    """
    Save benchmark results for the paper's table or chart.
    """

    fieldnames = [
        "state",
        "insert_ms",
        "update_ms",
        "total_write_ms",
        "size_after_update_mb",
        "read_latency_ms",
        "plan",
        "index_only_scan",
        "heap_fetches",
        "shared_hit_blocks",
        "shared_read_blocks",
    ]

    with RESULTS_FILE.open(
        "w",
        newline=""
    ) as file:

       writer = csv.DictWriter(
        file,
        fieldnames=fieldnames
       )

       writer.writeheader()
       writer.writerows(results)

def main():
    print("Generating 500,000 Route_Logs rows...")
    rows = generate_route_logs()

    print("Generating 100,000 Update Storm rows...")
    update_targets = generate_update_targets()

    results = []
    for state in ["A", "B", "C"]:
        results.append(
            run_benchmark_state(state, rows, update_targets)
        )

    save_results(results)
    print(f"Results saved to: {RESULTS_FILE}")


if __name__ == "__main__":
    main()