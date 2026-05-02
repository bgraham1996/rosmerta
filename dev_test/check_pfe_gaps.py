"""Diagnose the gap in PFE hourly price data."""
import psycopg2
from db_config import get_db_config


def main():
    conn = psycopg2.connect(**get_db_config())
    try:
        with conn.cursor() as cur:
            # Resolve ticker → stock_id
            cur.execute("SELECT stock_id, name FROM stocks WHERE symbol = 'PFE'")
            stock_id, name = cur.fetchone()
            print(f"PFE → stock_id={stock_id}, name={name}\n")

            # 1. First and last 20 timestamps
            print("─── First 20 timestamps ───")
            cur.execute(
                """
                SELECT timestamp FROM price_hourly
                WHERE stock_id = %s
                ORDER BY timestamp ASC
                LIMIT 20
                """,
                (stock_id,),
            )
            for (ts,) in cur.fetchall():
                print(f"  {ts}")

            print("\n─── Last 5 timestamps ───")
            cur.execute(
                """
                SELECT timestamp FROM price_hourly
                WHERE stock_id = %s
                ORDER BY timestamp DESC
                LIMIT 5
                """,
                (stock_id,),
            )
            for (ts,) in cur.fetchall():
                print(f"  {ts}")

            # 2. Distribution of bars per month — find sparse periods
            print("\n─── Bars per month ───")
            cur.execute(
                """
                SELECT date_trunc('month', timestamp) AS month, COUNT(*) AS bars
                FROM price_hourly
                WHERE stock_id = %s
                GROUP BY month
                ORDER BY month ASC
                """,
                (stock_id,),
            )
            for month, bars in cur.fetchall():
                bar = "█" * min(bars // 20, 60)
                print(f"  {month.strftime('%Y-%m')}  {bars:>5}  {bar}")

            # 3. Largest gaps between consecutive bars
            print("\n─── Top 10 largest gaps between consecutive bars ───")
            cur.execute(
                """
                WITH ordered AS (
                    SELECT timestamp,
                           LAG(timestamp) OVER (ORDER BY timestamp) AS prev_ts
                    FROM price_hourly
                    WHERE stock_id = %s
                )
                SELECT prev_ts, timestamp, timestamp - prev_ts AS gap
                FROM ordered
                WHERE prev_ts IS NOT NULL
                ORDER BY gap DESC
                LIMIT 10
                """,
                (stock_id,),
            )
            for prev_ts, ts, gap in cur.fetchall():
                print(f"  {prev_ts}  →  {ts}   (gap: {gap})")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
