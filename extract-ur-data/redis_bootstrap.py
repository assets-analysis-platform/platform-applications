import psycopg2
import redis


def load_data_from_postgres_to_redis():
    # Connect to PostgreSQL
    postgres_conn = psycopg2.connect(
        database="your_database",
        user="your_user",
        password="your_password",
        host="your_host",
        port="your_port"
    )

    # Create a cursor
    postgres_cursor = postgres_conn.cursor()

    # Execute a query to fetch data from PostgreSQL
    postgres_cursor.execute("SELECT id, name, value FROM your_table")

    # Fetch all rows
    rows = postgres_cursor.fetchall()

    # Connect to Redis
    redis_conn = redis.StrictRedis(
        host="your_redis_host",
        port=your_redis_port,
        password="your_redis_password",
        decode_responses=True
    )

    # Iterate through the rows and store data in Redis
    for row in rows:
        key = f"your_key_prefix:{row[0]}"  # Adjust key format as needed
        value = {
            "name": row[1],
            "value": row[2]
        }
        redis_conn.hmset(key, value)

    # Close connections
    postgres_cursor.close()
    postgres_conn.close()


if __name__ == "__main__":
    load_data_from_postgres_to_redis()