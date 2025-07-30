import redis
import argparse
import time

def import_slot_range(redis_client, start_slot, end_slot, batch_size):
    current_start = start_slot

    while current_start <= end_slot:
        current_end = min(current_start + batch_size - 1, end_slot)

        while True:
            try:
                result = redis_client.execute_command("CLUSTER", "MIGRATION", "IMPORT", current_start, current_end)
                print(f"Slots {current_start}-{current_end} imported successfully. ID: {result.decode() if isinstance(result, bytes) else result}")
                break
            except redis.exceptions.ResponseError as e:
                print(f"Slots {current_start}-{current_end}: Error -> {str(e)}. Retrying in 100ms...")
                time.sleep(0.1)
            except Exception as e:
                print(f"Slots {current_start}-{current_end}: Unexpected error -> {str(e)}. Retrying in 100ms...")
                time.sleep(0.1)

        current_start = current_end + 1

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-slot", type=int, required=True)
    parser.add_argument("--end-slot", type=int, required=True)
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=6379)
    args = parser.parse_args()

    r = redis.Redis(host=args.host, port=args.port)
    import_slot_range(r, args.start_slot, args.end_slot, args.batch_size)

if __name__ == "__main__":
    main()
