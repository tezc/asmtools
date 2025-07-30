import redis
import argparse
import time

def try_import(redis_client, start_slot, end_slot, node_label, last_error_msg_holder):
    try:
        result = redis_client.execute_command("CLUSTER", "MIGRATION", "IMPORT", start_slot, end_slot)
        print(f"[{node_label}] Slots {start_slot}-{end_slot} imported successfully. ID: {result.decode() if isinstance(result, bytes) else result}")
        last_error_msg_holder[0] = None
        return True
    except redis.exceptions.ResponseError as e:
        msg = str(e)
        if "this node is already the owner of the slot range" in msg:
            if last_error_msg_holder[0] != msg:
                print(f"[{node_label}] Already owns slots {start_slot}-{end_slot}. Trying other node...")
                last_error_msg_holder[0] = msg
            return False
        else:
            if last_error_msg_holder[0] != msg:
                print(f"[{node_label}] Error -> {msg}. Retrying in 100ms...")
                last_error_msg_holder[0] = msg
            time.sleep(0.1)
            return try_import(redis_client, start_slot, end_slot, node_label, last_error_msg_holder)
    except Exception as e:
        msg = str(e)
        if last_error_msg_holder[0] != msg:
            print(f"[{node_label}] Unexpected error -> {msg}. Retrying in 100ms...")
            last_error_msg_holder[0] = msg
        time.sleep(0.1)
        return try_import(redis_client, start_slot, end_slot, node_label, last_error_msg_holder)

def main():
    print("Starting...")

    parser = argparse.ArgumentParser()
    parser.add_argument("--start-slot", type=int, required=True)
    parser.add_argument("--end-slot", type=int, required=True)
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port-a", type=int, required=True)
    parser.add_argument("--port-b", type=int, required=True)
    args = parser.parse_args()

    r_a = redis.Redis(host=args.host, port=args.port_a)
    r_b = redis.Redis(host=args.host, port=args.port_b)

    # Bağlantı test
    try:
        r_a.ping()
        r_b.ping()
        print(f"Connected to ports {args.port_a} and {args.port_b}")
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    print(f"Starting import loop for slots {args.start_slot}-{args.end_slot}...")

    last_error_msg_holder = [None] 

    while True:
        try_import(r_a, args.start_slot, args.end_slot, f"Port {args.port_a}", last_error_msg_holder)
        try_import(r_b, args.start_slot, args.end_slot, f"Port {args.port_b}", last_error_msg_holder)
        time.sleep(0.05)  # wait 50 milliseconds

if __name__ == "__main__":
    main()

