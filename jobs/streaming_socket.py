import json
import socket
import time
import logging
import pandas as pd


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    level=logging.DEBUG
)


def send_data_over_socket(
    file_path: str,
    host: str = "127.0.0.1",
    port: str = 9999,
    chunk_size: int = 2
):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen(1)
    logging.info(f"Listened for connection on {host}:{port}.")

    last_sent_index = 0
    while True:
        conn, addr = s.accept()
        logging.info(f"Connection from {addr}.")

        try:
            with open(file_path, "r") as file:
                for _ in range(last_sent_index):
                    next(file)

                records = []
                for line in file:
                    records.append(json.loads(line))
                    if (len(records)) == chunk_size:
                        chunk = pd.DataFrame(data=records)
                        logging.info(f"Chunk: {chunk}")
                        for record in chunk.to_dict(orient="records"):
                            serialized_data = json.dumps(record).encode("utf-8")
                            conn.send(serialized_data + b"\n")
                            time.sleep(3)
                            last_sent_index += 1
                        records = []
        except (BrokenPipeError, ConnectionResetError):
            logging.error("Client disconnected.")
        finally:
            conn.close()
            logging.info("Connection closed.")


if __name__ == "__main__":
    send_data_over_socket(
        file_path="datasets/yelp_academic_dataset_review.json",
        host="127.0.0.1",
        port=9999,
        chunk_size=2
    )