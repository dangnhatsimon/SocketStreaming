import json
import socket
import time
import logging
import polars as pl


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
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen()
    logging.info(f"Listening for connection on {host}:{port}.")

    last_sent_index = 0
    while True:
        client_socket, client_address = server.accept()
        logging.info(f"Accepted connection from {client_address[0]}:{client_address[1]}.")

        try:
            with open(file_path, "r") as file:
                for _ in range(last_sent_index):
                    next(file)

                records = []
                for line in file:
                    records.append(json.loads(line))
                    if (len(records)) == chunk_size:
                        chunk = pl.DataFrame(data=records)
                        logging.info(f"Sending chunk to {client_address[0]}:{client_address[1]}: \n{chunk}")
                        for record in chunk.to_dict(orient="records"):
                            serialized_data = json.dumps(record).encode("utf-8")
                            client_socket.send(serialized_data + b"\n")
                            last_sent_index += 1
                        records = []
        except (BrokenPipeError, ConnectionResetError) as e:
            logging.error(f"Client {client_address[0]}:{client_address[1]} disconnected: {e}")
        except Exception as e:
            logging.error(f"Error with {client_address[0]}:{client_address[1]}: {e}", exc_info=True)
        finally:
            client_socket.close()
            logging.info(f"Connection with {client_address[0]}:{client_address[1]} closed.")


if __name__ == "__main__":
    # send_data_over_socket(
    #     file_path="./datasets/yelp_academic_dataset_review.json",
    #     host="127.0.0.1",
    #     port=9999,
    #     chunk_size=2
    # )
    send_data_over_socket(
        file_path="./datasets/yelp_academic_dataset_review.json",
        host="spark-master",
        port=9999,
        chunk_size=2
    )
    # docker exec -it socketstreaming-spark-master python jobs/streaming_socket.py
