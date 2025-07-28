import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        connection, _ = server_socket.accept()
        try:
            while True:
                data = connection.recv(1024)
                if not data:
                    break
                print(f"Received: {data.decode()}")

                # Parse RESP2 format - check if it's a PING command
                # PING can come as either "PING\r\n" or "*1\r\n$4\r\nPING\r\n"
                data_str = data.decode()
                if "PING" in data_str:
                    # Respond to PING with PONG
                    connection.sendall(b"+PONG\r\n")

        except Exception as e:
            print(f"Error handling connection: {e}")
        finally:
            connection.close()

if __name__ == "__main__":
    main()
