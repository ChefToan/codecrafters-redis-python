import socket  # noqa: F401
import asyncio
import time

# In-memory data store for key-value pairs with expiry
# Structure: {key: {"value": value, "expiry": timestamp_or_None}}
data_store = {}

async def parse_resp(reader):
    """Parse a RESP message from the reader and return a list of strings."""
    line = await reader.readline()
    if not line:
        return None
    if line.startswith(b'*'):
        # Array
        num_elements = int(line[1:].strip())
        elements = []
        for _ in range(num_elements):
            bulk_len_line = await reader.readline()
            if not bulk_len_line.startswith(b'$'):
                return None
            bulk_len = int(bulk_len_line[1:].strip())
            bulk_data = await reader.readexactly(bulk_len)
            await reader.readexactly(2)  # Discard \r\n
            elements.append(bulk_data.decode())
        return elements
    else:
        # Inline command (e.g., "PING\r\n")
        return line.decode().strip().split()

async def handle_client(reader, writer):
    client_address = writer.get_extra_info('peername')
    print(f"New connection from {client_address}")

    try:
        while True:
            cmd = await parse_resp(reader)
            if not cmd:
                break
            if len(cmd) == 0:
                continue
            command = cmd[0].upper()
            if command == "PING":
                writer.write(b"+PONG\r\n")
            elif command == "ECHO" and len(cmd) == 2:
                arg = cmd[1]
                resp = f"${len(arg)}\r\n{arg}\r\n".encode()
                writer.write(resp)
            elif command == "SET" and len(cmd) >= 3:
                key = cmd[1]
                value = cmd[2]
                expiry_time = None

                # Check for PX argument (expiry in milliseconds)
                if len(cmd) >= 5:
                    for i in range(3, len(cmd) - 1):
                        if cmd[i].upper() == "PX":
                            try:
                                expiry_ms = int(cmd[i + 1])
                                expiry_time = time.time() + (expiry_ms / 1000.0)
                                break
                            except (ValueError, IndexError):
                                pass

                data_store[key] = {"value": value, "expiry": expiry_time}
                writer.write(b"+OK\r\n")
            elif command == "GET" and len(cmd) == 2:
                key = cmd[1]
                if key in data_store:
                    entry = data_store[key]
                    # Check if key has expired
                    if entry["expiry"] is not None and time.time() > entry["expiry"]:
                        # Key has expired, remove it and return null
                        del data_store[key]
                        writer.write(b"$-1\r\n")
                    else:
                        # Key is valid, return the value
                        value = entry["value"]
                        resp = f"${len(value)}\r\n{value}\r\n".encode()
                        writer.write(resp)
                else:
                    # Key doesn't exist - return null bulk string
                    writer.write(b"$-1\r\n")
            else:
                writer.write(b"-ERR unknown command\r\n")
            await writer.drain()
    except Exception as e:
        print(f"Error handling connection: {e}")
    finally:
        writer.close()
        await writer.wait_closed()

async def main():
    print("Logs from your program will appear here!")
    server = await asyncio.start_server(
        handle_client,
        "localhost",
        6379,
        reuse_port=True
    )
    print("Redis server started on localhost:6379")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())