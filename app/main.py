import socket  # noqa: F401
import asyncio

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