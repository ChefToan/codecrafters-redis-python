import socket  # noqa: F401
import asyncio


async def handle_client(reader, writer):
    """Handle a single client connection asynchronously"""
    client_address = writer.get_extra_info('peername')
    print(f"New connection from {client_address}")

    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break
            print(f"Received: {data.decode()}")

            # Parse RESP2 format - check if it's a PING command
            # PING can come as either "PING\r\n" or "*1\r\n$4\r\nPING\r\n"
            data_str = data.decode()
            if "PING" in data_str:
                # Respond to PING with PONG
                writer.write(b"+PONG\r\n")
                await writer.drain()  # Ensure data is sent

    except Exception as e:
        print(f"Error handling connection: {e}")
    finally:
        writer.close()
        await writer.wait_closed()


async def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Start the asyncio server
    server = await asyncio.start_server(
        handle_client,
        "localhost",
        6379,
        reuse_port=True
    )

    print("Redis server started on localhost:6379")

    # Keep the server running
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
