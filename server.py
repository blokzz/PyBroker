import asyncio
import struct

header_format = "!IB"
header_size = struct.calcsize(header_format)

async def handle_client(writer , reader):
    address = writer.get_extra_info('peername')
    print(f"ESTABLISHED CONNECTION WITH {address}")
    try:
        while True:
            header = await reader.read(header_size)
            mes_length ,command= struct.unpack(header_format ,header)

    except asyncio.IncompleteReadError:
        print(f"CLIENT {address} DISCONNECTED")
    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        writer.close()
        await writer.wait_closed()
        print(f"CONNECTION WITH {address} CLOSED")