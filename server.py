import asyncio
import struct
import os

class LogManager:
    def __init__(self , filename="server.log"):
        self.filename = filename
        self.file = open(self.filename, "ab", buffering=0)
    def write(self , message):
        current_offset = self.file.tell()
        msg_len = len(message)
        entry = struct.pack("!I" ,msg_len) + message
        self.file.write(entry)
        return current_offset
    def read(self , offset):
        try:
            with open(self.filename , "rb") as f:
                f.seek(offset)
                length_data = f.read(4)
                if not length_data or len(length_data) < 4:
                    return None
                msg_len = struct.unpack("!I", length_data)[0]
                payload = f.read(msg_len)
                next_offset = offset + 4 + msg_len
                return payload , next_offset
        except FileNotFoundError:
            return None

header_format = "!IB"
header_size = struct.calcsize(header_format)
log_manager = LogManager()


async def handle_client(reader , writer):
    address = writer.get_extra_info('peername')
    print(f"ESTABLISHED CONNECTION WITH {address}")
    loop = asyncio.get_running_loop()
    try:
        while True:
            header = await reader.readexactly(header_size)
            mes_length ,command= struct.unpack(header_format ,header)

            payload = await reader.readexactly(mes_length)
            res = b""
            if command == 1:
                print(f"[{address}] PUBLISH: {len(payload)} BYTES")
                offset = await loop.run_in_executor(None,log_manager.write , payload)
                res = b"OK" + struct.pack("!Q", offset)
            elif command ==2:
                if len(payload)!=8 :
                    res = b"ER_BAD_PAYLOAD"
                else:
                    requested_offset = struct.unpack("!Q", payload)[0]
                    print(f"[{address}] FETCH FROM OFFSET: {requested_offset}")
                    result = await loop.run_in_executor(None, log_manager.read_message, requested_offset)
                    
                    if result:
                        msg_data, next_offset = result
                        res = b"OK" + struct.pack("!Q", next_offset) + msg_data
                    else:
                        res= b"NF"
            else:
                print(f"UNKNOWN COMMAND: {command}")
                res = b"ER"


            print(f"RECEIVED COMMAND FROM {address}:  {command} , PAYLOAD: {payload[:20]}...")
            writer.write(struct.pack("!I" ,len(res)) + res)
            await writer.drain()


    except asyncio.IncompleteReadError:
        print(f"CLIENT {address} DISCONNECTED")
    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        writer.close()
        await writer.wait_closed()
        print(f"CONNECTION WITH {address} CLOSED")

async def main():
    server = await asyncio.start_server(handle_client , "127.0.0.1" , 8888)
    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f"SERVER IS RUNNING ON {addrs}")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())