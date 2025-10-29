# subscriber.py
import os
import asyncio
import socketio

URL = os.getenv("WS_URL", "wss://workshop.grizzlyfrog.com")

sio = socketio.AsyncClient(reconnection=True)


@sio.event
async def connect():
    print("✅ connected", sio.sid)


@sio.event
async def disconnect():
    print("🔌 disconnected")


@sio.on("newSubmit")
async def on_new_submit(data):
    print("📩 newSubmit:", data)


async def main():
    try:
        await sio.connect(URL, transports=["websocket"])
        await sio.wait()  # blocks until disconnected / Ctrl+C
    except (asyncio.CancelledError, KeyboardInterrupt):
        # swallow Ctrl+C / task-cancel and exit cleanly
        pass
    finally:
        # ensure we disconnect without raising
        try:
            if sio.connected:
                await sio.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
