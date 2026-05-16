import asyncio
from strategy_cache import strategy_cache
from broker_router import route_signal

async def process_signal(signal):
    print("📡 Signal:", signal)

    users = signal.get("users")

    if not users:
        print("⚠️ No users found in signal")
        return

    await route_signal(signal, users)
