import asyncio
from signal_manager import process_signal

async def emit_signal(signal):
    await process_signal(signal)