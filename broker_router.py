# broker_router.py

import asyncio
from executors.dhan_executor import dhan_order
from executors.angel_executor import angel_order
from executors.zebu_executer import zebu_order
from executors.upstox_executor import upstox_order
from executors.ant_executer import ant_order
    
async def route_signal(signal, users):
    tasks = []
    print("USERS:")
    print("USERS: ",users)

    if "dhan" in users:
        tasks.append(execute(users["dhan"], signal, dhan_order))

    if "angelone" in users:
        tasks.append(execute(users["angelone"], signal, angel_order))

    if "upstox" in users:
        tasks.append(execute(users["upstox"], signal, upstox_order))

    if "zebumynt" in users:
        tasks.append(execute(users["zebumynt"], signal, zebu_order))

    if "aliceblue" in users:
        tasks.append(execute(users["aliceblue"], signal, ant_order))

    await asyncio.gather(*tasks)


async def execute(user_list, signal, broker_func):
    await asyncio.gather(*[
        broker_func(user, signal) for user in user_list
    ])