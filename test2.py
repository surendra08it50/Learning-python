import asyncio

async def m1(x):
    return x

print(asyncio.run(m1(3)))    