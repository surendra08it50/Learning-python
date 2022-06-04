import asyncio

async def main():
	print("hello")
	await asyncio.sleep(2)
	print("world")
	
asyncio.run(main())

#loop = asyncio.get_event_loop()
#loop.run_until_complete(main())
#loop.close()