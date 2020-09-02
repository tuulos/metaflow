import time
import asyncio
import json

from aiohttp import web

from metaflow.client.cache.cache_async_client import CacheAsyncClient
from search_action import Search

async def start_cache(app):
    actions = [Search]
    cache = app['cache'] = CacheAsyncClient('cache_data',
                                            actions,
                                            max_size=600000)
    await cache.start()
    app['cache'] = cache

async def stop_cache(app):
    await app['cache'].stop()

async def search(request):
    step_id = request.match_info['step_id']
    artifact = request.query['key']
    value = request.query['value']
    print('Request: %s %s=%s' % (step_id, artifact, value))
    res = await request.app['cache'].Search(step_id, artifact, value)
    print('cache keys', res.key_paths)

    ws = web.WebSocketResponse()
    await ws.prepare(request)
    async for event in res.stream():
        print('event', event)
        await ws.send_str(json.dumps(event))

    return ws

app = web.Application()
app.add_routes([web.get(r'/search/{step_id:.+}', search)])
app.on_startup.append(start_cache)
app.on_cleanup.append(stop_cache)
web.run_app(app, port=7777)
