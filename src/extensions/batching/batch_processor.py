import aiohttp
import asyncio
from aiohttp import ClientSession
from .models import BatchResponse, BatchRequest
from typing import List

class BatchProcessor:

    def __init__(self, host: str):
        self._host = host

    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.close()

    def _construct_url(self, relative_url: str) -> str:
        return f"{self._host}{relative_url}"

    async def _handle_request(self, request: BatchRequest) -> BatchResponse:
        for i in range(5):
            try:
                request_url = self._construct_url(request.url)
                response = await self._session.request(
                    method=request.method,
                    url=request_url,
                    headers=request.headers,
                    json=request.body
                )

                if response.status != 200:
                    print(request_url, request.body)

                try:
                    return BatchResponse(
                    id=request.id,
                    status=response.status,
                    headers=response.headers,
                    body = await response.json()
                )
                except Exception as e:
                    print(request.body)
                    print(e)
            except Exception as e:
                print(e)

        return BatchResponse(
            id=request.id,
            status=500,
            body = None)


    async def bound_fetch(self, sem, req):
        async with sem:
            return await self._handle_request(req)

    async def process(self, batch_requests: List[BatchRequest]) -> List[BatchResponse]:

        tasks = []
        sem = asyncio.Semaphore(1000)

        for req in batch_requests:
            task = asyncio.ensure_future(self.bound_fetch(sem, req))
            tasks.append(task)

        return await asyncio.gather(*tasks)