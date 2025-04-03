import abc
import asyncio
import httpx
import random


class ResultsObserver(abc.ABC):
    @abc.abstractmethod
    def observe(self, data: bytes) -> None: ...


async def do_reliable_request(url: str, observer: ResultsObserver) -> None:
    """
    Одна из главных проблем распределённых систем - это ненадёжность связи.

    Ваша задача заключается в том, чтобы таким образом исправить
    этот код, чтобы он умел переживать возвраты ошибок и
    таймауты со стороны сервера, гарантируя успешный запрос
    (в реальной жизни такая гарантия невозможна,
    но мы чуть упростим себе задачу).

    Все успешно полученные результаты
    должны регистрироваться с помощью обсёрвера.
    """

    async with httpx.AsyncClient() as client:
        max_attempts = 20
        for attempt in range(max_attempts):
            try:
                response = await client.get(url, timeout=30.0)
                response.raise_for_status()
                data = response.content

                observer.observe(data)
                return
            except (httpx.HTTPError, httpx.TimeoutException) as e:
                if attempt == max_attempts - 1:
                    raise e

                delay = min(2 ** attempt, 60) * (0.5 + 0.5 * random.random())
                await asyncio.sleep(delay)
