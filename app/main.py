import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.routers import routes
from app.scheduler import scheduled_task


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(scheduled_task())
    yield


app = FastAPI(lifespan=lifespan)

app.include_router(routes.router)