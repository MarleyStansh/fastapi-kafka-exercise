from contextlib import asynccontextmanager
import uvicorn
import asyncio
import kafka_pc
from fastapi import FastAPI
from config import settings
from views import router as msg_router
from dotenv import load_dotenv


load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(kafka_pc.consume())
    yield


app = FastAPI(lifespan=lifespan)
app.include_router(msg_router)


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.run.host,
        port=settings.run.port,
        reload=True,
    )
