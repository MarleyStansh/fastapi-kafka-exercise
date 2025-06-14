from fastapi import APIRouter
from schemas import Message
from kafka_pc import send, consume


router = APIRouter()


@router.post("/create_message/")
async def create_and_send_message(message: Message):
    await send(message)
