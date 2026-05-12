import os
import sys
import asyncio
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from dotenv import load_dotenv
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(base_dir, '.env')
load_dotenv(dotenv_path)
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from app.database import pool
from app.api.routers.db import db
from app.api.routers.users import users
from app.api.routers.chat import chat
from app.config import logger

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. При старте сервера открываем пул соединений
    await pool.open()
    logger.info("Пул соединений с БД открыт.")
    
    yield # Здесь сервер работает и принимает запросы
    
    # 2. При выключении сервера аккуратно закрываем соединения
    await pool.close()
    logger.info("Пул соединений с БД закрыт.")

app = FastAPI(title="AI Analyst API", lifespan=lifespan)   

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(users.router)
app.include_router(db.router)
app.include_router(chat.router)

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        #reload=True
    )