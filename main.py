import os
import sys
import asyncio
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from dotenv import load_dotenv
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(base_dir, '.env')
load_dotenv(dotenv_path)
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from app.database import pool, init_custom_tables
from app.agents.graph import init_graph
from app.api.routes import router 

@asynccontextmanager
async def lifespan(app: FastAPI):
    await pool.open()
    await init_custom_tables()
    
    checkpointer = AsyncPostgresSaver(pool)
    await checkpointer.setup() 
    
    # КЛАДЕМ ГРАФ В STATE:
    app.state.graph = init_graph(checkpointer)
    
    yield
    
    await pool.close()

app = FastAPI(title="AI Analyst API", lifespan=lifespan)   

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        #reload=True
    )