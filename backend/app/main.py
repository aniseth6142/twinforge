"""
TwinForge: Lightweight Production-Grade Digital Twin Platform
Main FastAPI Application Entry Point
"""

import logging
from contextlib import asynccontextmanager
from typing import Optional

import structlog
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlmodel import Session, create_engine, SQLModel, select
from sqlalchemy.pool import StaticPool

from app.api import auth, twins, simulations, predictions, data_sources
from app.core.realtime import ConnectionManager
from app.models.user import User
from app.utils.config import get_settings
from app.utils.database import get_session
from app.utils.security import verify_token

# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()
settings = get_settings()

# Global connection manager for WebSockets
connection_manager = ConnectionManager()

# Database setup
if settings.environment == "testing":
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
else:
    engine = create_engine(
        str(settings.database_url),
        echo=settings.debug,
        pool_pre_ping=True,
    )

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown."""
    # Startup
    logger.info("Starting TwinForge...", environment=settings.environment)
    SQLModel.metadata.create_all(engine)
    yield
    # Shutdown
    logger.info("Shutting down TwinForge...")

app = FastAPI(
    title="TwinForge API",
    description="Lightweight Digital Twin Platform",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if settings.debug else settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth.router, prefix="/api/auth", tags=["Authentication"])
app.include_router(twins.router, prefix="/api/twins", tags=["Digital Twins"])
app.include_router(simulations.router, prefix="/api/simulations", tags=["Simulations"])
app.include_router(predictions.router, prefix="/api/predictions", tags=["Predictions"])
app.include_router(data_sources.router, prefix="/api/data-sources", tags=["Data Sources"])

@app.get("/health", tags=["System"])
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "version": "1.0.0",
        "environment": settings.environment,
    }

@app.get("/api/info", tags=["System"])
async def platform_info():
    """Get platform information."""
    return {
        "name": "TwinForge",
        "version": "1.0.0",
        "description": "Lightweight Digital Twin Platform",
        "features": [
            "Graph Modeling",
            "Real-Time Data Ingestion",
            "Physics Simulations",
            "AI/ML Predictions",
            "Anomaly Detection",
            "Parameter Optimization",
            "Interactive Dashboards",
        ],
    }

@app.websocket("/ws/{twin_id}/{user_id}")
async def websocket_endpoint(websocket: WebSocket, twin_id: str, user_id: str, token: str):
    """
    WebSocket endpoint for real-time twin data streaming.
    
    Args:
        websocket: WebSocket connection
        twin_id: Digital Twin ID
        user_id: User ID
        token: JWT token for authentication
    """
    try:
        # Verify token
        username = verify_token(token)
        await connection_manager.connect(f"{twin_id}:{user_id}", websocket)
        logger.info("WebSocket connected", twin_id=twin_id, user_id=user_id)
        
        while True:
            data = await websocket.receive_json()
            await connection_manager.broadcast_to_twin(twin_id, data)
    
    except WebSocketDisconnect:
        connection_manager.disconnect(f"{twin_id}:{user_id}")
        logger.info("WebSocket disconnected", twin_id=twin_id, user_id=user_id)
    
    except HTTPException as e:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        logger.warning("WebSocket auth failed", error=str(e))

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Global exception handler."""
    logger.error("Unhandled exception", exc_info=exc)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
        log_level="info",
    )
