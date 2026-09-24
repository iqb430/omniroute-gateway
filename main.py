from fastapi import FastAPI, Depends, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
import asyncio
import json
import redis.asyncio as redis
from contextlib import asynccontextmanager
import httpx
import time
import logging
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gateway")

# Prometheus Metrics
http_requests_total = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["tenant", "model", "status_code"]
)
http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["tenant", "model"]
)
active_streaming_connections = Gauge(
    "active_streaming_connections",
    "Current in-flight streams",
    ["tenant", "model"]
)
tokens_streamed_total = Counter(
    "tokens_streamed_total",
    "Total output tokens streamed",
    ["tenant", "model"]
)

redis_client = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    yield
    await redis_client.close()

app = FastAPI(title="OmniRoute Gateway - Streaming Toll Gate", lifespan=lifespan)

@app.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

async def verify_and_rate_limit(request: Request):
    api_key = request.headers.get("Authorization")
    if not api_key or not api_key.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid API key")
    
    key = api_key.split(" ")[1]
    
    # Rate limit check (Fixed window, 5 requests per minute)
    redis_key = f"rate_limit:{key}"
    
    try:
        current_count = await redis_client.incr(redis_key)
        if current_count == 1:
            await redis_client.expire(redis_key, 60)
            
        if current_count > 5:
            raise HTTPException(status_code=429, detail="Rate limit exceeded")
    except redis.ConnectionError:
        # Fallback or error handling if Redis is down
        raise HTTPException(status_code=500, detail="Internal Server Error: Redis connection failed")
    
    return {"tenant_id": "tenant_123", "tier": "premium"}

async def mock_fallback_stream(prompt: str, tenant_id: str, model: str):
    logger.info("Using mock fallback stream")
    fallback_message = "This is a fallback response due to primary upstream failure."
    tokens = fallback_message.split(" ")
    for i, token in enumerate(tokens):
        content = token + (" " if i < len(tokens) - 1 else "")
        chunk = {
            "id": f"chatcmpl-fallback-{int(time.time())}",
            "object": "chat.completion.chunk",
            "choices": [{"delta": {"content": content}, "index": 0, "finish_reason": None}]
        }
        tokens_streamed_total.labels(tenant=tenant_id, model=model).inc()
        yield f"data: {json.dumps(chunk)}\n\n"
        await asyncio.sleep(0.05)
    
    final_chunk = {
        "id": f"chatcmpl-fallback-{int(time.time())}",
        "object": "chat.completion.chunk",
        "choices": [{"delta": {"content": ""}, "index": 0, "finish_reason": "stop"}]
    }
    yield f"data: {json.dumps(final_chunk)}\n\n"

async def try_stream(url: str, model: str, prompt: str, tenant_id: str):
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": True
    }
    async with httpx.AsyncClient() as client:
        request = client.build_request("POST", url, json=payload)
        response = await client.send(request, stream=True, timeout=3.0)
        response.raise_for_status()
        
        async for line in response.aiter_lines():
            if line:
                try:
                    data = json.loads(line)
                    content = data.get("response", "")
                    is_done = data.get("done", False)
                    
                    tokens_streamed_total.labels(tenant=tenant_id, model=model).inc()
                        
                    chunk = {
                        "id": f"chatcmpl-{int(time.time())}",
                        "object": "chat.completion.chunk",
                        "choices": [{"delta": {"content": content}, "index": 0, "finish_reason": "stop" if is_done else None}]
                    }
                    yield f"data: {json.dumps(chunk)}\n\n"
                except json.JSONDecodeError:
                    continue

async def stream_with_fallback(prompt: str, original_model: str, tenant_id: str):
    primary_url = "http://localhost:11434/api/generate"
    primary_model = original_model
    fallback_model = "fallback_mock"

    start_time = time.time()
    active_streaming_connections.labels(tenant=tenant_id, model=primary_model).inc()
    
    success = False
    try:
        async for chunk in try_stream(primary_url, primary_model, prompt, tenant_id):
            yield chunk
        success = True
        http_requests_total.labels(tenant=tenant_id, model=primary_model, status_code="200").inc()
    except Exception as e:
        logger.warning(f"Primary model {primary_model} failed: {e}. Routing to fallback...")
        http_requests_total.labels(tenant=tenant_id, model=primary_model, status_code="500").inc()
    finally:
        active_streaming_connections.labels(tenant=tenant_id, model=primary_model).dec()

    if not success:
        active_streaming_connections.labels(tenant=tenant_id, model=fallback_model).inc()
        try:
            async for chunk in mock_fallback_stream(prompt, tenant_id, fallback_model):
                yield chunk
            http_requests_total.labels(tenant=tenant_id, model=fallback_model, status_code="200").inc()
        except Exception as fallback_e:
            logger.error(f"Fallback also failed: {fallback_e}")
            http_requests_total.labels(tenant=tenant_id, model=fallback_model, status_code="500").inc()
            error_chunk = {
                "id": "chatcmpl-error",
                "object": "chat.completion.chunk",
                "choices": [{"delta": {"content": f"\n[Error: {str(fallback_e)}]"}, "index": 0, "finish_reason": "stop"}]
            }
            yield f"data: {json.dumps(error_chunk)}\n\n"
        finally:
            active_streaming_connections.labels(tenant=tenant_id, model=fallback_model).dec()
            
    duration = time.time() - start_time
    http_request_duration_seconds.labels(tenant=tenant_id, model=original_model).observe(duration)
    yield "data: [DONE]\n\n"

@app.post("/v1/chat/completions")
async def chat_completions(
    request: Request, 
    tenant: dict = Depends(verify_and_rate_limit)
):
    """
    The 'Toll Gate' endpoint with fallback routing and metrics.
    """
    try:
        body = await request.json()
        messages = body.get("messages", [])
        model = body.get("model", "llama3")
        
        prompt = messages[-1]["content"] if messages else ""
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    return StreamingResponse(
        stream_with_fallback(prompt, model, tenant["tenant_id"]),
        media_type="text/event-stream"
    )
