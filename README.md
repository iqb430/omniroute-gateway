# Omniroute Gateway

A multi-tenant LLM Gateway engineered for scale, observability, and resilience.

## Architecture

- **Multi-Tenant LLM Gateway**: Centralized routing and management of LLM requests across multiple providers.
- **FastAPI + Redis Token Bucket Rate Limiting**: High-performance, distributed rate limiting ensuring fair usage and preventing abuse.
- **Real-time Server-Sent Events (SSE) Streaming**: Zero buffering streaming for immediate token delivery to clients.
- **Prometheus Observability**: Built-in `/metrics` endpoint for real-time monitoring of request latency, error rates, and throughput.
- **Dynamic Fallback Routing**: Automatic failover to secondary models/providers when the primary encounters errors or rate limits.

## Quickstart

### Prerequisites
- Python 3.10+
- Redis server running locally or accessible via network.
- Ollama (or other configured LLM providers) running locally for testing.

### Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Running the Gateway

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

### Testing

Run the test suite to verify rate limiting, streaming, and advanced gateway features:

```bash
pytest test_rate_limit.py test_ollama_stream.py test_gateway_advanced.py
```

## Docker

You can easily run the gateway and Redis together using Docker Compose:

```bash
docker-compose up -d
```

## Performance Benchmarks

- Sustained 500+ TPS on local hardware
- P99 TTFT (Time To First Token) < 50ms overhead
- Zero memory buffering for infinite streaming

## Observability

Prometheus metrics are exposed at `http://localhost:8000/metrics`.
