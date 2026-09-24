import asyncio
import json
import httpx
import pytest
from main import app, verify_and_rate_limit

# Override rate limiting to avoid needing Redis for tests
def override_verify():
    return {"tenant_id": "test_tenant", "tier": "premium"}

app.dependency_overrides[verify_and_rate_limit] = override_verify

@pytest.mark.asyncio
async def test_metrics_endpoint():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/metrics")
        assert response.status_code == 200
        assert "http_requests_total" in response.text
        assert "active_streaming_connections" in response.text

@pytest.mark.asyncio
async def test_normal_streaming(monkeypatch):
    original_send = httpx.AsyncClient.send

    class MockResponse:
        def __init__(self):
            self.status_code = 200
        def raise_for_status(self):
            pass
        async def aiter_lines(self):
            yield json.dumps({"response": "Hello", "done": False})
            yield json.dumps({"response": " World", "done": True})

    async def mock_send(self, request, *args, **kwargs):
        if "11434" in str(request.url):
            return MockResponse()
        return await original_send(self, request, *args, **kwargs)

    monkeypatch.setattr(httpx.AsyncClient, "send", mock_send)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        payload = {"model": "llama3", "messages": [{"role": "user", "content": "Hi"}]}
        response = await client.post("/v1/chat/completions", json=payload)
        
        assert response.status_code == 200
        content = response.read().decode("utf-8")
        assert "Hello" in content
        assert " World" in content
        assert "[DONE]" in content

    # Check metrics
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        metrics_resp = await client.get("/metrics")
        metrics_text = metrics_resp.text
        assert 'http_requests_total{model="llama3",status_code="200",tenant="test_tenant"}' in metrics_text
        assert 'tokens_streamed_total{model="llama3",tenant="test_tenant"}' in metrics_text

@pytest.mark.asyncio
async def test_fallback_routing(monkeypatch):
    original_send = httpx.AsyncClient.send

    async def mock_send_fail(self, request, *args, **kwargs):
        if "11434" in str(request.url):
            raise httpx.ConnectTimeout("Timeout connecting to Ollama")
        return await original_send(self, request, *args, **kwargs)

    monkeypatch.setattr(httpx.AsyncClient, "send", mock_send_fail)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        payload = {"model": "llama3", "messages": [{"role": "user", "content": "Hi"}]}
        response = await client.post("/v1/chat/completions", json=payload)
        
        assert response.status_code == 200
        content = response.read().decode("utf-8")
        assert "fallback" in content.lower()
        assert "[DONE]" in content

    # Check metrics for fallback
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        metrics_resp = await client.get("/metrics")
        metrics_text = metrics_resp.text
        assert 'http_requests_total{model="llama3",status_code="500",tenant="test_tenant"}' in metrics_text
        assert 'http_requests_total{model="fallback_mock",status_code="200",tenant="test_tenant"}' in metrics_text
        assert 'tokens_streamed_total{model="fallback_mock",tenant="test_tenant"}' in metrics_text

if __name__ == "__main__":
    pytest.main(["-v", "test_gateway_advanced.py"])
