import httpx
import asyncio
import json
import uuid

async def test_stream():
    url = "http://localhost:8000/v1/chat/completions"
    # Use a random API key to avoid hitting the rate limit from previous tests
    api_key = f"sk-{uuid.uuid4()}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "llama3",
        "messages": [{"role": "user", "content": "Write a short poem about a gateway."}]
    }
    
    print(f"Sending request to {url} with API key {api_key}...")
    async with httpx.AsyncClient() as client:
        try:
            async with client.stream("POST", url, headers=headers, json=payload, timeout=60.0) as response:
                print(f"Status code: {response.status_code}")
                if response.status_code != 200:
                    print(f"Error: {await response.aread()}")
                    return
                    
                print("Response stream:")
                async for line in response.aiter_lines():
                    if line:
                        print(line)
        except Exception as e:
            print(f"Connection error: {e}")

if __name__ == "__main__":
    asyncio.run(test_stream())
