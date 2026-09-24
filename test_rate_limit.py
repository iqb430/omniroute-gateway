import requests
import time

url = "http://localhost:8000/v1/chat/completions"
headers = {
    "Authorization": "Bearer test_api_key_123",
    "Content-Type": "application/json"
}
payload = {
    "messages": [{"role": "user", "content": "Hello"}]
}

print("Testing missing API key (Expect 401):")
response_401 = requests.post(url, json=payload)
print(f"Status: {response_401.status_code}, Response: {response_401.text}\n")

print("Sending 10 rapid requests to test rate limiting (Limit: 5 per minute)...")
for i in range(1, 11):
    response = requests.post(url, headers=headers, json=payload, stream=True)
    status = response.status_code
    if status == 429:
        print(f"Request {i}: Status {status} -> Rate limit hit: {response.json()}")
    elif status == 200:
        # Read a bit of the stream to ensure it works, then close to save time
        chunk = next(response.iter_lines(), b"").decode('utf-8')
        print(f"Request {i}: Status {status} -> Success (stream started, first chunk: {chunk})")
        response.close()
    else:
        print(f"Request {i}: Status {status} -> Other error: {response.text}")
    time.sleep(0.1)
