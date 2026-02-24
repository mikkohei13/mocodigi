import json
import os
import urllib.error
import urllib.request


BASE_URL = os.getenv("LM_STUDIO_BASE_URL", "http://host.docker.internal:1234/v1")
MODEL = os.getenv("LM_STUDIO_MODEL", "local-model")
PROMPT = "Write a short story about a cat, max 3 sentences."


def main() -> None:
    payload = {
        "model": MODEL,
        "messages": [{"role": "user", "content": PROMPT}],
        "temperature": 0.7,
    }

    request = urllib.request.Request(
        url=f"{BASE_URL}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            data = json.loads(response.read().decode("utf-8"))
    except urllib.error.URLError as error:
        print(f"Request failed: {error}")
        return

    print(data)


if __name__ == "__main__":
    main()

'''
Example response:



{
   "id":"chatcmpl-0gq48vv2goc6dr9buyrpiqv",
   "object":"chat.completion",
   "created":1771946643,
   "model":"openai/gpt-oss-20b",
   "choices":[
      {
         "index":0,
         "message":{
            "role":"assistant",
            "content":"Midnight, the alley’s quiet guard, padded across the fence and slipped into the old bakery where flour dusted his whiskers like stardust. He leapt onto the counter, pawing at a stray loaf that suddenly sprouted wings and fluttered away, leaving only a faint scent of cinnamon. The baker, blinking in surprise, whispered to the cat, “You’ve made my day a little sweeter.”",
            "reasoning":"Need <=3 sentences short story about a cat. Provide 1-3 sentences.",
            "tool_calls":[
            ]
         },
         "logprobs":"None",
         "finish_reason":"stop"
      }
   ],
   "usage":{
      "prompt_tokens":80,
      "completion_tokens":111,
      "total_tokens":191
   },
   "stats":{
   },
   "system_fingerprint":"openai/gpt-oss-20b"
}



'''