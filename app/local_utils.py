import json
import os
import urllib.error
import urllib.request


BASE_URL = os.getenv("LM_STUDIO_BASE_URL", "http://host.docker.internal:1234/v1")
MODEL = os.getenv("LM_STUDIO_MODEL", "local-model")


def get_local_client(base_url: str | None = None) -> str:
    """Return local OpenAI-compatible API base URL."""
    return base_url or BASE_URL


def _extract_response_text(data: dict) -> str:
    """Extract assistant text from OpenAI-compatible chat completion payload."""
    choices = data.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""

    message = choices[0].get("message", {})
    content = message.get("content", "")

    if isinstance(content, str):
        return content

    # Some providers return structured content arrays.
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict) and isinstance(item.get("text"), str):
                parts.append(item["text"])
        return "".join(parts)

    return ""


def generate_content(
    client: str,
    content: str,
    model_name: str,
    system_prompt: str,
    temperature: float = 0.0,
    thinking_budget: int = 128,
    max_chars: int = 200,
    timeout: int = 120,
) -> str:
    """
    Generate content from a local OpenAI-compatible endpoint (e.g., LM Studio).
    This mirrors the gemini_utils helper signature; thinking_budget is unused.
    """
    _ = thinking_budget  # Not supported by OpenAI-compatible local chat endpoints.

    payload = {
        "model": model_name,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": content},
        ],
        "temperature": temperature,
    }

    request = urllib.request.Request(
        url=f"{client}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            data = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        return f"HTTP_ERROR: {error}"
    except urllib.error.URLError as error:
        return f"REQUEST_FAILED: {error}"
    except json.JSONDecodeError as error:
        return f"RESPONSE_JSON_DECODE_ERROR: {error}"

    text = _extract_response_text(data)
    if text:
        return text[:max_chars] if max_chars > 0 else text

    return json.dumps(data)


def generate_consolidation(
    client: str,
    text_content: str,
    model_name: str,
    system_prompt: str,
    temperature: float = 0.0,
    thinking_budget: int = 128,
    max_chars: int = 200,
) -> str:
    """Generate consolidation text using local OpenAI-compatible chat completions."""
    return generate_content(
        client=client,
        content=text_content,
        model_name=model_name or MODEL,
        system_prompt=system_prompt,
        temperature=temperature,
        thinking_budget=thinking_budget,
        max_chars=max_chars,
    )


if __name__ == "__main__":
    print(
        generate_consolidation(
            client=get_local_client(),
            text_content="Write a short story about a cat, max 3 sentences.",
            model_name=MODEL,
            system_prompt="You are a helpful assistant.",
            temperature=0.7,
            max_chars=1000,
        )
    )

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