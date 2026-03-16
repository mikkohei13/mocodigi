"""Helper functions for Google Gemini API calls."""
import os
import google.genai as genai
from google.genai import types
from typing import Any


def get_gemini_client(use_vertex_ai: bool = False) -> genai.Client:
    """
    Initialize and return a Gemini client.

    When use_vertex_ai is True, uses Application Default Credentials (ADC) with
    GOOGLE_CLOUD_PROJECT and GOOGLE_CLOUD_LOCATION env vars.
    Otherwise uses GEMINI_DEVELOPER_API_KEY for the Gemini Developer API.
    """
    if use_vertex_ai:
        project = os.getenv("GOOGLE_CLOUD_PROJECT")
        location = os.getenv("GOOGLE_CLOUD_LOCATION", "europe-west1")
        if not project:
            raise ValueError(
                "GOOGLE_CLOUD_PROJECT is not set. "
                "Please set it in .env and ensure GOOGLE_APPLICATION_CREDENTIALS points to your ADC JSON."
            )
        return genai.Client(vertexai=True, project=project, location=location)

    api_key = os.getenv("GEMINI_DEVELOPER_API_KEY")
    if not api_key:
        raise ValueError("API key is not set. Please set GEMINI_DEVELOPER_API_KEY in .env.")
    return genai.Client(api_key=api_key)


def _build_thinking_config(model_name: str) -> types.ThinkingConfig | None:
    """Return minimal-thinking config for supported Gemini models."""
    normalized_model = model_name.strip()
    if normalized_model == "gemini-3-flash-preview":
        return types.ThinkingConfig(thinking_level=types.ThinkingLevel.MINIMAL)
    if normalized_model == "gemini-3.1-pro-preview":
        return types.ThinkingConfig(thinking_level=types.ThinkingLevel.LOW)
    if normalized_model == "gemini-2.5-pro":
        return types.ThinkingConfig(thinking_budget=128)
    if normalized_model.startswith("gemini-3"):
        return types.ThinkingConfig(thinking_level=types.ThinkingLevel.LOW)
    return None


def generate_content(
    client: genai.Client,
    content: types.Part | str,
    model_name: str,
    system_prompt: str,
    temperature: float = 0.0,
    max_chars: int = 200
) -> str:
    """
    Generate content using Gemini API with streaming to prevent excessive token costs.
    
    Args:
        client: Initialized Gemini client
        content: Content to process (Part object for images, str for text)
        model_name: Name of the Gemini model to use
        system_prompt: System instruction prompt
        temperature: Temperature for generation (default: 0.0)
        max_chars: Maximum characters to accumulate before breaking
        
    Returns:
        Response text from the API (may be truncated if max_chars exceeded)
    """
    config_kwargs: dict[str, Any] = {
        "temperature": temperature,
        "system_instruction": system_prompt,
    }
    thinking_config = _build_thinking_config(model_name)
    if thinking_config is not None:
        config_kwargs["thinking_config"] = thinking_config

    response_stream = client.models.generate_content_stream(
        model=model_name,
        contents=[content],
        config=types.GenerateContentConfig(**config_kwargs),
    )
    
    collected_text = ""
    for chunk in response_stream:
        if chunk.text:
            collected_text += chunk.text
            # Break if we exceed the character limit to avoid excessive token costs
            if len(collected_text) > max_chars:
                break
    
    return collected_text


def _serialize_chunk(chunk: Any) -> dict:
    """Convert a Gemini chunk object into a JSON-serializable dictionary."""
    if hasattr(chunk, "model_dump"):
        return chunk.model_dump(mode="json", exclude_none=False)
    return {"repr": repr(chunk)}


def generate_content_with_stream_capture(
    client: genai.Client,
    content: types.Part | str,
    model_name: str,
    system_prompt: str,
    temperature: float = 0.0,
    max_chars: int = 200
) -> dict:
    """
    Stream Gemini response while capturing raw chunks and truncating output text.

    Returns:
        Dictionary containing:
            - transcript_text: potentially truncated text used downstream
            - full_text_received: all text received before stopping stream
            - was_truncated: True if stream was cut after max_chars
            - max_chars: configured max char limit
            - chunks: serialized stream chunks received
    """
    config_kwargs: dict[str, Any] = {
        "temperature": temperature,
        "system_instruction": system_prompt,
    }
    thinking_config = _build_thinking_config(model_name)
    if thinking_config is not None:
        config_kwargs["thinking_config"] = thinking_config

    response_stream = client.models.generate_content_stream(
        model=model_name,
        contents=[content],
        config=types.GenerateContentConfig(**config_kwargs),
    )

    full_text_received = ""
    chunks: list[dict] = []
    was_truncated = False

    for chunk in response_stream:
        chunks.append(_serialize_chunk(chunk))
        if chunk.text:
            full_text_received += chunk.text
            if len(full_text_received) > max_chars:
                was_truncated = True
                break

    transcript_text = full_text_received[:max_chars] if was_truncated else full_text_received
    return {
        "transcript_text": transcript_text,
        "full_text_received": full_text_received,
        "was_truncated": was_truncated,
        "max_chars": max_chars,
        "chunks": chunks
    }


def generate_transcription(
    client: genai.Client,
    image_part: types.Part,
    model_name: str,
    system_prompt: str,
    temperature: float = 0.0,
    max_chars: int = 200
) -> str:
    """
    Generate transcription for an image using Gemini API.
    
    Args:
        client: Initialized Gemini client
        image_part: Image part object
        model_name: Name of the Gemini model to use
        system_prompt: System instruction prompt
        temperature: Temperature for generation (default: 0.0)
        max_chars: Maximum characters to accumulate before breaking (default: 200)
        
    Returns:
        Response text from the API (may be truncated if max_chars exceeded)
    """
    return generate_content(
        client=client,
        content=image_part,
        model_name=model_name,
        system_prompt=system_prompt,
        temperature=temperature,
        max_chars=max_chars
    )


def generate_transcription_with_stream_capture(
    client: genai.Client,
    image_part: types.Part,
    model_name: str,
    system_prompt: str,
    temperature: float = 0.0,
    max_chars: int = 200
) -> dict:
    """
    Generate transcription with stream chunk capture for diagnostics/auditing.
    """
    return generate_content_with_stream_capture(
        client=client,
        content=image_part,
        model_name=model_name,
        system_prompt=system_prompt,
        temperature=temperature,
        max_chars=max_chars
    )


def generate_consolidation(
    client: genai.Client,
    text_content: str,
    model_name: str,
    system_prompt: str,
    temperature: float = 0.0,
    max_chars: int = 200
) -> str:
    """
    Generate consolidation for concatenated transcripts using Gemini API.
    
    Args:
        client: Initialized Gemini client
        text_content: Concatenated transcript text
        model_name: Name of the Gemini model to use
        system_prompt: System instruction prompt
        temperature: Temperature for generation (default: 0.0)
        max_chars: Maximum characters to accumulate before breaking (default: 200)
        
    Returns:
        Response text from the API (may be truncated if max_chars exceeded)
    """
    return generate_content(
        client=client,
        content=text_content,
        model_name=model_name,
        system_prompt=system_prompt,
        temperature=temperature,
        max_chars=max_chars
    )

