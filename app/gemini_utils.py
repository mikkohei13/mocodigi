"""Helper functions for Google Gemini API calls."""
import os
import google.genai as genai
from google.genai import types


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


def generate_content(
    client: genai.Client,
    content: types.Part | str,
    model_name: str,
    system_prompt: str,
    temperature: float = 0.0,
    thinking_budget: int = 128,
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
        thinking_budget: Thinking budget for the model (default: 128)
        max_chars: Maximum characters to accumulate before breaking
        
    Returns:
        Response text from the API (may be truncated if max_chars exceeded)
    """
    response_stream = client.models.generate_content_stream(
        model=model_name,
        contents=[content],
        config=types.GenerateContentConfig(
            temperature=temperature,
            system_instruction=system_prompt,
            thinking_config=types.ThinkingConfig(thinking_budget=thinking_budget)
        )
    )
    
    collected_text = ""
    for chunk in response_stream:
        if chunk.text:
            collected_text += chunk.text
            # Break if we exceed the character limit to avoid excessive token costs
            if len(collected_text) > max_chars:
                break
    
    return collected_text


def generate_transcription(
    client: genai.Client,
    image_part: types.Part,
    model_name: str,
    system_prompt: str,
    temperature: float = 0.0,
    thinking_budget: int = 128,
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
        thinking_budget: Thinking budget for the model (default: 128)
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
        thinking_budget=thinking_budget,
        max_chars=max_chars
    )


def generate_consolidation(
    client: genai.Client,
    text_content: str,
    model_name: str,
    system_prompt: str,
    temperature: float = 0.0,
    thinking_budget: int = 128,
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
        thinking_budget: Thinking budget for the model (default: 128)
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
        thinking_budget=thinking_budget,
        max_chars=max_chars
    )

