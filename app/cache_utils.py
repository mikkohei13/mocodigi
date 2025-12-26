"""Helper functions for caching transcriptions."""
import json
from pathlib import Path
from datetime import datetime


def get_cache_path(image_file: Path, run_version: str) -> Path:
    """
    Get the cache file path for an image file.
    
    Args:
        image_file: Path to the image file
        run_version: Version string to include in subfolder name
        
    Returns:
        Path to the cache file in the run_{run_version} subdirectory
    """
    run_dir = image_file.parent / f"run_{run_version}"
    cache_filename = f"{image_file.stem}_transcription.json"
    return run_dir / cache_filename


def cache_exists(image_file: Path, run_version: str) -> bool:
    """
    Check if a cache file exists for an image.
    
    Args:
        image_file: Path to the image file
        run_version: Version string to include in subfolder name
        
    Returns:
        True if cache file exists, False otherwise
    """
    cache_path = get_cache_path(image_file, run_version)
    return cache_path.exists()


def load_cache(image_file: Path, run_version: str) -> dict:
    """
    Load transcription from cache file.
    
    Args:
        image_file: Path to the image file
        run_version: Version string to include in subfolder name
        
    Returns:
        Dictionary containing cached transcription data
        
    Raises:
        FileNotFoundError: If cache file doesn't exist
        json.JSONDecodeError: If cache file is invalid JSON
    """
    cache_path = get_cache_path(image_file, run_version)
    with open(cache_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_cache(
    image_file: Path,
    transcription: str,
    model_name: str,
    prompt: str,
    temperature: float,
    run_version: str
) -> Path:
    """
    Save transcription to cache file.
    
    Args:
        image_file: Path to the image file
        transcription: Transcription text
        model_name: Model name used
        prompt: System prompt used
        temperature: Temperature setting used
        
    Returns:
        Path to the saved cache file
    """
    cache_path = get_cache_path(image_file, run_version)
    
    # Create run directory if it doesn't exist
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    
    cache_data = {
        "format_version": "0.1",
        "type": "transcript",
        "datetime": datetime.now().isoformat(),
        "settings": {
            "run_version": run_version,
            "model": model_name,
            "prompt": prompt,
            "temperature": temperature
        },
        "data": {
            "transcription": transcription
        }
    }
    
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(cache_data, f, indent=2, ensure_ascii=False)
    
    return cache_path

