"""Helper functions for image file handling."""
from pathlib import Path
import io
import PIL.Image
from google.genai import types


# Supported image extensions
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}

# MIME type mapping
MIME_TYPE_MAP = {
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.png': 'image/png',
    '.gif': 'image/gif',
    '.webp': 'image/webp'
}


def get_image_files_from_folder(folder_path: str) -> list[Path]:
    """
    Get all image files from a folder.
    
    Args:
        folder_path: Path to the folder containing images
        
    Returns:
        List of Path objects for image files
        
    Raises:
        FileNotFoundError: If folder doesn't exist
        ValueError: If folder contains no image files
    """
    folder = Path(folder_path)
    if not folder.exists():
        raise FileNotFoundError(f"Folder '{folder_path}' does not exist")
    
    if not folder.is_dir():
        raise ValueError(f"'{folder_path}' is not a directory")
    
    image_files = [
        f for f in folder.iterdir()
        if f.suffix.lower() in IMAGE_EXTENSIONS
    ]
    
    if not image_files:
        raise ValueError(f"No image files found in '{folder_path}'")
    
    return sorted(image_files)


def load_image_as_part(image_file: Path) -> types.Part:
    """
    Load an image file and convert it to a Part object for the Gemini API.
    
    Args:
        image_file: Path to the image file
        
    Returns:
        Part object containing the image data and MIME type
    """
    # Load the image
    image = PIL.Image.open(image_file)
    
    # Convert PIL Image to bytes
    img_bytes = io.BytesIO()
    image.save(img_bytes, format=image.format or 'JPEG')
    img_bytes.seek(0)
    image_bytes = img_bytes.read()
    
    # Determine MIME type based on file extension
    mime_type = MIME_TYPE_MAP.get(image_file.suffix.lower(), 'image/jpeg')
    
    # Create and return image part
    return types.Part.from_bytes(data=image_bytes, mime_type=mime_type)

