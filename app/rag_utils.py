'''
Retrieval-augmented generation (RAG) utilities.
'''
from pathlib import Path
import json


def load_meta_json(folder_path: str | Path) -> dict:
    """
    Load meta.json from the specified folder if it exists.
    
    Args:
        folder_path: Path to the image folder.
    
    Returns:
        Dictionary with metadata, or empty dict if file doesn't exist or can't be read.
    """
    meta_path = Path(folder_path) / "meta.json"
    if not meta_path.exists():
        return {}
    
    try:
        with open(meta_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def get_rag_content(folder_path: str | Path = None, transcripts_content: str = None) -> str:
    """
    Generate RAG content with optional metadata from meta.json.
    
    Args:
        folder_path: Path to the image folder. If provided, will attempt to load
                     meta.json from this folder and inject its data into the context.
        transcripts_content: Content of the transcripts.
    
    Returns:
        RAG context string with metadata if available.
    """
    metadata = load_meta_json(folder_path) if folder_path else {}
    context = "# Context:\n"
    
    # Build metadata context string
    meta_context = ""
    if metadata:
        if "country" in metadata:
            if metadata["country"] != "world":
                context += f"\n- The specimen has been collected in {metadata['country']}."
            else:
                context += f"\n- The specimen could have been collected anywhere in the world."

        context += "\n- The specimen belongs to "
        if "class" in metadata:
            context += f"class {metadata['class']} "
        if "order" in metadata:
            context += f"order {metadata['order']} "
        if "species" in metadata:
            context += f"species {metadata['species']}"
        context += "."

    if "Loan No." in transcripts_content:
        context += "\n- The specimen contains a single loan number, with format 'Mus. Zool. Helsinki Loan No. HE <integer>'."

    content = context + f"""
- The specimen was probably collected in 1900s. Year might be abbreviated as YY, or written as YYYY.
- The labels may be in any language using the Latin alphabet with diacritics.
- The labels often contain the following types of information, but **capture all legible content even if it does not fit these categories**:
  - **Locality names:** country, region, abbreviation, coordinates.
  - **Collection Data:** dates (months often in Roman numerals), and collector names (sometimes with 'leg' or 'coll').
  - **Taxonomy:** binomial scientific names, author names, and determiner names (sometimes with 'det').
  - **Curatorial:** loan info, catalog numbers, type status.
"""
    
    return content