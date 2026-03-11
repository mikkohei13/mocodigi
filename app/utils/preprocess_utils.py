"""Utilities for transcript text preprocessing."""

from __future__ import annotations

import re


def preprocess(text: str) -> tuple[str, dict[str, object]]:
    """Apply transcript preprocessing rules and return details.
    
    Args:
        text: The text to preprocess.

    Returns:
        A tuple containing the preprocessed text and a dictionary of details.
    """

    preprocessing_done = dict[str, object]()

    if not text:
        return text, preprocessing_done
    
    # -----
    # Preprocess steps
    # -----

    # If 2x id.luomus.fi present in the text, flag the case.
    # Example: C.320243
    if text.count("id.luomus.fi") > 1:
        preprocessing_done["multiple_specimens"] = "True"
        return text, preprocessing_done
 
    # QUADR. / QUAOR. / QUOR. etc. -> remove
    quadr_pattern = re.compile(
        r"QUADR\.|QUAOR\.|QUOR\.|QUACR\.|QUAGR\.",
        flags=re.IGNORECASE,
    )
    quadr_match = quadr_pattern.search(text)
    if quadr_match:
        text = quadr_pattern.sub("", text, count=1).strip()
        preprocessing_done["remove_quadr"] = "True"
    else:
        preprocessing_done["remove_quadr"] = "False"

    # Identifier and digitization date
    # http://id.luomus.fi/<identifier> <date> Museum Botanicum Univ. (H) Helsinki
    luomus_pattern = re.compile(
        r"(http://id\.luomus\.fi/)\s*([A-Za-z]{1,4}\.[0-9]+)\s*([0-9]{4}-[0-9]{2}-[0-9]{2})\s*"
        r"",
        flags=re.IGNORECASE,
    )
    luomus_match = luomus_pattern.search(text)
    if luomus_match:
        base_uri, specimen_id, specimen_date = luomus_match.groups()
        preprocessing_done["http_uri"] = f"{base_uri}{specimen_id}"
        preprocessing_done["digitization_date"] = specimen_date
        preprocessing_done["missing_http_uri"] = "False"
        text = luomus_pattern.sub("", text, count=1).strip()
    else:
        preprocessing_done["missing_http_uri"] = "True"

    # H-number
    # MUSEUM BOTANICUM UNIV. (H). HELSINKI <number> -> remove and store number
    helsinki_pattern = re.compile(
        r"MUSEUM[\s.,]*BOTANICUM[\s.,]*UNIV[\s.,]*(?:\(\s*H\s*\)[\s.,]*)?HELSINKI[\s.,]*([0-9]{4,8})",
        flags=re.IGNORECASE,
    )
    helsinki_match = helsinki_pattern.search(text)
    if helsinki_match:
        h_number = int(helsinki_match.group(1))
        if 1000 <= h_number <= 10_000_000:
            text = helsinki_pattern.sub("", text, count=1).strip()
            preprocessing_done["h_number"] = h_number

    # Remove "Museum Botanicum Univ. (H) Helsinki" from digitization label
    helsinki_pattern = re.compile(
        r"MUSEUM[\s.,]*BOTANICUM[\s.,]*UNIV[\s.,]*(?:\(\s*H\s*\)[\s.,]*)?HELSINKI",
        flags=re.IGNORECASE,
    )
    helsinki_match = helsinki_pattern.search(text)
    if helsinki_match:
        text = helsinki_pattern.sub("", text, count=1).strip()

    # Remove "MUSEUM BOTANICUM UNIVERSITATIS, HELSINKI" traditional title
    helsinki_pattern = re.compile(
        r"MUSEUM[\s.,]*BOTANICUM[\s.,]*UNIVERSITATIS[\s.,]*HELSINKI",
        flags=re.IGNORECASE,
    )
    helsinki_match = helsinki_pattern.search(text)
    if helsinki_match:
        text = helsinki_pattern.sub("", text, count=1).strip()

    # Remove "HERBARIUM MUSEI HELSINGIENSIS" traditional title
    helsinki_pattern = re.compile(
        r"HERBARIUM[\s.,]*MUSEI[\s.,]*HELSINGIENSIS",
        flags=re.IGNORECASE,
    )
    helsinki_match = helsinki_pattern.search(text)
    if helsinki_match:
        text = helsinki_pattern.sub("", text, count=1).strip()

    # Remove "HORTUS BOTANICUS UNIVERSITATIS HELSINGIENSIS" traditional title
    helsinki_pattern = re.compile(
        r"HORTUS[\s.,]*BOTANICUS[\s.,]*UNIVERSITATIS[\s.,]*HELSINGIENSIS",
        flags=re.IGNORECASE,
    )
    helsinki_match = helsinki_pattern.search(text)
    if helsinki_match:
        text = helsinki_pattern.sub("", text, count=1).strip()

    # Remove "Botanical Museum University of Helsinki" title, ignoring whitespace
    helsinki_pattern = re.compile(
        r"Botanical[\s.,]*Museum[\s.,]*University[\s.,]*of[\s.,]*Helsinki",
        flags=re.IGNORECASE,
    )
    helsinki_match = helsinki_pattern.search(text)
    if helsinki_match:
        text = helsinki_pattern.sub("", text, count=1).strip()

    # If still mentions Helsinki or Helsingfors, flag the case.
    # Todo: Check what these cases are, and whether the remaining Helsinki hinders text structurization
    if "Helsinki" in text or "Helsingin" in text or "Helsingfors" in text:
        preprocessing_done["multiple_helsinki"] = "True"
    else:
        preprocessing_done["multiple_helsinki"] = "False"

    # -----
    # End of preprocess steps
    # -----
    

    return text, preprocessing_done

