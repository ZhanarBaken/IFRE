from __future__ import annotations

import re


def normalize_task_type(value: str | None) -> str:
    if not value:
        return ""
    text = re.sub(r"\s+", " ", str(value).strip().lower())
    token = text.split(" ")[0] if text else ""
    return token.strip(" ,.;")


def normalize_unit_type(value: str | None) -> str:
    if not value:
        return ""
    text = re.sub(r"\s+", " ", str(value).strip().lower())
    return text.strip(" ,.;")


def normalize_plate(value: str | None) -> str:
    if not value:
        return ""
    text = re.sub(r"[^0-9A-ZА-Я]+", "", str(value).upper())
    return text
