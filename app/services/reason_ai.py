from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Sequence
from urllib.error import URLError
from urllib.request import Request, urlopen

from app.core.config import settings

logger = logging.getLogger(__name__)


class ReasonAIService:
    def __init__(self) -> None:
        self.enabled = bool(
            settings.reason_ai_enabled
            and settings.reason_ai_api_url
            and settings.reason_ai_api_key
            and settings.reason_ai_model
        )

    def rewrite_one(self, reason: str, context: str) -> str:
        if not reason:
            return reason
        items = self.rewrite_many([reason], context=context)
        return items[0] if items else reason

    def rewrite_many(self, reasons: Sequence[str], context: str) -> list[str]:
        items = [r or "" for r in reasons]
        if not items or not self.enabled:
            return items

        unique: list[str] = []
        index_map: dict[str, int] = {}
        remap: list[int] = []
        for reason in items:
            idx = index_map.get(reason)
            if idx is None:
                idx = len(unique)
                index_map[reason] = idx
                unique.append(reason)
            remap.append(idx)

        rewritten_unique = self._rewrite_unique(unique, context=context)
        if not rewritten_unique or len(rewritten_unique) != len(unique):
            return [self.humanize_fallback(item) for item in items]
        final_unique: list[str] = []
        for original, rewritten in zip(unique, rewritten_unique):
            normalized = rewritten.strip()
            if not normalized or normalized == original.strip():
                normalized = self.humanize_fallback(original)
            if context.endswith("unassigned") and "не назнач" not in normalized.lower():
                normalized = self.humanize_fallback(original)
            if not context.endswith("unassigned") and normalized.lower().startswith("не назнач"):
                normalized = self.humanize_fallback(original)
            if context == "multitask_single_unit":
                if "осталь" in normalized.lower() and "осталь" not in original.lower():
                    normalized = self.humanize_fallback(original)
            final_unique.append(normalized)
        return [final_unique[idx] for idx in remap]

    def _rewrite_unique(self, reasons: list[str], context: str) -> list[str] | None:
        system_content = self._system_prompt(context)
        style_text = self._style_hint(context)
        payload = {
            "model": settings.reason_ai_model,
            "messages": [
                {
                    "role": "system",
                    "content": system_content,
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "context": context,
                            "reasons": reasons,
                            "style": style_text,
                        },
                        ensure_ascii=False,
                    ),
                },
            ],
            "temperature": 0.2,
        }

        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = Request(
            settings.reason_ai_api_url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {settings.reason_ai_api_key}",
            },
        )

        try:
            with urlopen(req, timeout=settings.reason_ai_timeout_sec) as resp:
                raw = resp.read().decode("utf-8")
        except URLError as exc:
            logger.warning("reason ai request failed: %s", exc)
            return None
        except Exception as exc:
            logger.warning("reason ai unexpected error: %s", exc)
            return None

        try:
            data = json.loads(raw)
            content = data["choices"][0]["message"]["content"]
        except Exception:
            logger.warning("reason ai bad response shape")
            return None

        return self._parse_json_array(content, expected_len=len(reasons))

    def _parse_json_array(self, text: str, expected_len: int) -> list[str] | None:
        candidate = text.strip()
        if candidate.startswith("```"):
            candidate = candidate.strip("`")
            if candidate.startswith("json"):
                candidate = candidate[4:].strip()

        try:
            parsed = json.loads(candidate)
        except Exception:
            start = candidate.find("[")
            end = candidate.rfind("]")
            if start == -1 or end == -1 or end <= start:
                return None
            try:
                parsed = json.loads(candidate[start : end + 1])
            except Exception:
                return None

        if isinstance(parsed, dict):
            parsed = parsed.get("reasons")
        if not isinstance(parsed, list):
            return None

        out = [str(item).strip() for item in parsed]
        if len(out) != expected_len:
            return None
        return out

    def _system_prompt(self, context: str) -> str:
        base = (
            "Ты помощник диспетчера спецтехники. "
            "Сохрани факты, числа, ID, лимиты и ограничения без изменений. "
            "Не используй аббревиатуры ETA/prio/compat_penalty. "
            "Используй термины 'техника' или 'машина'. "
            "Ответ верни строго JSON-массивом строк той же длины, без markdown."
        )
        if context == "recommendations":
            return (
                f"{base} "
                "Для рекомендаций сформулируй короткое обоснование назначения в стиле:"
                " 'ближайшая свободная, совместима по типу работ';"
                " 'свободна, но дальше на X км';"
                " 'занята, освободится через X мин, затем ближайшая'."
            )
        if context == "recommendations_baseline":
            return f"{base} Для baseline коротко укажи, что кандидат выбран только по расстоянию."
        if context.startswith("multitask_"):
            return (
                f"{base} "
                "Для multitask явно укажи: какие задачи объединены, почему остальные отдельно, "
                "и итоговую экономию км/мин относительно baseline."
            )
        if context.endswith("unassigned"):
            return (
                f"{base} "
                "Для отказа обязательно начни с 'Не назначена, потому что ...' и укажи конкретный ограничивающий фактор."
            )
        return (
            f"{base} "
            "Сформулируй понятное объяснение решения, а не перефраз. "
            "Добавь причинно-следственную связь."
        )

    def _style_hint(self, context: str) -> str:
        if context in {"recommendations", "recommendations_baseline"}:
            return "1 короткая фраза (до 18 слов), без лишних деталей."
        if context.startswith("multitask_"):
            return "1-2 предложения: объединение/разделение и экономия."
        if context.endswith("unassigned"):
            return "1-2 предложения в формате причины отказа."
        return "1-3 коротких предложения, максимально конкретно."

    def humanize_fallback(self, original: str) -> str:
        text = original
        dist = re.search(r"расстояние\s+([0-9.]+)\s*км(?:\s*\(вклад\s*([0-9.]+)\))?", text)
        eta = re.search(r"(?:ETA|время в пути)\s*([0-9]+)\s*мин(?:\s*\(вклад\s*([0-9.]+)\))?", text)
        wait = re.search(r"ожидание\s+([0-9]+)\s*мин(?:\s*\(вклад\s*([0-9.]+)\))?", text)
        late = re.search(r"(?:SLA-)?опоздание\s+([0-9]+)\s*мин(?:\s*\(вклад\s*([0-9.]+)\))?", text)
        prio = re.search(r"приоритет\s+([a-zA-Zа-яА-Я]+)", text)
        score = re.search(r"балл\s+([0-9.]+)/100", text)
        compat = re.search(r"штраф совместимости\s+([0-9.]+)", text)
        cost = re.search(r"итоговая стоимость\s+([0-9.]+)", text)
        deadline = re.search(r"дедлайн\s+([0-9T:.-]+)", text)
        available_at = re.search(r"доступность техники\s+([0-9T:.-]+)", text)
        start_at = re.search(r"старт\s+([0-9T:.-]+)", text)

        if dist and eta and wait and late and prio:
            prio_text = prio.group(1)
            prio_map = {"high": "высокий", "medium": "средний", "low": "низкий"}
            prio_text = prio_map.get(prio_text.lower(), prio_text)
            dist_v = dist.group(1).rstrip(".")
            eta_v = eta.group(1).rstrip(".")
            wait_v = wait.group(1).rstrip(".")
            late_v = late.group(1).rstrip(".")
            dist_c = dist.group(2).rstrip(".") if dist.group(2) else None
            eta_c = eta.group(2).rstrip(".") if eta.group(2) else None
            wait_c = wait.group(2).rstrip(".") if wait.group(2) else None
            late_c = late.group(2).rstrip(".") if late.group(2) else None

            factors = [
                f"расстояние {dist_v} км" + (f" (вклад {dist_c})" if dist_c else ""),
                f"время в пути {eta_v} мин" + (f" (вклад {eta_c})" if eta_c else ""),
                f"ожидание {wait_v} мин" + (f" (вклад {wait_c})" if wait_c else ""),
                f"SLA-опоздание {late_v} мин" + (f" (вклад {late_c})" if late_c else ""),
            ]
            parts = [f"Факторы выбора: {', '.join(factors)}; приоритет {prio_text}."]

            late_num = int(late.group(1))
            if late_num > 0 and available_at and deadline:
                try:
                    available_dt = datetime.fromisoformat(available_at.group(1).rstrip("."))
                    deadline_dt = datetime.fromisoformat(deadline.group(1).rstrip("."))
                    if available_dt > deadline_dt:
                        parts.append("Опоздание возникло, потому что техника стала доступна после SLA-дедлайна.")
                except Exception:
                    pass
            if late_num > 0 and start_at:
                parts.append(f"Фактический старт: {start_at.group(1).rstrip('.')}.")
            if compat:
                parts.append(f"Учтен штраф совместимости {compat.group(1).rstrip('.')}.")
            if cost:
                parts.append(f"Итоговая стоимость {cost.group(1).rstrip('.')}.")
            if score:
                parts.append(f"Итоговый балл {score.group(1).rstrip('.')}/100.")
            return " ".join(parts)

        replacements = [
            ("ETA", "время в пути"),
            ("prio high", "приоритет высокий"),
            ("prio medium", "приоритет средний"),
            ("prio low", "приоритет низкий"),
            ("приоритет high", "приоритет высокий"),
            ("приоритет medium", "приоритет средний"),
            ("приоритет low", "приоритет низкий"),
            ("compat_penalty", "штраф совместимости"),
            ("baseline: nearest by distance", "базовый режим: выбрана ближайшая техника по расстоянию"),
            ("waits", "ожидание"),
        ]
        for src, dst in replacements:
            text = text.replace(src, dst)
        return text
