from __future__ import annotations

from app.parser import ParsedInput


class OpenAIDrafter:
    def __init__(self, api_key: str | None, model: str) -> None:
        self.api_key = api_key
        self.model = model

    def draft(self, doc_type: str, parsed: ParsedInput) -> str | None:
        if not self.api_key or doc_type not in {"door_offer", "measurement_estimate", "offer", "claim_reply", "official_letter"}:
            return None

        try:
            from openai import OpenAI

            client = OpenAI(api_key=self.api_key, timeout=12)
            response = client.responses.create(
                model=self.model,
                input=[
                    {
                        "role": "system",
                        "content": (
                            "Ты помощник по российскому деловому документообороту для компании, которая продает и устанавливает двери. "
                            "Пиши кратко, официально, продающе, без выдумывания реквизитов, цен и правовых гарантий."
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"Тип документа: {doc_type}\n"
                            f"Данные пользователя:\n{parsed.raw}\n\n"
                            "Составь основной текст документа на русском языке. "
                            "Не добавляй подписи и реквизиты. "
                            "Для дверных КП и смет не выделяй стандартный монтаж отдельной строкой или отдельной услугой, "
                            "если пользователь явно не попросил обратное."
                        ),
                    },
                ],
            )
            return response.output_text.strip()
        except Exception:
            return None
