from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from app.company import load_companies
from app.documents import generate_document
from app.parser import parse_user_input


def main() -> None:
    companies = load_companies()
    parsed = parse_user_input(
        """
Клиент: ООО Ромашка
ИНН: 1234567890
КПП: 123401001
Адрес: г. Уфа, ул. Примерная, 1
Предмет: перевозка груза по маршруту Уфа - Казань
Позиции: Перевозка груза | 1 | рейс | 55000
Сумма: 55000
НДС: Без НДС
Срок: до 5 рабочих дней
Оплата: 100% предоплата
Номер: 15
""".strip()
    )
    for doc_type in ["invoice", "contract", "offer", "claim_reply", "act", "waybill", "reconciliation", "official_letter"]:
        generated = generate_document(doc_type, companies["ooo"], parsed, output_dir=BASE_DIR / "generated")
        print(generated.path)


if __name__ == "__main__":
    main()
