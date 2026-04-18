from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from uuid import uuid4

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from app.company import Company
from app.documents import (
    DOC_TYPES,
    GeneratedDocument,
    PASSPORT_GENERAL_INFO,
    PASSPORT_INSTALLATION,
    PASSPORT_OPERATION,
    PASSPORT_PACKAGE,
    PASSPORT_PROHIBITED,
    PASSPORT_SAFETY,
    PASSPORT_TECH_SPECS,
    PASSPORT_TRANSPORT,
    WARRANTY_COVERAGE,
    WARRANTY_EXCLUSIONS,
    WARRANTY_MAINTENANCE,
    WARRANTY_MANUFACTURER,
)
from app.payments import generate_payment_qr_png
from app.parser import ParsedInput


FONT_REGULAR = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
FONT_BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"


def generate_pdf_document(
    doc_type: str,
    company: Company,
    parsed: ParsedInput,
    output_dir: Path,
    ai_text: str | None = None,
) -> GeneratedDocument:
    output_dir.mkdir(parents=True, exist_ok=True)
    _register_fonts()

    title = DOC_TYPES.get(doc_type, "Документ")
    number = parsed.fields.get("number") or date.today().strftime("%y%m%d")
    filename = f"{doc_type}_{number}_{uuid4().hex[:8]}.pdf"
    path = output_dir / filename

    styles = _styles()
    items = _display_items(doc_type, parsed)
    story = [
        _brand_header(company, title, number, styles),
        Spacer(1, 5 * mm),
        Paragraph(f"{_party_label(doc_type)}: {_counterparty_line(parsed.fields)}", styles["Body"]),
        Spacer(1, 4 * mm),
    ]

    intro = _intro_text(doc_type, parsed, ai_text)
    if intro:
        story.append(Paragraph(intro, styles["Body"]))
        story.append(Spacer(1, 4 * mm))

    specs = _spec_rows(parsed.fields)
    if specs:
        story.append(Paragraph(_spec_heading(doc_type), styles["Section"]))
        story.append(_kv_table(specs, styles))
        story.append(Spacer(1, 4 * mm))

    if items:
        story.append(Paragraph(_items_heading(doc_type), styles["Section"]))
        story.append(_items_table(items, styles))
        total_label = "Итого по документу" if doc_type in {"measurement_estimate", "act", "installation_act"} else "Итого"
        story.append(Paragraph(f"{total_label}: {_format_money(_items_total(items))} руб.", styles["Body"]))
        story.append(Spacer(1, 4 * mm))

    if doc_type == "invoice":
        qr_path = generate_payment_qr_png(company, parsed, output_dir, number)
        story.append(_payment_qr_block(qr_path, styles))
        story.append(Spacer(1, 4 * mm))

    story.extend(_extra_sections(doc_type, company, parsed, styles))

    story.extend(
        [
            Spacer(1, 8 * mm),
            Paragraph("Реквизиты исполнителя", styles["Section"]),
            _kv_table(_company_rows(company), styles),
            Spacer(1, 8 * mm),
        ]
    )
    if doc_type in {"measurement_act", "installation_act", "act"}:
        story.append(_signatures_table(company, parsed.fields, styles))
    else:
        story.append(Paragraph(f"{company.data['manager_title']} __________________ {company.data['manager_name']}", styles["Body"]))

    doc = SimpleDocTemplate(
        str(path),
        pagesize=A4,
        rightMargin=14 * mm,
        leftMargin=14 * mm,
        topMargin=12 * mm,
        bottomMargin=12 * mm,
        title=title,
    )
    doc.build(story)
    return GeneratedDocument(title=title, path=path, filename=filename)


def _register_fonts() -> None:
    if "DejaVuSans" not in pdfmetrics.getRegisteredFontNames():
        pdfmetrics.registerFont(TTFont("DejaVuSans", FONT_REGULAR))
    if "DejaVuSans-Bold" not in pdfmetrics.getRegisteredFontNames():
        pdfmetrics.registerFont(TTFont("DejaVuSans-Bold", FONT_BOLD))


def _styles() -> dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    return {
        "Title": ParagraphStyle("Title", parent=base["Title"], fontName="DejaVuSans-Bold", fontSize=15, leading=18, alignment=0, textColor=colors.white),
        "HeaderMeta": ParagraphStyle("HeaderMeta", parent=base["Normal"], fontName="DejaVuSans", fontSize=8, leading=10, alignment=2, textColor=colors.white),
        "Center": ParagraphStyle("Center", parent=base["Normal"], fontName="DejaVuSans", fontSize=9, leading=12, alignment=1),
        "Section": ParagraphStyle("Section", parent=base["Normal"], fontName="DejaVuSans-Bold", fontSize=10, leading=13, spaceAfter=4),
        "Body": ParagraphStyle("Body", parent=base["Normal"], fontName="DejaVuSans", fontSize=9, leading=12, spaceAfter=3),
        "Cell": ParagraphStyle("Cell", parent=base["Normal"], fontName="DejaVuSans", fontSize=8, leading=10),
        "CellBold": ParagraphStyle("CellBold", parent=base["Normal"], fontName="DejaVuSans-Bold", fontSize=8, leading=10),
    }


def _brand_header(company: Company, title: str, number: str, styles: dict[str, ParagraphStyle]) -> Table:
    data = [
        [
            Paragraph(f"{title} N {number}", styles["Title"]),
            Paragraph(f"{_today_ru()}<br/>{company.data['phone']}<br/>{company.data['email']}", styles["HeaderMeta"]),
        ],
        [
            Paragraph(company.data["full_name"], styles["HeaderMeta"]),
            Paragraph("Двери, доставка, монтаж, документы", styles["HeaderMeta"]),
        ],
    ]
    table = Table(data, colWidths=[112 * mm, 57 * mm])
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#1F2937")),
                ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#1F2937")),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 7),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
            ]
        )
    )
    return table


def _payment_qr_block(qr_path: Path, styles: dict[str, ParagraphStyle]) -> Table:
    text = (
        "QR-код для оплаты счета. Клиент открывает приложение банка, выбирает оплату по QR "
        "и проверяет сумму, получателя и назначение платежа перед оплатой."
    )
    data = [[Paragraph(text, styles["Body"]), Image(str(qr_path), width=36 * mm, height=36 * mm)]]
    table = Table(data, colWidths=[120 * mm, 45 * mm])
    table.setStyle(
        TableStyle(
            [
                ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#CBD5E1")),
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#F8FAFC")),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    return table


def _items_table(items: list[dict[str, str]], styles: dict[str, ParagraphStyle]) -> Table:
    data = [[_p("N", styles["CellBold"]), _p("Наименование", styles["CellBold"]), _p("Кол-во", styles["CellBold"]), _p("Ед.", styles["CellBold"]), _p("Цена", styles["CellBold"]), _p("Сумма", styles["CellBold"])]]
    for idx, item in enumerate(items, start=1):
        data.append(
            [
                _p(str(idx), styles["Cell"]),
                _p(item.get("name", ""), styles["Cell"]),
                _p(item.get("qty", "1"), styles["Cell"]),
                _p(item.get("unit", "усл."), styles["Cell"]),
                _p(_format_money(_decimal(item.get("price", "0"))), styles["Cell"]),
                _p(_format_money(_decimal(item.get("total", "0"))), styles["Cell"]),
            ]
        )
    table = Table(data, colWidths=[8 * mm, 78 * mm, 18 * mm, 15 * mm, 25 * mm, 25 * mm])
    table.setStyle(_table_style())
    return table


def _kv_table(rows: list[tuple[str, str]], styles: dict[str, ParagraphStyle]) -> Table:
    data = [[_p(key, styles["CellBold"]), _p(value or "-", styles["Cell"])] for key, value in rows]
    table = Table(data, colWidths=[45 * mm, 124 * mm])
    table.setStyle(_table_style())
    return table


def _table_style() -> TableStyle:
    return TableStyle(
        [
            ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#B8B8B8")),
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F2F4F7")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]
    )


def _contract_terms(parsed: ParsedInput, styles: dict[str, ParagraphStyle]) -> list[object]:
    fields = parsed.fields
    return [
        Paragraph("Условия", styles["Section"]),
        Paragraph(fields.get("payment_terms") or "Оплата производится на основании счета. Размер предоплаты и окончательного платежа согласуется сторонами.", styles["Body"]),
        Paragraph(fields.get("term") or "Сроки поставки и монтажа согласуются после подтверждения заказа и поступления предоплаты.", styles["Body"]),
        Paragraph("Приемка товара и работ оформляется актом. Заказчик обеспечивает готовность объекта к монтажу.", styles["Body"]),
    ]


def _door_contract_terms(parsed: ParsedInput, styles: dict[str, ParagraphStyle]) -> list[object]:
    fields = parsed.fields
    blocks: list[object] = [
        Paragraph("3. Замер, технические параметры и подготовка проема", styles["Section"]),
        Paragraph("- Размеры, сторона открывания, отделка, комплектация и иные параметры фиксируются в акте замера, спецификации или ином согласованном документе.", styles["Body"]),
        Paragraph("- Заказчик подтверждает корректность исходных данных и обязан сообщить обо всех особенностях объекта, влияющих на монтаж.", styles["Body"]),
        Paragraph("- Заказчик обеспечивает готовность проема, свободный доступ к месту установки, возможность подъема изделия и наличие условий для выполнения работ.", styles["Body"]),
        Paragraph("4. Права и обязанности Исполнителя", styles["Section"]),
        Paragraph("- Поставить дверной блок, комплектующие и выполнить согласованные работы в объеме, предусмотренном договором и спецификацией.", styles["Body"]),
        Paragraph("- Использовать материалы и комплектующие, соответствующие модели, конструктиву и согласованной комплектации.", styles["Body"]),
        Paragraph("- Уведомлять Заказчика о выявленных обстоятельствах, влияющих на сроки, стоимость или возможность монтажа.", styles["Body"]),
        Paragraph("- Приостановить работы до устранения препятствий, если объект не готов или выполнение монтажа может привести к повреждению изделия либо имущества.", styles["Body"]),
        Paragraph("5. Права и обязанности Заказчика", styles["Section"]),
        Paragraph("- Предоставить достоверные сведения о проеме, параметрах объекта и обеспечить допуск на объект в согласованное время.", styles["Body"]),
        Paragraph("- Обеспечить сохранность рабочей зоны и условия для транспортировки изделия к месту монтажа.", styles["Body"]),
        Paragraph("- Принять изделие и работы, подписать документы приемки либо своевременно направить письменные мотивированные замечания.", styles["Body"]),
        Paragraph("- Своевременно оплатить стоимость двери, доставки, монтажа и дополнительных работ.", styles["Body"]),
        Paragraph("6. Сроки исполнения", styles["Section"]),
        Paragraph(fields.get("term") or "Срок поставки и выполнения работ согласуется после подтверждения заказа, результатов замера, комплектации и поступления предусмотренной оплаты.", styles["Body"]),
        Paragraph("Сроки подлежат переносу при отсутствии готовности объекта, задержке оплаты, невозможности доступа на объект или необходимости дополнительного согласования конструкции и отделки.", styles["Body"]),
        Paragraph("7. Порядок расчетов", styles["Section"]),
        Paragraph(fields.get("payment_terms") or "Оплата производится на основании счета Исполнителя. Размер аванса и окончательного платежа определяется согласованными условиями сделки.", styles["Body"]),
        Paragraph("Дополнительные работы, включая демонтаж, подъем, расширение проема, нестандартную доставку, доборы и отделку откосов, оплачиваются дополнительно после согласования.", styles["Body"]),
        Paragraph("8. Доставка, монтаж и дополнительные работы", styles["Section"]),
        Paragraph("- Стандартный монтаж включается в стоимость только в случаях, прямо согласованных сторонами; иные работы и услуги отражаются отдельно.", styles["Body"]),
        Paragraph("- Монтаж выполняется после подтверждения готовности объекта и соблюдения технических требований к проему.", styles["Body"]),
        Paragraph("- При невозможности завершить работы по причинам, зависящим от Заказчика, Исполнитель вправе оформить частичное исполнение и перенести остаток работ.", styles["Body"]),
        Paragraph("9. Сдача и приемка", styles["Section"]),
        Paragraph("- Поставка подтверждается накладной, УПД или иным документом передачи товара.", styles["Body"]),
        Paragraph("- Выполнение монтажных и дополнительных работ подтверждается актом выполненных работ или иным документом приемки.", styles["Body"]),
        Paragraph("- Заказчик обязан проверить комплектность, внешний вид, работоспособность замков и фурнитуры в момент приемки.", styles["Body"]),
        Paragraph("- После подписания приемочных документов претензии по явным недостаткам и внешнему виду принимаются только при наличии отметки в документах.", styles["Body"]),
        Paragraph("10. Гарантия и правила эксплуатации", styles["Section"]),
        Paragraph("Гарантийные обязательства действуют при соблюдении правил эксплуатации, хранения, монтажа, технического обслуживания и требований паспорта изделия.", styles["Body"]),
        Paragraph("Гарантия не распространяется на механические повреждения, самостоятельный ремонт, нарушение условий эксплуатации, воздействие влаги, конденсата и установку не предусмотренных конструкцией элементов.", styles["Body"]),
        Paragraph("11. Ответственность сторон", styles["Section"]),
        Paragraph("Стороны несут ответственность за нарушение обязательств по договору в соответствии с законодательством Российской Федерации.", styles["Body"]),
        Paragraph("Исполнитель не отвечает за скрытые дефекты проема, стен, инженерных сетей и иные обстоятельства объекта, которые не могли быть выявлены до начала работ.", styles["Body"]),
        Paragraph("12. Форс-мажор", styles["Section"]),
        Paragraph("Стороны освобождаются от ответственности за неисполнение обязательств вследствие чрезвычайных и непредотвратимых обстоятельств, возникших после заключения договора.", styles["Body"]),
        Paragraph("13. Разрешение споров и заключительные положения", styles["Section"]),
        Paragraph("Споры разрешаются путем переговоров, а при недостижении соглашения — в судебном порядке по месту нахождения Исполнителя, если иное не предусмотрено обязательными нормами права.", styles["Body"]),
        Paragraph("Спецификации, счета, акты замера, акты выполненных работ, гарантийные документы и иная переписка сторон являются неотъемлемой частью договора.", styles["Body"]),
    ]
    return blocks


def _extra_sections(doc_type: str, company: Company, parsed: ParsedInput, styles: dict[str, ParagraphStyle]) -> list[object]:
    fields = parsed.fields
    if doc_type == "door_offer":
        return [
            Paragraph("Условия", styles["Section"]),
            Paragraph(fields.get("payment_terms") or "Оплата производится на основании счета. Размер предоплаты и окончательного платежа согласуется сторонами.", styles["Body"]),
            Paragraph(fields.get("term") or "Срок поставки и монтажа согласуется после подтверждения заказа и готовности объекта.", styles["Body"]),
            Paragraph("Если иное не согласовано отдельно, стандартный монтаж включен в стоимость двери.", styles["Body"]),
            Paragraph(
                "Документ является коммерческим предложением для согласования комплектации и стоимости. "
                "При изменении размеров, отделки, фурнитуры, логистики или состава работ расчет пересматривается.",
                styles["Body"],
            ),
        ]
    if doc_type == "measurement_estimate":
        return [
            Paragraph("Примечания к смете", styles["Section"]),
            Paragraph(
                "Смета отражает расчет стоимости изделий, материалов и работ по данным замера на текущую дату. "
                "Документ не подтверждает факт выполнения работ и не заменяет акт приемки.",
                styles["Body"],
            ),
            Paragraph(
                "Итоговая стоимость уточняется при изменении комплектации, размеров проема, способа монтажа, "
                "необходимости демонтажа, подъема или дополнительных материалов.",
                styles["Body"],
            ),
        ]
    if doc_type == "door_contract":
        return _door_contract_terms(parsed, styles)
    if doc_type == "contract":
        return _contract_terms(parsed, styles)
    if doc_type == "warranty":
        blocks: list[object] = [
            Paragraph("Входные двери. Паспорт на блок дверной стальной", styles["Section"]),
            Paragraph("Сведения об изделии", styles["Section"]),
            _kv_table(
                [
                    ("Производитель", WARRANTY_MANUFACTURER),
                    ("Срок гарантии", fields.get("warranty_period", "12 месяцев")),
                    ("Сервис", "service@tk23.ru"),
                ],
                styles,
            ),
        ]
        blocks.append(Paragraph("1. Общие сведения об изделии", styles["Section"]))
        for line in PASSPORT_GENERAL_INFO:
            blocks.append(Paragraph(f"- {line}", styles["Body"]))
        blocks.append(Paragraph("2. Основные технические данные и характеристики", styles["Section"]))
        for line in PASSPORT_TECH_SPECS:
            blocks.append(Paragraph(f"- {line}", styles["Body"]))
        blocks.append(Paragraph("3. Комплект поставки", styles["Section"]))
        for line in PASSPORT_PACKAGE:
            blocks.append(Paragraph(f"- {line}", styles["Body"]))
        blocks.append(Paragraph("4. Требования безопасности", styles["Section"]))
        for line in PASSPORT_SAFETY:
            blocks.append(Paragraph(f"- {line}", styles["Body"]))
        blocks.append(Paragraph("5. Транспортирование и хранение", styles["Section"]))
        for line in PASSPORT_TRANSPORT:
            blocks.append(Paragraph(f"- {line}", styles["Body"]))
        blocks.append(Paragraph("6. Правила эксплуатации и технического обслуживания", styles["Section"]))
        for line in PASSPORT_OPERATION:
            blocks.append(Paragraph(f"- {line}", styles["Body"]))
        blocks.append(Paragraph("Запрещается", styles["Section"]))
        for line in PASSPORT_PROHIBITED:
            blocks.append(Paragraph(f"- {line}", styles["Body"]))
        blocks.append(Paragraph("7. Инструкция по монтажу", styles["Section"]))
        for line in PASSPORT_INSTALLATION:
            blocks.append(Paragraph(f"- {line}", styles["Body"]))
        blocks.append(Paragraph("8. Гарантийные обязательства", styles["Section"]))
        for line in WARRANTY_COVERAGE:
            blocks.append(Paragraph(f"- {line}", styles["Body"]))
        blocks.append(Paragraph("8.1. Условия эксплуатации и обслуживания", styles["Section"]))
        for line in WARRANTY_MAINTENANCE:
            blocks.append(Paragraph(f"- {line}", styles["Body"]))
        blocks.append(Paragraph("8.2. Случаи, не подпадающие под гарантию", styles["Section"]))
        for line in WARRANTY_EXCLUSIONS:
            blocks.append(Paragraph(f"- {line}", styles["Body"]))
        blocks.append(Paragraph("9. Свидетельство о приемке", styles["Section"]))
        blocks.append(Paragraph("Блок дверной металлический соответствует требованиям паспорта и признан годным к эксплуатации при соблюдении правил монтажа и обслуживания.", styles["Body"]))
        blocks.append(Paragraph("Дата выпуска: ____________________    Контролер ОТК: ____________________", styles["Body"]))
        blocks.append(Paragraph("Дата установки: ____________________", styles["Body"]))
        return blocks
    if doc_type == "measurement_act":
        return [
            Paragraph("Подтверждение замера", styles["Section"]),
            Paragraph(
                "Заказчик подтверждает корректность указанных размеров и технических условий на дату замера. "
                "Документ фиксирует исходные данные для расчета и подбора изделия.",
                styles["Body"],
            ),
        ]
    if doc_type == "installation_act":
        return [
            Paragraph("Подтверждение приемки", styles["Section"]),
            Paragraph(
                "Монтажные работы выполнены, изделие установлено и передано Заказчику. "
                "Претензий по объему и качеству на момент подписания акта не заявлено.",
                styles["Body"],
            ),
        ]
    if doc_type == "act":
        return [
            Paragraph("Подтверждение приемки", styles["Section"]),
            Paragraph(
                "Работы, услуги или поставка приняты Заказчиком в полном объеме. "
                "Претензий по срокам, объему и качеству на дату подписания акта стороны не имеют.",
                styles["Body"],
            ),
        ]
    return []


def _intro_text(doc_type: str, parsed: ParsedInput, ai_text: str | None) -> str:
    if ai_text:
        return ai_text
    if doc_type in {"door_offer", "offer"}:
        template = parsed.fields.get("template", "стандарт")
        return (
            f"Предлагаем поставку дверей. Стандартный монтаж включен в стоимость изделия. Формат предложения: {template}. "
            "Ниже приведены согласуемые параметры, комплектация и расчет стоимости."
        )
    if doc_type == "measurement_estimate":
        return (
            "Смета составлена на основании данных замера и выбранной комплектации. "
            "Документ фиксирует расчет стоимости, а не факт выполнения работ. "
            "Стандартный монтаж включается в стоимость двери, если отдельно не оговорено иное."
        )
    if doc_type == "measurement_act":
        return "Акт замера фиксирует фактические размеры, параметры проема и технические условия объекта."
    if doc_type == "installation_act":
        return "Акт подтверждает выполнение монтажных работ и передачу результата Заказчику."
    if doc_type == "act":
        return "Акт подтверждает сдачу и приемку выполненных работ, оказанных услуг или поставленного товара."
    if doc_type == "claim_reply":
        return parsed.fields.get("claim_text") or "Претензия рассмотрена. Ответ подготовлен с учетом предоставленных данных."
    if doc_type == "official_letter":
        return parsed.fields.get("description") or parsed.fields.get("subject", "")
    return parsed.fields.get("subject", "")


def _spec_rows(fields: dict[str, str]) -> list[tuple[str, str]]:
    mapping = [
        ("Адрес объекта", fields.get("object_address") or fields.get("counterparty_address", "")),
        ("Модель", fields.get("model", "")),
        ("Размер", fields.get("size") or fields.get("opening_size", "")),
        ("Толщина стены", fields.get("wall_depth", "")),
        ("Цвет/отделка", fields.get("color", "")),
        ("Открывание", fields.get("opening_side", "")),
        ("Доставка", fields.get("delivery", "")),
        ("Монтаж", fields.get("installation", "")),
        ("Гарантия", fields.get("warranty_period", "")),
        ("Комментарий", fields.get("description", "")),
    ]
    return [(key, value) for key, value in mapping if value]


def _display_items(doc_type: str, parsed: ParsedInput) -> list[dict[str, str]]:
    if doc_type in {"measurement_act", "warranty", "claim_reply", "official_letter"} and _looks_like_placeholder_items(parsed):
        return []
    return parsed.items


def _looks_like_placeholder_items(parsed: ParsedInput) -> bool:
    if len(parsed.items) != 1:
        return False
    item = parsed.items[0]
    if _decimal(item.get("price", "0")) != 0 or _decimal(item.get("total", "0")) != 0:
        return False
    name = item.get("name", "").strip()
    placeholder_names = {
        "Услуги по заявке",
        parsed.fields.get("subject", "").strip(),
        parsed.fields.get("description", "").strip(),
    }
    return name in {value for value in placeholder_names if value}


def _party_label(doc_type: str) -> str:
    if doc_type in {"measurement_act", "installation_act", "act", "door_contract", "contract"}:
        return "Заказчик"
    return "Клиент"


def _spec_heading(doc_type: str) -> str:
    if doc_type == "measurement_estimate":
        return "Исходные данные по объекту"
    if doc_type == "measurement_act":
        return "Результаты замера"
    if doc_type in {"installation_act", "act"}:
        return "Параметры объекта и работ"
    return "Параметры"


def _items_heading(doc_type: str) -> str:
    if doc_type in {"door_offer", "offer"}:
        return "Состав предложения"
    if doc_type == "measurement_estimate":
        return "Сметный расчет"
    if doc_type in {"installation_act", "act"}:
        return "Перечень выполненных работ"
    return "Позиции"


def _signatures_table(company: Company, fields: dict[str, str], styles: dict[str, ParagraphStyle]) -> Table:
    left = f"{company.data['manager_title']} __________________ {company.data['manager_name']}"
    right_name = fields.get("counterparty_manager") or fields.get("counterparty_name", "Заказчик")
    right = f"Заказчик __________________ {right_name}"
    table = Table([[Paragraph(left, styles["Body"]), Paragraph(right, styles["Body"])]], colWidths=[84 * mm, 84 * mm])
    table.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                ("TOPPADDING", (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
            ]
        )
    )
    return table


def _company_rows(company: Company) -> list[tuple[str, str]]:
    data = company.data
    return [
        ("ИНН/КПП", f"{data['inn']} / {data.get('kpp', '-')}"),
        ("Р/с", data["bank_account"]),
        ("Банк", data["bank_name"]),
        ("К/с", data["corr_account"]),
        ("БИК", data["bik"]),
        ("Телефон", data["phone"]),
        ("Email", data["email"]),
    ]


def _counterparty_line(fields: dict[str, str]) -> str:
    name = fields.get("counterparty_name", "Контрагент")
    inn = fields.get("counterparty_inn")
    return f"{name}, ИНН {inn}" if inn else name


def _items_total(items: list[dict[str, str]]) -> Decimal:
    total = Decimal("0")
    for item in items:
        total += _decimal(item.get("total", "0"))
    return total


def _decimal(value: str) -> Decimal:
    try:
        clean = "".join(ch for ch in str(value).replace(",", ".") if ch.isdigit() or ch in ".-")
        return Decimal(clean or "0")
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _format_money(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.01'))}"


def _today_ru() -> str:
    return date.today().strftime("%d.%m.%Y")


def _p(value: str, style: ParagraphStyle) -> Paragraph:
    escaped = str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return Paragraph(escaped, style)
