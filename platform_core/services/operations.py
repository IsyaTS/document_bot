from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.orm import Session

from platform_core.exceptions import PlatformCoreError, TenantContextError
from platform_core.models import Customer, Deal, Document, Employee, InstallationRequest, Product, Purchase, StockItem, StockMovement, Warehouse
from platform_core.tenancy import TenantContext, require_account_id


@dataclass(frozen=True)
class StagnantStockRow:
    stock_item: StockItem
    product: Product
    warehouse: Warehouse
    days_since_movement: int | None
    status: str


class OperationsService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list_products(self, context: TenantContext) -> list[Product]:
        account_id = require_account_id(context)
        return self.session.execute(
            select(Product).where(Product.account_id == account_id).order_by(Product.name.asc(), Product.id.asc())
        ).scalars().all()

    def list_warehouses(self, context: TenantContext) -> list[Warehouse]:
        account_id = require_account_id(context)
        return self.session.execute(
            select(Warehouse).where(Warehouse.account_id == account_id).order_by(Warehouse.name.asc(), Warehouse.id.asc())
        ).scalars().all()

    def list_purchases(self, context: TenantContext) -> list[Purchase]:
        account_id = require_account_id(context)
        return self.session.execute(
            select(Purchase).where(Purchase.account_id == account_id).order_by(Purchase.created_at.desc(), Purchase.id.desc())
        ).scalars().all()

    def list_documents(self, context: TenantContext) -> list[Document]:
        account_id = require_account_id(context)
        return self.session.execute(
            select(Document).where(Document.account_id == account_id).order_by(Document.issued_at.desc(), Document.id.desc())
        ).scalars().all()

    def list_installation_requests(self, context: TenantContext) -> list[InstallationRequest]:
        account_id = require_account_id(context)
        return self.session.execute(
            select(InstallationRequest)
            .where(InstallationRequest.account_id == account_id)
            .order_by(InstallationRequest.scheduled_for.desc(), InstallationRequest.id.desc())
        ).scalars().all()

    def create_product(
        self,
        context: TenantContext,
        *,
        sku: str | None,
        name: str,
        unit: str | None,
        list_price: Decimal,
        cost_price: Decimal,
        min_stock_level: Decimal,
    ) -> Product:
        account_id = require_account_id(context)
        cleaned_name = name.strip()
        if not cleaned_name:
            raise PlatformCoreError("Product name is required.")
        if list_price < 0 or cost_price < 0 or min_stock_level < 0:
            raise PlatformCoreError("Product amounts cannot be negative.")
        product = Product(
            account_id=account_id,
            sku=(sku or "").strip() or None,
            name=cleaned_name,
            unit=(unit or "").strip() or "pcs",
            status="active",
            list_price=list_price,
            cost_price=cost_price,
            min_stock_level=min_stock_level,
        )
        self.session.add(product)
        self.session.flush()
        return product

    def create_warehouse(
        self,
        context: TenantContext,
        *,
        code: str,
        name: str,
        location: str | None,
    ) -> Warehouse:
        account_id = require_account_id(context)
        cleaned_code = code.strip()
        cleaned_name = name.strip()
        if not cleaned_code:
            raise PlatformCoreError("Warehouse code is required.")
        if not cleaned_name:
            raise PlatformCoreError("Warehouse name is required.")
        warehouse = Warehouse(
            account_id=account_id,
            code=cleaned_code,
            name=cleaned_name,
            status="active",
            location=(location or "").strip() or None,
        )
        self.session.add(warehouse)
        self.session.flush()
        return warehouse

    def create_purchase_request(
        self,
        context: TenantContext,
        *,
        supplier_customer_id: int | None,
        warehouse_id: int | None,
        product_id: int,
        quantity: Decimal,
        unit_cost: Decimal,
        notes: str | None = None,
    ) -> Purchase:
        account_id = require_account_id(context)
        product = self._product(account_id, product_id)
        if warehouse_id is not None:
            self._warehouse(account_id, warehouse_id)
        if supplier_customer_id is not None:
            self._customer(account_id, supplier_customer_id)
        if quantity <= 0:
            raise PlatformCoreError("Purchase quantity must be positive.")
        if unit_cost < 0:
            raise PlatformCoreError("Unit cost cannot be negative.")
        purchase = Purchase(
            account_id=account_id,
            supplier_customer_id=supplier_customer_id,
            warehouse_id=warehouse_id,
            purchase_number=f"PR-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
            status="requested",
            ordered_at=datetime.now(timezone.utc),
            currency="RUB",
            total_amount=(quantity * unit_cost),
            notes_json={
                "lines": [
                    {
                        "product_id": product.id,
                        "product_name": product.name,
                        "quantity": str(quantity),
                        "unit_cost": str(unit_cost),
                    }
                ],
                "notes": notes or "",
            },
        )
        self.session.add(purchase)
        self.session.flush()
        return purchase

    def receive_purchase(self, context: TenantContext, purchase_id: int) -> Purchase:
        purchase = self._purchase(context, purchase_id)
        if purchase.status == "received":
            return purchase
        lines = list((purchase.notes_json or {}).get("lines") or [])
        if not lines:
            raise PlatformCoreError("Purchase request has no line items.")
        if purchase.warehouse_id is None:
            raise PlatformCoreError("Purchase warehouse is required before receiving stock.")
        for line in lines:
            product_id = int(line["product_id"])
            quantity = Decimal(str(line["quantity"]))
            unit_cost = Decimal(str(line.get("unit_cost") or "0"))
            stock_item = self.session.execute(
                select(StockItem).where(
                    StockItem.account_id == purchase.account_id,
                    StockItem.warehouse_id == purchase.warehouse_id,
                    StockItem.product_id == product_id,
                )
            ).scalar_one_or_none()
            if stock_item is None:
                stock_item = StockItem(
                    account_id=purchase.account_id,
                    warehouse_id=purchase.warehouse_id,
                    product_id=product_id,
                    quantity_on_hand=Decimal("0"),
                    quantity_reserved=Decimal("0"),
                    min_quantity=Decimal("0"),
                    reorder_quantity=Decimal("0"),
                )
                self.session.add(stock_item)
                self.session.flush()
            stock_item.quantity_on_hand = Decimal(stock_item.quantity_on_hand) + quantity
            stock_item.last_movement_at = datetime.now(timezone.utc)
            self.session.add(
                StockMovement(
                    account_id=purchase.account_id,
                    warehouse_id=purchase.warehouse_id,
                    product_id=product_id,
                    purchase_id=purchase.id,
                    movement_type="purchase_receipt",
                    reference_type="purchase",
                    reference_id=str(purchase.id),
                    quantity_delta=quantity,
                    unit_cost=unit_cost,
                    occurred_at=datetime.now(timezone.utc),
                    notes_json={"purchase_number": purchase.purchase_number},
                )
            )
        purchase.status = "received"
        purchase.received_at = datetime.now(timezone.utc)
        self.session.flush()
        return purchase

    def create_document(
        self,
        context: TenantContext,
        *,
        document_type: str,
        document_number: str | None,
        customer_id: int | None,
        deal_id: int | None,
        status: str,
        issued_at: datetime | None,
        total_amount: Decimal,
        summary: str | None,
    ) -> Document:
        account_id = require_account_id(context)
        if customer_id is not None:
            self._customer(account_id, customer_id)
        if deal_id is not None:
            self._deal(account_id, deal_id)
        if total_amount < 0:
            raise PlatformCoreError("Document total amount cannot be negative.")
        document = Document(
            account_id=account_id,
            customer_id=customer_id,
            deal_id=deal_id,
            document_type=document_type,
            document_number=document_number,
            status=status,
            issued_at=issued_at,
            total_amount=total_amount,
            currency="RUB",
            snapshot_json={"summary": summary or ""},
        )
        self.session.add(document)
        self.session.flush()
        return document

    def create_installation_request(
        self,
        context: TenantContext,
        *,
        customer_id: int | None,
        deal_id: int | None,
        assigned_employee_id: int | None,
        title: str,
        address: str | None,
        scheduled_for: datetime | None,
        notes: str | None,
    ) -> InstallationRequest:
        account_id = require_account_id(context)
        if not title.strip():
            raise PlatformCoreError("Installation request title is required.")
        if customer_id is not None:
            self._customer(account_id, customer_id)
        if deal_id is not None:
            self._deal(account_id, deal_id)
        if assigned_employee_id is not None:
            self._employee(account_id, assigned_employee_id)
        request = InstallationRequest(
            account_id=account_id,
            customer_id=customer_id,
            deal_id=deal_id,
            assigned_employee_id=assigned_employee_id,
            request_number=f"INST-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
            title=title.strip(),
            status="open",
            address=(address or "").strip() or None,
            scheduled_for=scheduled_for,
            notes_json={"notes": notes or ""},
        )
        self.session.add(request)
        self.session.flush()
        return request

    def stagnant_stock(self, context: TenantContext, *, threshold_days: int = 30) -> list[StagnantStockRow]:
        account_id = require_account_id(context)
        items = self.session.execute(
            select(StockItem).where(StockItem.account_id == account_id).order_by(StockItem.id.asc())
        ).scalars().all()
        if not items:
            return []
        product_ids = {item.product_id for item in items}
        warehouse_ids = {item.warehouse_id for item in items}
        product_map = {
            item.id: item
            for item in self.session.execute(
                select(Product).where(Product.account_id == account_id, Product.id.in_(product_ids))
            ).scalars().all()
        }
        warehouse_map = {
            item.id: item
            for item in self.session.execute(
                select(Warehouse).where(Warehouse.account_id == account_id, Warehouse.id.in_(warehouse_ids))
            ).scalars().all()
        }
        now = datetime.now(timezone.utc)
        rows: list[StagnantStockRow] = []
        for item in items:
            if Decimal(item.quantity_on_hand) <= Decimal("0"):
                continue
            last_movement = item.last_movement_at.astimezone(timezone.utc) if item.last_movement_at and item.last_movement_at.tzinfo else (
                item.last_movement_at.replace(tzinfo=timezone.utc) if item.last_movement_at else None
            )
            days_since = None if last_movement is None else max(0, (now - last_movement).days)
            if last_movement is None:
                status = "warning"
            elif days_since >= threshold_days * 2:
                status = "critical"
            elif days_since >= threshold_days:
                status = "warning"
            else:
                status = "healthy"
            if status == "healthy":
                continue
            rows.append(
                StagnantStockRow(
                    stock_item=item,
                    product=product_map[item.product_id],
                    warehouse=warehouse_map[item.warehouse_id],
                    days_since_movement=days_since,
                    status=status,
                )
            )
        rows.sort(key=lambda item: (-self._status_weight(item.status), -(item.days_since_movement or 0), item.product.name.lower()))
        return rows

    def _purchase(self, context: TenantContext, purchase_id: int) -> Purchase:
        account_id = require_account_id(context)
        purchase = self.session.execute(
            select(Purchase).where(Purchase.account_id == account_id, Purchase.id == purchase_id)
        ).scalar_one_or_none()
        if purchase is None:
            raise TenantContextError("Purchase not found in selected account.")
        return purchase

    def _product(self, account_id: int, product_id: int) -> Product:
        product = self.session.execute(
            select(Product).where(Product.account_id == account_id, Product.id == product_id)
        ).scalar_one_or_none()
        if product is None:
            raise PlatformCoreError("Product not found in selected account.")
        return product

    def _customer(self, account_id: int, customer_id: int) -> Customer:
        customer = self.session.execute(
            select(Customer).where(Customer.account_id == account_id, Customer.id == customer_id)
        ).scalar_one_or_none()
        if customer is None:
            raise PlatformCoreError("Customer not found in selected account.")
        return customer

    def _deal(self, account_id: int, deal_id: int) -> Deal:
        deal = self.session.execute(
            select(Deal).where(Deal.account_id == account_id, Deal.id == deal_id)
        ).scalar_one_or_none()
        if deal is None:
            raise PlatformCoreError("Deal not found in selected account.")
        return deal

    def _employee(self, account_id: int, employee_id: int) -> Employee:
        employee = self.session.execute(
            select(Employee).where(Employee.account_id == account_id, Employee.id == employee_id)
        ).scalar_one_or_none()
        if employee is None:
            raise PlatformCoreError("Employee not found in selected account.")
        return employee

    def _warehouse(self, account_id: int, warehouse_id: int) -> Warehouse:
        warehouse = self.session.execute(
            select(Warehouse).where(Warehouse.account_id == account_id, Warehouse.id == warehouse_id)
        ).scalar_one_or_none()
        if warehouse is None:
            raise PlatformCoreError("Warehouse not found in selected account.")
        return warehouse

    def _status_weight(self, status_code: str) -> int:
        return {"critical": 3, "warning": 2, "healthy": 1}.get(status_code, 0)
