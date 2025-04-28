from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional, Union
import json # Needed for generating the schema string later

# --- Nested Models ---

class ContactInfo(BaseModel):
    """Generalized contact details for vendor or customer."""
    name: Optional[str] = Field(None, description="Name of the company or person.")
    address: Optional[str] = Field(None, description="Full address.")
    email: Optional[str] = Field(None, description="Contact email address.")
    phone: Optional[str] = Field(None, description="Contact phone number.")
    vat_id: Optional[str] = Field(None, description="VAT identification number (e.g., USt-IdNr.), if provided.")
    customer_id: Optional[str] = Field(None, description="Customer identification number (e.g., Kundennr.), if provided.")
    contact_person: Optional[str] = Field(None, description="Specific contact person name, if mentioned.")
    # Add catch-all dictionary
    other_data: Optional[Dict[str, Any]] = Field(None, description="Any other relevant line item details not covered by specific fields (e.g., SKU, specific tax/discount info, product code).")
    
class SimpleLineItem(BaseModel):
    """Core details for a single line item, allowing for variations."""
    description: Optional[str] = Field(None, description="Description of the item or service (e.g., 'Service Description', 'Leistungsbeschreibung').")
    quantity: Optional[Union[float, int, str]] = Field(None, description="Quantity (e.g., 1, 1.00, '1'). Use string if format is unusual.")
    unit_price: Optional[float] = Field(None, description="Price/cost per unit (e.g., 'Rate/Price', 'Unit Cost', 'Betrag -ohne MwSt.-' if applicable per unit). Might be absent.")
    line_total: Optional[float] = Field(None, description="Total amount for this line item before tax (e.g., 'Sub Total', 'Amount', 'Gesamtbetrag -ohne MwSt.-').")
    # Add catch-all dictionary
    other_data: Optional[Dict[str, Any]] = Field(None, description="Any other relevant line item details not covered by specific fields (e.g., SKU, specific tax/discount info, product code).")
    
# --- Main Generalized Invoice Schema ---

class GeneralizedInvoiceData(BaseModel):
    """A generalized, structured representation of common invoice data."""
    invoice_number: Optional[str] = Field(None, description="The main invoice identifier (e.g., 'Invoice Number', 'Rechnungsnummer').")
    invoice_date: Optional[str] = Field(None, description="Date the invoice was issued (e.g., 'Invoice Date', 'Issue Date', 'Datum'). Use original format.")
    due_date: Optional[str] = Field(None, description="Payment due date (e.g., 'Due Date'). May need inference from payment terms. Use original format.")
    invoice_period: Optional[str] = Field(None, description="Billing period covered by the invoice, if specified (e.g., 'Invoice Period', 'Rechnungsperiode').")

    vendor: Optional[ContactInfo] = Field(None, description="Details of the company sending the invoice (e.g., 'From', Sender).")
    customer: Optional[ContactInfo] = Field(None, description="Details of the company/person receiving the invoice (e.g., 'To', 'Bill To', Recipient).")

    line_items: Optional[List[SimpleLineItem]] = Field(None, description="List of items/services being billed.")

    subtotal: Optional[float] = Field(None, description="Total amount before taxes (e.g., 'Sub Total', 'Gesamtbetrag -ohne MwSt.-').")
    tax_amount: Optional[float] = Field(None, description="Total tax amount charged (e.g., 'Tax', 'MwSt.').")
    tax_rate: Optional[Union[str, float]] = Field(None, description="Tax rate applied, if specified (e.g., 'Tax Rate', '19 %', '0%'). Can be string or float.")
    total_amount: Optional[float] = Field(None, description="The final total amount including tax (e.g., 'Total', 'Total Due', 'Amount Due', 'Gesamtbetrag inkl. MwSt.').")
    currency: Optional[str] = Field(None, description="Currency symbol or code (e.g., '$', '€', 'USD', 'EUR'). Infer if possible.")

    payment_status: Optional[str] = Field(None, description="Indicates if the invoice is 'PAID', 'Due', etc., if specified.")
    # Optional fields for less common but potentially useful info:
    order_number: Optional[str] = Field(None, description="Order number associated with the invoice, if present.")
    payment_terms_or_notes: Optional[str] = Field(None, description="Combines payment terms (e.g., 'Zahlungsbedingungen'), bank details (IBAN/BIC), instructions, or other relevant notes found on the invoice.")
    # Add catch-all dictionary
    other_data: Optional[Dict[str, Any]] = Field(None, description="Any other relevant line item details not covered by specific fields (e.g., SKU, specific tax/discount info, product code).")
    
    class Config:
        # Example for generating schema documentation if needed
        # This helps understand the structure but isn't strictly required for validation
        json_schema_extra = {
            "example": {
                "invoice_number": "123100401",
                "invoice_date": "1. März 2024",
                "due_date": "Sofort", # Inferred from terms
                "invoice_period": "01.02.2024 - 29.02.2024",
                "vendor": {
                    "name": "CPB Software (Germany) GmbH",
                    "address": "Im Bruch 3 - 63897 Miltenberg/Main",
                    "email": None,
                    "phone": "+49 9371 9786-0",
                    "vat_id": "DE199378386",
                    "customer_id": None,
                    "contact_person": "Stefanie Müller",
                    "other_data": {"website": "https://www.examplevendor.com"}
                },
                "customer": {
                    "name": "Musterkunde AG",
                    "address": "Mr. John Doe\nMusterstr. 23\n12345 Musterstadt",
                    "email": None,
                    "phone": None,
                    "vat_id": None,
                    "customer_id": "12345",
                    "contact_person": "Mr. John Doe",
                    "other_data": {"website": "https://www.examplecustumer.com"}
                },
                "line_items": [
                    {
                        "description": "Basic Fee wmView",
                        "quantity": 1,
                        "unit_price": 130.00,
                        "line_total": 130.00,
                        "other_data": {"product_code": "SVC-WD-01"}
                    },
                    {
                        "description": "Transaction Fee T1",
                        "quantity": 14,
                        "unit_price": 0.58,
                        "line_total": 8.12,
                        "other_data": {"product_code": "SVC-WD-02"}
                    },
                    {
                        "description": "Transaction Fee T3",
                        "quantity": 162,
                        "unit_price": 1.50,
                        "line_total": 243.00,
                        "other_data": {"product_code": "SVC-WD-03"}
                    }
                ],
                "subtotal": 381.12,
                "tax_amount": 72.41,
                "tax_rate": "19 %",
                "total_amount": 453.53,
                "currency": "€",
                "payment_status": "Due", # Inferred
                "order_number": None,
                "payment_terms_or_notes": "Terms of Payment: Immediate payment without discount. Any bank charges must be paid by the invoice recipient.\nPlease credit the amount invoiced to IBAN DE29 1234 5678 9012 3456 78 | BIC GENODE51MIC (SEPA Credit Transfer)"
            }
        }

def get_invoice_schema_json_string():
    schema_json_string = json.dumps(GeneralizedInvoiceData.model_json_schema(), indent=2)
    return schema_json_string
