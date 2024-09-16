"""Stream type classes for tap-sap-b1."""

from __future__ import annotations

from pathlib import Path

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_sap_b1.client import SAPB1Stream

class InvoicesStream(SAPB1Stream):
    name = "invoices"
    path = "/PurchaseInvoices?$select=DocEntry,DocNum,DocType,DocTotal,DocDate&$orderby=DocDate"
    primary_keys = ["DocNum"]
    replication_key = "DocDate"

    schema = th.PropertiesList(
        th.Property(
            "DocNum",
            th.IntegerType,
        ),
        th.Property(
            "DocEntry",
            th.IntegerType,
        ),
        th.Property(
            "DocType",
            th.StringType,
        ),
        th.Property(
            "DocTotal",
            th.NumberType,
        ),
        th.Property(
            "DocDate",
            th.DateTimeType,
        ),
        th.Property(
            "odata.etag",
            th.StringType,
        ),
    ).to_dict()

    def get_child_context(self, record: dict, context) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "doc_num": record["DocNum"],
        }


class InvoiceDetailsStream(SAPB1Stream):
    name = "invoice_details"
    path = "/SQLQueries('sql02')/List?DocNum={doc_num}"
    primary_keys = ["DocNum"]
    replication_key = None
    parent_stream_type = InvoicesStream

    schema = th.PropertiesList(
        th.Property(
            "CardCode",
            th.StringType,
        ),
        th.Property(
            "CardName",
            th.StringType,
        ),
        th.Property(
            "DocDate",
            th.DateTimeType,
        ),
        th.Property(
            "DocEntry",
            th.IntegerType,
        ),
        th.Property(
            "DocNum",
            th.IntegerType,
        ),
        th.Property(
            "Description",
            th.StringType,
        ),
        th.Property(
            "ItemCode",
            th.StringType,
        ),
        th.Property(
            "ItemGrpCod",
            th.IntegerType,
        ),
        th.Property(
            "ItemGrpNam",
            th.StringType,
        ),
        th.Property(
            "LineNum",
            th.IntegerType,
        ),
        th.Property(
            "LineTotal",
            th.NumberType,
        ),
        th.Property(
            "Price",
            th.NumberType,
        ),
        th.Property(
            "Quantity",
            th.NumberType,
        ),
    ).to_dict()
