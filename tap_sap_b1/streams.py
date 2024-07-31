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
