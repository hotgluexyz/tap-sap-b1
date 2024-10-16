"""Stream type classes for tap-sap-b1."""

import json
import requests
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_sap_b1.client import SAPB1Stream


class QueryAlreadyExistsException(Exception):
    pass


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


class ItemGroupsQueryStream(SAPB1Stream):
    name = "item_groups_query"
    query_name = "GMEHG01"
    primary_keys = ["DocNum"]
    replication_key = None

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

    @property
    def path(self):
        group_code = self.config.get('group_code')  # format should be an integer
        start_date = self.config.get('start_date')  # format should be "YYYY-MM-DD"
        if 'T' in start_date:
            start_date = start_date.split('T')[0]
        end_date = self.config.get('end_date')  # format should be "YYYY-MM-DD"
        if 'T' in end_date:
            end_date = end_date.split('T')[0]
        return f"/SQLQueries('{self.query_name}')/List?GroupCode={group_code}&StartDate='{start_date}'&EndDate='{end_date}'"
    
    def create_query(self, context):
        url = self.url_base + "/SQLQueries"
        data = {
            "SqlCode": f"{self.query_name}",
            "SqlName": "QryItemGroups",
            "SqlText": "Select T0.[DocEntry],T0.[DocNum],T0.[DocDate],T0.[CardCode],T0.[CardName] , T1.[ItemCode], T1.[Dscription], T1.[LineNum], T1.[Quantity], T1.[Price], T1.[LineTotal]  , T2.[ItmsGrpCod]  , T3.[ItmsGrpNam] from [OPCH] T0   inner join [PCH1] T1 on T0.[DocEntry] = T1.[DocEntry]      inner join [OITM] T2 on T1.[ItemCode] = T2.[ItemCode] inner join [OITB] T3 on T2.[ItmsGrpCod] = T3.[ItmsGrpCod] where T2.[ItmsGrpCod]  = :GroupCode and T0.[DocDate] >= :StartDate and T0.[DocDate] <= :EndDate"
        }
        prepared_request = self.build_prepared_request(method="POST", url=url, data=json.dumps(data), headers=self.http_headers, params=self.get_url_params(context, None))
        response = self._request(prepared_request, context)
        return response
    
    def delete_query(self, context):
        url = self.url_base + f"/SQLQueries('{self.query_name}')"
        prepared_request = self.build_prepared_request(method="DELETE", url=url)
        response = self._request(prepared_request, context)
        return response
    
    def _sync_records(
        self,
        context
    ):
    
        try:
            self.create_query(context)
        except QueryAlreadyExistsException:
            self.delete_query(context)
            self.create_query(context)

        try:
            yield from super()._sync_records(context)
        except Exception as exc:
            raise Exception(f"Sync error during query {self.query_name}. group_code={self.config.get('group_code')}. start_date={self.config.get('start_date')}. end_date={self.config.get('end_date')}") from exc
        finally:
            self.delete_query(context)
    
    def validate_response(self, response: requests.Response) -> None:
        if response.status_code == 400 and response.json().get("error", {}).get("code") == -2035:
            raise QueryAlreadyExistsException()
        return super().validate_response(response)
