"""REST client handling, including SAPB1Stream base class."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Iterable

import requests
from urllib.parse import parse_qs, urlparse

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class SAPB1Stream(RESTStream):
    """SAPB1 stream class."""

    @property
    def url_base(self) -> str:
        return self.config["api_url"]

    records_jsonpath = "$.value[*]"  # Or override `parse_response`.

    @property
    def authenticator(self):
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        payload = {
            "CompanyDB": self.config.get("dbname"),
            "Password": self.config.get("password"),
            "UserName": self.config.get("username"),
        }
        r = requests.post(f"{self.url_base}/Login", json=payload, verify=False)
        data = r.json()
        self.logger.info(f"Made login request. Response={r.status_code} {r.text}")

        return data.get("SessionId")

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")
        return headers

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: Any | None,
    ) -> Any | None:
        """Return a token for identifying next page or None if no more pages.

        Args:
            response: The HTTP ``requests.Response`` object.
            previous_token: The previous page token value.

        Returns:
            The next pagination token.
        """
        resp = response.json()
        return resp.get("odata.nextLink")

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}

        if self.replication_key:
            replication_date = self.get_starting_timestamp(context)
            if replication_date:
                replication_date = replication_date.strftime("%Y-%m-%d")
                params["$filter"] = f"{self.replication_key} ge '{replication_date}'"

        if next_page_token:
            # Extract the query part from the URL
            parsed_url = urlparse(next_page_token)
            query_params = parsed_url.query

            # Parse the query parameters
            params = parse_qs(query_params)

        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        # TODO: Delete this method if not needed.
        return row

    def build_prepared_request(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> requests.PreparedRequest:
        request = requests.Request(
            *args,
            cookies={"B1SESSION": self.authenticator},
            **kwargs,
        )
        return self.requests_session.prepare_request(request)

    def _request(
        self,
        prepared_request: requests.PreparedRequest,
        context: dict | None,
    ) -> requests.Response:
        """TODO.

        Args:
            prepared_request: TODO
            context: Stream partition or context dictionary.

        Returns:
            TODO
        """
        response = self.requests_session.send(
            prepared_request, timeout=self.timeout, verify=False
        )
        self._write_request_duration_log(
            endpoint=self.path,
            response=response,
            context=context,
            extra_tags=(
                {"url": prepared_request.path_url}
                if self._LOG_REQUEST_METRIC_URLS
                else None
            ),
        )
        self.validate_response(response)
        return response
