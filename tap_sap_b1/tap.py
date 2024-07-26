"""SAPB1 tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_sap_b1 import streams


class TapSAPB1(Tap):
    """SAPB1 tap class."""

    name = "tap-sap-b1"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "dbname",
            th.StringType,
        ),
        th.Property(
            "username",
            th.StringType,
        ),
        th.Property(
            "password",
            th.StringType,
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://205.251.136.61:50000/b1s/v1"
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.SAPB1Stream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.InvoicesStream(self),
        ]


if __name__ == "__main__":
    TapSAPB1.cli()
