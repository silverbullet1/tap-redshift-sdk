"""Redshift tap class."""

from __future__ import annotations

from singer_sdk import SQLTap
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk._singerlib import CatalogEntry, MetadataMapping, Schema

from tap_redshift.client import RedshiftStream
from functools import cached_property
from tap_redshift.client import RedshiftStream, RedshiftConnector
import typing as t

import sqlalchemy
from sqlalchemy.engine import Engine
from singer_sdk._singerlib import CatalogEntry, MetadataMapping, Schema
from sqlalchemy.engine import Inspector
from sqlalchemy.engine.url import URL
from sqlalchemy.engine.url import make_url


class TapRedshift(SQLTap):
    """Redshift tap class."""
    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        """Initialize the SQL tap.

        The SQLTap initializer additionally creates a cache variable for _catalog_dict.

        Args:
            *args: Positional arguments for the Tap initializer.
            **kwargs: Keyword arguments for the Tap initializer.
        """
        self._catalog_dict: dict | None = None
        super().__init__(*args, **kwargs)

        # There's a few ways to do this in JSON Schema but it is schema draft dependent.
        # https://stackoverflow.com/questions/38717933/jsonschema-attribute-conditionally-required # noqa: E501
        assert (self.config.get("sqlalchemy_url") is not None) or (
            self.config.get("host") is not None
            and self.config.get("port") is not None
            and self.config.get("database") is not None
            and self.config.get("user") is not None
            and self.config.get("password") is not None
        ), (
            "Need either the sqlalchemy_url to be set or host, db, port, user"
            + "password to be set"
        )

    name = "tap-redshift"
    default_stream_class = RedshiftStream

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "host",
            th.StringType,
            description=(
                    "Hostname for postgres instance. "
                    + "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "port",
            th.IntegerType,
            default=5439,
            description=(
                    "The port on which postgres is awaiting connection. "
                    + "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "username",
            th.StringType,
            description=(
                    "User name used to authenticate. "
                    + "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "password",
            th.StringType,
            description=(
                "Password used to authenticate. "
                "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "database",
            th.StringType,
            description=(
                    "Database name. "
                    + "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            description=(
                    "SQLAlchemy connection string. "
                    + "This will override using host, user, password, port, "
                    + "dialect, and all ssl settings. Note that you must escape password "
                    + "special characters properly. See "
                    + "https://docs.sqlalchemy.org/en/20/core/engines.html#escaping-special-characters-such-as-signs-in-passwords"
            # noqa: E501
            ),
        ),
    ).to_dict()


    @cached_property
    def connector(self) -> RedshiftConnector:
        """Get a configured connector for this Tap.

        Connector is a singleton (one instance is used by the Tap and Streams).

        """
        if self.config.get("sqlalchemy_url"):
            url = make_url(self.config["sqlalchemy_url"])
        else:
            url = URL.create(
                drivername='redshift+redshift_connector',  # indicate redshift_connector driver and dialect will be used
                host=self.config['host'],  # Amazon Redshift host
                port=self.config['port'],  # Amazon Redshift port
                database=self.config['database'],  # Amazon Redshift database
                username=self.config['user'],  # Amazon Redshift username
                password=self.config['password']  # Amazon Redshift password
            )
        return RedshiftConnector(
            config=dict(self.config),
            sqlalchemy_url=url.render_as_string(hide_password=False),
        )

    def discover_catalog_entry(
        self,
        engine: Engine,  # noqa: ARG002
        inspected: Inspector,
        schema_name: str,
        table_name: str,
        is_view: bool,  # noqa: FBT001
    ) -> CatalogEntry:
        """Create `CatalogEntry` object for the given table or a view.

        Args:
            engine: SQLAlchemy engine
            inspected: SQLAlchemy inspector instance for engine
            schema_name: Schema name to inspect
            table_name: Name of the table or a view
            is_view: Flag whether this object is a view, returned by `get_object_names`

        Returns:
            `CatalogEntry` object for the given table or a view
        """
        # Initialize unique stream name
        unique_stream_id = self.connector.get_fully_qualified_name(
            db_name=None,
            schema_name=schema_name,
            table_name=table_name,
            delimiter="-",
        )

        # Detect key properties
        possible_primary_keys: list[list[str]] = []
        pk_def = inspected.get_pk_constraint(table_name, schema=schema_name)
        if pk_def and "constrained_columns" in pk_def:
            possible_primary_keys.append(pk_def["constrained_columns"])

        possible_primary_keys.extend(
            index_def["column_names"]
            for index_def in inspected.get_indexes(table_name, schema=schema_name)
            if index_def.get("unique", False)
        )
        key_properties = next(iter(possible_primary_keys), None)
        # Initialize columns list
        table_schema = th.PropertiesList()
        for column_def in inspected.get_columns(table_name, schema=schema_name):
            column_name = column_def["name"]
            is_nullable = column_def.get("nullable", False)
            jsonschema_type: dict = self.connector.to_jsonschema_type(
                t.cast(sqlalchemy.types.TypeEngine, column_def["type"]),
            )
            table_schema.append(
                th.Property(
                    name=column_name,
                    wrapped=th.CustomType(jsonschema_type),
                    required=not is_nullable,
                ),
            )
        schema = table_schema.to_dict()

        # Initialize available replication methods
        addl_replication_methods: list[str] = [""]  # By default an empty list.
        # Notes regarding replication methods:
        # - 'INCREMENTAL' replication must be enabled by the user by specifying
        #   a replication_key value.
        # - 'LOG_BASED' replication must be enabled by the developer, according
        #   to source-specific implementation capabilities.
        replication_method = next(reversed(["FULL_TABLE", *addl_replication_methods]))

        # Create the catalog entry object
        return CatalogEntry(
            tap_stream_id=unique_stream_id,
            stream=unique_stream_id,
            table=table_name,
            key_properties=key_properties,
            schema=Schema.from_dict(schema),
            is_view=is_view,
            replication_method="INCREMENTAL",
            # replication_method="replication_method",
            metadata=MetadataMapping.get_standard_metadata(
                schema_name=schema_name,
                schema=schema,
                replication_method=replication_method,
                key_properties=key_properties,
                valid_replication_keys=None,  # Must be defined by user
            ),
            database=None,  # Expects single-database context
            row_count=None,
            stream_alias=None,
            replication_key=None,  # Must be defined by user
        )

    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.

        Returns:
            The tap's catalog as a dict
        """
        if self._catalog_dict:
            return self._catalog_dict

        if self.input_catalog:
            return self.input_catalog.to_dict()

        result: dict[str, list[dict]] = {"streams": []}
        result["streams"].extend(self.discover_catalog_entries())

        self._catalog_dict: dict = result
        return self._catalog_dict

    def discover_streams(self) -> list[Stream]:
        """Initialize all available streams and return them as a list.

        Returns:
            List of discovered Stream objects.
        """
        return [
            RedshiftStream(self, catalog_entry, connector=self.connector)
            for catalog_entry in self.catalog_dict["streams"]
        ]

    # overridden to select specific tables in input and filter out the information_schema from catalog discovery
    def discover_catalog_entries(self) -> list[dict]:
        """Return a list of catalog entries from discovery.

        Returns:
            The discovered catalog entries as a list.
        """
        result: list[dict] = []
        tables = [t.lower() for t in self.config.get("tables", [])]
        engine = self.connector.create_sqlalchemy_engine()
        inspected = sqlalchemy.inspect(engine)
        if tables:
            schema_names = [table_name.split('.')[0] for table_name in list(self.config['tables'])]
        else:
            schema_names = [
                schema_name
                for schema_name in self.connector.get_schema_names(engine, inspected)
                if schema_name.lower() != "information_schema"
            ]
        for schema_name in schema_names:
            # Iterate through each table and view
            for table_name, is_view in self.connector.get_object_names(
                engine, inspected, schema_name
            ):
                if (not tables) or (f"{schema_name}.{table_name}" in tables):
                    catalog_entry = self.discover_catalog_entry(
                        engine, inspected, schema_name, table_name, is_view
                    )
                    result.append(catalog_entry.to_dict())

        return result

if __name__ == "__main__":
    TapRedshift.cli()
