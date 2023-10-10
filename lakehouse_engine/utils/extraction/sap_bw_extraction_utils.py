"""Utilities module for SAP BW extraction processes."""
from copy import copy
from dataclasses import dataclass
from logging import Logger
from typing import Optional, Tuple

from lakehouse_engine.core.definitions import InputFormat, InputSpec, ReadType
from lakehouse_engine.transformers.aggregators import Aggregators
from lakehouse_engine.utils.extraction.jdbc_extraction_utils import (
    JDBCExtraction,
    JDBCExtractionType,
    JDBCExtractionUtils,
)
from lakehouse_engine.utils.logging_handler import LoggingHandler


@dataclass
class SAPBWExtraction(JDBCExtraction):
    """Configurations available for an Extraction from SAP BW.

    It inherits from SAPBWExtraction configurations, so it can use
    and/or overwrite those configurations.

    These configurations cover:
        latest_timestamp_input_col: the column containing the actrequest timestamp
            in the dataset in latest_timestamp_data_location. Default:
            "actrequest_timestamp".
        act_request_table: the name of the SAP BW activation requests table.
            Composed of database.table. Default: SAPPHA.RSODSACTREQ.
        request_col_name: name of the column having the request to join
            with the activation request table. Default: actrequest.
        act_req_join_condition: the join condition into activation table
            can be changed using this property.
            Default: 'changelog_tbl.request = act_req.request_col_name'.
        odsobject: name of BW Object, used for joining with the activation request
            table to get the max actrequest_timestamp to consider while filtering
            the changelog table.
        include_changelog_tech_cols: whether to include the technical columns
            (usually coming from the changelog) table or not. Default: True.
        extra_cols_act_request: list of columns to be added from act request table.
            It needs to contain the prefix "act_req.". E.g. "act_req.col1
            as column_one, act_req.col2 as column_two".
        get_timestamp_from_act_request: whether to get init timestamp
            from act request table or assume current/given timestamp.
        sap_bw_schema: sap bw schema. Default: SAPPHA.
        max_timestamp_custom_schema: the custom schema to apply on the calculation of
            the max timestamp to consider for the delta extractions.
            Default: timestamp DECIMAL(23,0).
        default_max_timestamp: the timestamp to use as default, when it is not possible
            to derive one.
    """

    latest_timestamp_input_col: str = "actrequest_timestamp"
    act_request_table: str = "SAPPHA.RSODSACTREQ"
    request_col_name: str = "actrequest"
    act_req_join_condition: Optional[str] = None
    odsobject: Optional[str] = None
    include_changelog_tech_cols: bool = True
    extra_cols_act_request: Optional[str] = None
    get_timestamp_from_act_request: bool = False
    sap_bw_schema: str = "SAPPHA"
    max_timestamp_custom_schema: str = "timestamp DECIMAL(15,0)"
    default_max_timestamp: str = "197000000000000"


class SAPBWExtractionUtils(JDBCExtractionUtils):
    """Utils for managing data extraction from particularly relevant JDBC sources."""

    def __init__(self, sap_bw_extraction: SAPBWExtraction):
        """Construct SAPBWExtractionUtils.

        Args:
            sap_bw_extraction: SAP BW Extraction configurations.
        """
        self._LOGGER: Logger = LoggingHandler(__name__).get_logger()
        self._BW_EXTRACTION = sap_bw_extraction
        self._BW_EXTRACTION.changelog_table = self.get_changelog_table()
        self._MAX_TIMESTAMP_QUERY = f""" --# nosec
                (SELECT COALESCE(MAX(timestamp),
                    {self._BW_EXTRACTION.default_max_timestamp}) as timestamp
                FROM {self._BW_EXTRACTION.act_request_table}
                WHERE odsobject = '{self._BW_EXTRACTION.odsobject}'
                 AND operation = 'A' AND status = '0')
            """  # nosec: B608
        super().__init__(sap_bw_extraction)

    def get_changelog_table(self) -> str:
        """Get the changelog table, given an odsobject.

        Returns:
             String to use as changelog_table.
        """
        if (
            self._BW_EXTRACTION.odsobject is not None
            and self._BW_EXTRACTION.changelog_table is None
            and self._BW_EXTRACTION.extraction_type != JDBCExtractionType.INIT.value
        ):
            escaped_odsobject = copy(self._BW_EXTRACTION.odsobject).replace("_", "$_")

            if self._BW_EXTRACTION.sap_bw_schema:
                system_table = f"{self._BW_EXTRACTION.sap_bw_schema}.RSTSODS"
            else:
                system_table = "RSTSODS"

            jdbc_args = {
                "url": self._BW_EXTRACTION.url,
                "table": f""" -- # nosec
                    (SELECT ODSNAME_TECH
                    FROM {system_table}
                    WHERE ODSNAME LIKE '8{escaped_odsobject}$_%'
                        ESCAPE '$' AND USERAPP = 'CHANGELOG' AND VERSION = '000')
                """,  # nosec: B608
                "properties": {
                    "user": self._BW_EXTRACTION.user,
                    "password": self._BW_EXTRACTION.password,
                    "driver": self._BW_EXTRACTION.driver,
                },
            }
            from lakehouse_engine.io.reader_factory import ReaderFactory

            changelog_df = ReaderFactory.get_data(
                InputSpec(
                    spec_id="changelog_table",
                    data_format=InputFormat.JDBC.value,
                    read_type=ReadType.BATCH.value,
                    jdbc_args=jdbc_args,
                )
            )
            changelog_table = (
                f'{self._BW_EXTRACTION.sap_bw_schema}."{changelog_df.first()[0]}"'
                if self._BW_EXTRACTION.sap_bw_schema
                else str(changelog_df.first()[0])
            )
        else:
            changelog_table = (
                self._BW_EXTRACTION.changelog_table
                if self._BW_EXTRACTION.changelog_table
                else f"{self._BW_EXTRACTION.dbtable}_cl"
            )
        self._LOGGER.info(f"The changelog table derived is: '{changelog_table}'")

        return changelog_table

    @staticmethod
    def get_odsobject(input_spec_opt: dict) -> str:
        """Get the odsobject based on the provided options.

        With the table name we may also get the db name, so we need to split.
        Moreover, there might be the need for people to specify odsobject if
        it is different from the dbtable.

        Args:
            input_spec_opt: options from the input_spec.

        Returns:
            A string with the odsobject.
        """
        return str(
            input_spec_opt["dbtable"].split(".")[1]
            if len(input_spec_opt["dbtable"].split(".")) > 1
            else input_spec_opt["dbtable"]
        )

    def _get_init_query(self) -> Tuple[str, str]:
        """Get a query to do an init load based on a DSO on a SAP BW system.

        Returns:
            A query to submit to SAP BW for the initial data extraction. The query
            is enclosed in parentheses so that Spark treats it as a table and supports
            it in the dbtable option.
        """
        if self._BW_EXTRACTION.get_timestamp_from_act_request:
            # check if we are dealing with a DSO of type Write Optimised
            if self._BW_EXTRACTION.dbtable == self._BW_EXTRACTION.changelog_table:
                extraction_query = self._get_init_extraction_query_act_req_timestamp()
            else:
                raise AttributeError(
                    "Not able to get the extraction query. The option "
                    "'get_timestamp_from_act_request' is only "
                    "available/useful for DSOs of type Write Optimised."
                )
        else:
            extraction_query = self._get_init_extraction_query()

        predicates_query = f"""
        (SELECT DISTINCT({self._BW_EXTRACTION.partition_column})
        FROM {self._BW_EXTRACTION.dbtable} t)
        """  # nosec: B608

        return extraction_query, predicates_query

    def _get_init_extraction_query(self) -> str:
        """Get extraction query based on given/current timestamp.

        Returns:
            A query to submit to SAP BW for the initial data extraction.
        """
        changelog_tech_cols = (
            f"""'0' AS request,
                CAST({self._BW_EXTRACTION.extraction_timestamp} AS DECIMAL(15, 0))
                 AS actrequest_timestamp,
                '0' AS datapakid,
                0 AS partno,
                0 AS record,"""
            if self._BW_EXTRACTION.include_changelog_tech_cols
            else f"CAST({self._BW_EXTRACTION.extraction_timestamp} "
            f"AS DECIMAL(15, 0))"
            f" AS actrequest_timestamp,"
        )

        extraction_query = f"""
                (SELECT t.*,
                    {changelog_tech_cols}
                    CAST({self._BW_EXTRACTION.extraction_timestamp}
                        AS DECIMAL(15, 0)) AS extraction_start_timestamp
                FROM {self._BW_EXTRACTION.dbtable} t
                )"""  # nosec: B608

        return extraction_query

    def _get_init_extraction_query_act_req_timestamp(self) -> str:
        """Get extraction query assuming the init timestamp from act_request table.

        Returns:
            A query to submit to SAP BW for the initial data extraction from
            write optimised DSOs, receiving the actrequest_timestamp from
            the activation requests table.
        """
        extraction_query = f"""
            (SELECT t.*,
                act_req.timestamp as actrequest_timestamp,
                CAST({self._BW_EXTRACTION.extraction_timestamp} AS DECIMAL(15, 0))
                 AS extraction_start_timestamp
            FROM {self._BW_EXTRACTION.dbtable} t
            JOIN {self._BW_EXTRACTION.act_request_table} AS act_req ON
                t.request = act_req.{self._BW_EXTRACTION.request_col_name}
            WHERE act_req.odsobject = '{self._BW_EXTRACTION.odsobject}'
                AND operation = 'A' AND status = '0'
            )"""  # nosec: B608

        return extraction_query

    def _get_delta_query(self) -> Tuple[str, str]:
        """Get a delta query for an SAP BW DSO.

        An SAP BW DSO requires a join with a special type of table often called
        activation requests table, in which BW tracks down the timestamps associated
        with the several data loads that were performed into BW. Because the changelog
        table only contains the active request id, and that cannot be sorted by the
        downstream consumers to figure out the latest change, we need to join the
        changelog table with this special table to get the activation requests
        timestamps to then use them to figure out the latest changes in the delta load
        logic afterwards.

        Additionally, we also need to know which was the latest timestamp already loaded
        into the lakehouse bronze layer. The latest timestamp should always be available
        in the bronze dataset itself or in a dataset that tracks down all the actrequest
        timestamps that were already loaded. So we get the max value out of the
        respective actrequest timestamp column in that dataset.

        Returns:
            A query to submit to SAP BW for the delta data extraction. The query
            is enclosed in parentheses so that Spark treats it as a table and supports
            it in the dbtable option.
        """
        if not self._BW_EXTRACTION.min_timestamp:
            from lakehouse_engine.io.reader_factory import ReaderFactory

            latest_timestamp_data_df = ReaderFactory.get_data(
                InputSpec(
                    spec_id="data_with_latest_timestamp",
                    data_format=self._BW_EXTRACTION.latest_timestamp_data_format,
                    read_type=ReadType.BATCH.value,
                    location=self._BW_EXTRACTION.latest_timestamp_data_location,
                )
            )
            min_timestamp = latest_timestamp_data_df.transform(
                Aggregators.get_max_value(
                    self._BW_EXTRACTION.latest_timestamp_input_col
                )
            ).first()[0]
        else:
            min_timestamp = self._BW_EXTRACTION.min_timestamp

        max_timestamp = (
            self._BW_EXTRACTION.max_timestamp
            if self._BW_EXTRACTION.max_timestamp
            else self._get_max_timestamp(self._MAX_TIMESTAMP_QUERY)
        )

        if self._BW_EXTRACTION.act_req_join_condition:
            join_condition = f"{self._BW_EXTRACTION.act_req_join_condition}"
        else:
            join_condition = (
                f"changelog_tbl.request = "
                f"act_req.{self._BW_EXTRACTION.request_col_name}"
            )

        base_query = f""" --# nosec
        FROM {self._BW_EXTRACTION.changelog_table} AS changelog_tbl
        JOIN {self._BW_EXTRACTION.act_request_table} AS act_req
            ON {join_condition}
        WHERE act_req.odsobject = '{self._BW_EXTRACTION.odsobject}'
            AND act_req.timestamp > {min_timestamp}
            AND act_req.timestamp <= {max_timestamp}
            AND operation = 'A' AND status = '0')
        """

        main_cols = f"""
            (SELECT changelog_tbl.*,
                act_req.TIMESTAMP AS actrequest_timestamp,
                CAST({self._BW_EXTRACTION.extraction_timestamp} AS DECIMAL(15,0))
                    AS extraction_start_timestamp
            """
        # We join the main columns considered for the extraction with
        # extra_cols_act_request that people might want to use, filtering to only
        # add the comma and join the strings, in case extra_cols_act_request is
        # not None or empty.
        extraction_query_cols = ",".join(
            filter(None, [main_cols, self._BW_EXTRACTION.extra_cols_act_request])
        )

        extraction_query = extraction_query_cols + base_query

        predicates_query = f"""
        (SELECT DISTINCT({self._BW_EXTRACTION.partition_column})
        {base_query}
        """

        return extraction_query, predicates_query
