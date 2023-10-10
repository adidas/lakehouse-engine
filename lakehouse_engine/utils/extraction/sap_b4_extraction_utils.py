"""Utilities module for SAP B4 extraction processes."""
import re
from dataclasses import dataclass
from enum import Enum
from logging import Logger
from typing import Any, Optional, Tuple

from lakehouse_engine.core.definitions import InputSpec, ReadType
from lakehouse_engine.transformers.aggregators import Aggregators
from lakehouse_engine.utils.extraction.jdbc_extraction_utils import (
    JDBCExtraction,
    JDBCExtractionUtils,
)
from lakehouse_engine.utils.logging_handler import LoggingHandler


class ADSOTypes(Enum):
    """Standardise the types of ADSOs we can have for Extractions from SAP B4."""

    AQ: str = "AQ"
    CL: str = "CL"
    SUPPORTED_TYPES: list = [AQ, CL]


@dataclass
class SAPB4Extraction(JDBCExtraction):
    """Configurations available for an Extraction from SAP B4.

    It inherits from JDBCExtraction configurations, so it can use
    and/or overwrite those configurations.

    These configurations cover:
        latest_timestamp_input_col: the column containing the request timestamps
            in the dataset in latest_timestamp_data_location. Default: REQTSN.
        request_status_tbl: the name of the SAP B4 table having information
            about the extraction requests. Composed of database.table.
            Default: SAPHANADB.RSPMREQUEST.
        request_col_name: name of the column having the request timestamp to join
            with the request status table. Default: REQUEST_TSN.
        data_target: the data target to extract from. User in the join operation with
            the request status table.
        act_req_join_condition: the join condition into activation table
            can be changed using this property.
            Default: 'tbl.reqtsn = req.request_col_name'.
        include_changelog_tech_cols: whether to include the technical columns
            (usually coming from the changelog) table or not.
        extra_cols_req_status_tbl: columns to be added from request status table.
            It needs to contain the prefix "req.". E.g. "req.col1 as column_one,
            req.col2 as column_two".
        request_status_tbl_filter: filter to use for filtering the request status table,
            influencing the calculation of the max timestamps and the delta extractions.
        adso_type: the type of ADSO that you are extracting from. Can be "AQ" or "CL".
        max_timestamp_custom_schema: the custom schema to apply on the calculation of
            the max timestamp to consider for the delta extractions.
            Default: timestamp DECIMAL(23,0).
        default_max_timestamp: the timestamp to use as default, when it is not possible
            to derive one.
        custom_schema: specify custom_schema for particular columns of the
            returned dataframe in the init/delta extraction of the source table.
    """

    latest_timestamp_input_col: str = "REQTSN"
    request_status_tbl: str = "SAPHANADB.RSPMREQUEST"
    request_col_name: str = "REQUEST_TSN"
    data_target: Optional[str] = None
    act_req_join_condition: Optional[str] = None
    include_changelog_tech_cols: Optional[bool] = None
    extra_cols_req_status_tbl: Optional[str] = None
    request_status_tbl_filter: Optional[str] = None
    adso_type: Optional[str] = None
    max_timestamp_custom_schema: str = "timestamp DECIMAL(23,0)"
    default_max_timestamp: str = "1970000000000000000000"
    custom_schema: str = "REQTSN DECIMAL(23,0)"


class SAPB4ExtractionUtils(JDBCExtractionUtils):
    """Utils for managing data extraction from SAP B4."""

    def __init__(self, sap_b4_extraction: SAPB4Extraction):
        """Construct SAPB4ExtractionUtils.

        Args:
            sap_b4_extraction: SAP B4 Extraction configurations.
        """
        self._LOGGER: Logger = LoggingHandler(__name__).get_logger()
        self._B4_EXTRACTION = sap_b4_extraction
        self._B4_EXTRACTION.request_status_tbl_filter = (
            self._get_req_status_tbl_filter()
        )
        self._MAX_TIMESTAMP_QUERY = f""" --# nosec
                (SELECT COALESCE(MAX({self._B4_EXTRACTION.request_col_name}),
                    {self._B4_EXTRACTION.default_max_timestamp}) as timestamp
                FROM {self._B4_EXTRACTION.request_status_tbl}
                WHERE {self._B4_EXTRACTION.request_status_tbl_filter})
            """  # nosec: B608
        super().__init__(sap_b4_extraction)

    @staticmethod
    def get_data_target(input_spec_opt: dict) -> str:
        """Get the data_target from the data_target option or derive it.

        By definition data_target is the same for the table and changelog table and
        is the same string ignoring everything before / and the first and last
        character after /. E.g. for a dbtable /BIC/abtable12, the data_target
        would be btable1.

        Args:
            input_spec_opt: options from the input_spec.

        Returns:
            A string with the data_target.
        """
        exclude_chars = """["'\\\\]"""

        return input_spec_opt.get(
            "data_target",
            re.sub(exclude_chars, "", input_spec_opt["dbtable"]).split("/")[-1][1:-1],
        )

    def _get_init_query(self) -> Tuple[str, str]:
        """Get a query to do an init load based on a ADSO on a SAP B4 system.

        Returns:
            A query to submit to SAP B4 for the initial data extraction. The query
            is enclosed in parenthesis so that Spark treats it as a table and supports
            it in the dbtable option.
        """
        extraction_query = self._get_init_extraction_query()

        predicates_query = f"""
        (SELECT DISTINCT({self._B4_EXTRACTION.partition_column})
        FROM {self._B4_EXTRACTION.dbtable} t)
        """  # nosec: B608

        return extraction_query, predicates_query

    def _get_init_extraction_query(self) -> str:
        """Get the init extraction query based on current timestamp.

        Returns:
            A query to submit to SAP B4 for the initial data extraction.
        """
        changelog_tech_cols = (
            f"""{self._B4_EXTRACTION.extraction_timestamp}000000000 AS reqtsn,
                '0' AS datapakid,
                0 AS record,"""
            if self._B4_EXTRACTION.include_changelog_tech_cols
            else ""
        )

        extraction_query = f"""
                (SELECT t.*, {changelog_tech_cols}
                    CAST({self._B4_EXTRACTION.extraction_timestamp}
                        AS DECIMAL(15,0)) AS extraction_start_timestamp
                FROM {self._B4_EXTRACTION.dbtable} t
                )"""  # nosec: B608

        return extraction_query

    def _get_delta_query(self) -> Tuple[str, str]:
        """Get a delta query for an SAP B4 ADSO.

        An SAP B4 ADSO requires a join with a special type of table often called
        requests status table (RSPMREQUEST), in which B4 tracks down the timestamps,
        status and metrics associated with the several data loads that were performed
        into B4. Depending on the type of ADSO (AQ or CL) the join condition and also
        the ADSO/table to consider to extract from will be different.
        For AQ types, there is only the active table, from which we extract both inits
        and deltas and this is also the table used to join with RSPMREQUEST to derive
        the next portion of the data to extract.
        For the CL types, we have an active table/adso from which we extract the init
        and one changelog table from which we extract the delta portions of data.
        Depending, if it is an init or delta one table or the other is also used to join
        with RSPMREQUEST.

        The logic on this function basically ensures that we are reading from the source
        table considering the data that has arrived between the maximum timestamp that
        is available in our target destination and the max timestamp of the extractions
        performed and registered in the RSPMREQUEST table, which follow the filtering
         criteria.

        Returns:
            A query to submit to SAP B4 for the delta data extraction. The query
            is enclosed in parenthesis so that Spark treats it as a table and supports
            it in the dbtable option.
        """
        if not self._B4_EXTRACTION.min_timestamp:
            from lakehouse_engine.io.reader_factory import ReaderFactory

            latest_timestamp_data_df = ReaderFactory.get_data(
                InputSpec(
                    spec_id="data_with_latest_timestamp",
                    data_format=self._B4_EXTRACTION.latest_timestamp_data_format,
                    read_type=ReadType.BATCH.value,
                    location=self._B4_EXTRACTION.latest_timestamp_data_location,
                )
            )
            min_timestamp = latest_timestamp_data_df.transform(
                Aggregators.get_max_value(
                    self._B4_EXTRACTION.latest_timestamp_input_col
                )
            ).first()[0]
        else:
            min_timestamp = self._B4_EXTRACTION.min_timestamp

        max_timestamp = (
            self._B4_EXTRACTION.max_timestamp
            if self._B4_EXTRACTION.max_timestamp
            else self._get_max_timestamp(self._MAX_TIMESTAMP_QUERY)
        )

        if self._B4_EXTRACTION.act_req_join_condition:
            join_condition = f"{self._B4_EXTRACTION.act_req_join_condition}"
        else:
            join_condition = f"tbl.reqtsn = req.{self._B4_EXTRACTION.request_col_name}"

        base_query = f""" --# nosec
        FROM {self._B4_EXTRACTION.changelog_table} AS tbl
        JOIN {self._B4_EXTRACTION.request_status_tbl} AS req
            ON {join_condition}
        WHERE {self._B4_EXTRACTION.request_status_tbl_filter}
            AND req.{self._B4_EXTRACTION.request_col_name} > {min_timestamp}
            AND req.{self._B4_EXTRACTION.request_col_name} <= {max_timestamp})
        """

        main_cols = f"""
            (SELECT tbl.*,
                CAST({self._B4_EXTRACTION.extraction_timestamp} AS DECIMAL(15,0))
                    AS extraction_start_timestamp
            """

        # We join the main columns considered for the extraction with
        # extra_cols_act_request that people might want to use, filtering to only
        # add the comma and join the strings, in case extra_cols_act_request is
        # not None or empty.
        extraction_query_cols = ",".join(
            filter(None, [main_cols, self._B4_EXTRACTION.extra_cols_req_status_tbl])
        )

        extraction_query = extraction_query_cols + base_query

        predicates_query = f"""
        (SELECT DISTINCT({self._B4_EXTRACTION.partition_column})
        {base_query}
        """

        return extraction_query, predicates_query

    def _get_req_status_tbl_filter(self) -> Any:
        if self._B4_EXTRACTION.request_status_tbl_filter:
            return self._B4_EXTRACTION.request_status_tbl_filter
        else:
            if self._B4_EXTRACTION.adso_type == ADSOTypes.AQ.value:
                return f"""
                    STORAGE = 'AQ' AND REQUEST_IS_IN_PROCESS = 'N' AND
                    LAST_OPERATION_TYPE IN ('C', 'U') AND REQUEST_STATUS IN ('GG', 'GR')
                    AND UPPER(DATATARGET) = UPPER('{self._B4_EXTRACTION.data_target}')
                """
            elif self._B4_EXTRACTION.adso_type == ADSOTypes.CL.value:
                return f"""
                    STORAGE = 'AT' AND REQUEST_IS_IN_PROCESS = 'N' AND
                    LAST_OPERATION_TYPE IN ('C', 'U') AND REQUEST_STATUS IN ('GG')
                    AND UPPER(DATATARGET) = UPPER('{self._B4_EXTRACTION.data_target}')
                """
            else:
                raise NotImplementedError(
                    f"The requested ADSO Type is not fully implemented and/or tested."
                    f"Supported ADSO Types: {ADSOTypes.SUPPORTED_TYPES}"
                )
