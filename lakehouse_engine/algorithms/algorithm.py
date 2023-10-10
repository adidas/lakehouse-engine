"""Module containing the Algorithm class."""
from typing import List, Tuple

from lakehouse_engine.core.definitions import (
    DQDefaults,
    DQFunctionSpec,
    DQSpec,
    OutputFormat,
)
from lakehouse_engine.core.executable import Executable


class Algorithm(Executable):
    """Class to define the behavior of every algorithm based on ACONs."""

    def __init__(self, acon: dict):
        """Construct Algorithm instances.

        Args:
            acon: algorithm configuration.
        """
        self.acon = acon

    @classmethod
    def get_dq_spec(
        cls, spec: dict
    ) -> Tuple[DQSpec, List[DQFunctionSpec], List[DQFunctionSpec]]:
        """Get data quality specification object from acon.

        Args:
            spec: data quality specifications.

        Returns:
            The DQSpec and the List of DQ Functions Specs.
        """
        dq_spec = DQSpec(
            spec_id=spec["spec_id"],
            input_id=spec["input_id"],
            dq_type=spec["dq_type"],
            dq_functions=[],
            unexpected_rows_pk=spec.get(
                "unexpected_rows_pk", DQSpec.unexpected_rows_pk
            ),
            gx_result_format=spec.get("gx_result_format", DQSpec.gx_result_format),
            tbl_to_derive_pk=spec.get("tbl_to_derive_pk", DQSpec.tbl_to_derive_pk),
            tag_source_data=spec.get("tag_source_data", DQSpec.tag_source_data),
            data_asset_name=spec.get("data_asset_name", DQSpec.data_asset_name),
            expectation_suite_name=spec.get(
                "expectation_suite_name", DQSpec.expectation_suite_name
            ),
            store_backend=spec.get("store_backend", DQDefaults.STORE_BACKEND.value),
            local_fs_root_dir=spec.get("local_fs_root_dir", DQSpec.local_fs_root_dir),
            bucket=spec.get("bucket", DQSpec.bucket),
            data_docs_bucket=spec.get("data_docs_bucket", DQSpec.data_docs_bucket),
            checkpoint_store_prefix=spec.get(
                "checkpoint_store_prefix", DQDefaults.CHECKPOINT_STORE_PREFIX.value
            ),
            expectations_store_prefix=spec.get(
                "expectations_store_prefix",
                DQDefaults.EXPECTATIONS_STORE_PREFIX.value,
            ),
            data_docs_prefix=spec.get(
                "data_docs_prefix", DQDefaults.DATA_DOCS_PREFIX.value
            ),
            validations_store_prefix=spec.get(
                "validations_store_prefix",
                DQDefaults.VALIDATIONS_STORE_PREFIX.value,
            ),
            assistant_options=spec.get("assistant_options", {}),
            result_sink_db_table=spec.get(
                "result_sink_db_table", DQSpec.result_sink_db_table
            ),
            result_sink_location=spec.get(
                "result_sink_location", DQSpec.result_sink_location
            ),
            result_sink_partitions=spec.get(
                "result_sink_partitions", DQSpec.result_sink_partitions
            ),
            result_sink_format=spec.get(
                "result_sink_format", OutputFormat.DELTAFILES.value
            ),
            result_sink_options=spec.get(
                "result_sink_options", DQSpec.result_sink_options
            ),
            result_sink_explode=spec.get(
                "result_sink_explode", DQSpec.result_sink_explode
            ),
            result_sink_extra_columns=spec.get("result_sink_extra_columns", []),
            source=spec.get("source", spec["input_id"]),
            fail_on_error=spec.get("fail_on_error", DQSpec.fail_on_error),
            cache_df=spec.get("cache_df", DQSpec.cache_df),
            critical_functions=spec.get(
                "critical_functions", DQSpec.critical_functions
            ),
            max_percentage_failure=spec.get(
                "max_percentage_failure", DQSpec.max_percentage_failure
            ),
        )

        dq_functions = cls._get_dq_functions(spec, "dq_functions")

        critical_functions = cls._get_dq_functions(spec, "critical_functions")

        cls._validate_dq_tag_strategy(dq_spec)

        return dq_spec, dq_functions, critical_functions

    @staticmethod
    def _get_dq_functions(spec: dict, function_key: str) -> List[DQFunctionSpec]:
        """Get DQ Functions from a DQ Spec, based on a function_key.

        Args:
            spec: data quality specifications.
            function_key: dq function key ("dq_functions" or
            "critical_functions").

        Returns:
            a list of DQ Function Specs.
        """
        functions = []

        if spec.get(function_key, []):
            for f in spec.get(function_key, []):
                dq_fn_spec = DQFunctionSpec(
                    function=f["function"],
                    args=f.get("args", {}),
                )
                functions.append(dq_fn_spec)

        return functions

    @staticmethod
    def _validate_dq_tag_strategy(spec: DQSpec) -> None:
        """Validate DQ Spec arguments related with the data tagging strategy.

        Args:
            spec: data quality specifications.
        """
        if spec.tag_source_data:
            spec.gx_result_format = DQSpec.gx_result_format
            spec.fail_on_error = False
            spec.result_sink_explode = DQSpec.result_sink_explode
        elif spec.gx_result_format != DQSpec.gx_result_format:
            spec.tag_source_data = False
