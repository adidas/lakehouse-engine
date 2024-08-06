"""Module to perform validations and resolve the acon."""

from lakehouse_engine.core.definitions import DQType, InputFormat, OutputFormat
from lakehouse_engine.io.exceptions import WrongIOFormatException
from lakehouse_engine.utils.dq_utils import PrismaUtils
from lakehouse_engine.utils.logging_handler import LoggingHandler

_LOGGER = LoggingHandler(__name__).get_logger()


def validate_and_resolve_acon(acon: dict, execution_point: str = "") -> dict:
    """Function to validate and resolve the acon.

    Args:
        acon: Acon to be validated and resolved.
        execution_point: Execution point to resolve the dq functions.

    Returns:
        Acon after validation and resolution.
    """
    # Performing validations
    validate_readers(acon)
    validate_writers(acon)

    # Resolving the acon
    if execution_point:
        acon = resolve_dq_functions(acon, execution_point)

    _LOGGER.info(f"Read Algorithm Configuration: {str(acon)}")

    return acon


def validate_readers(acon: dict) -> None:
    """Function to validate the readers in the acon.

    Args:
        acon: Acon to be validated.

    Raises:
        RuntimeError: If the input format is not supported.
    """
    if "input_specs" in acon.keys() or "input_spec" in acon.keys():
        for spec in acon.get("input_specs", []) or [acon.get("input_spec", {})]:
            if (
                not InputFormat.exists(spec.get("data_format"))
                and "db_table" not in spec.keys()
            ):
                raise WrongIOFormatException(
                    f"Input format not supported: {spec.get('data_format')}"
                )


def validate_writers(acon: dict) -> None:
    """Function to validate the writers in the acon.

    Args:
        acon: Acon to be validated.

    Raises:
        RuntimeError: If the output format is not supported.
    """
    if "output_specs" in acon.keys() or "output_spec" in acon.keys():
        for spec in acon.get("output_specs", []) or [acon.get("output_spec", {})]:
            if not OutputFormat.exists(spec.get("data_format")):
                raise WrongIOFormatException(
                    f"Output format not supported: {spec.get('data_format')}"
                )


def resolve_dq_functions(acon: dict, execution_point: str) -> dict:
    """Function to resolve the dq functions in the acon.

    Args:
        acon: Acon to resolve the dq functions.
        execution_point: Execution point of the dq_functions.

    Returns:
        Acon after resolving the dq functions.
    """
    if acon.get("dq_spec"):
        if acon.get("dq_spec").get("dq_type") == DQType.PRISMA.value:
            acon["dq_spec"] = PrismaUtils.build_prisma_dq_spec(
                spec=acon.get("dq_spec"), execution_point=execution_point
            )
    elif acon.get("dq_specs"):
        resolved_dq_specs = []
        for spec in acon.get("dq_specs", []):
            if spec.get("dq_type") == DQType.PRISMA.value:
                resolved_dq_specs.append(
                    PrismaUtils.build_prisma_dq_spec(
                        spec=spec, execution_point=execution_point
                    )
                )
            else:
                resolved_dq_specs.append(spec)
        acon["dq_specs"] = resolved_dq_specs
    return acon
