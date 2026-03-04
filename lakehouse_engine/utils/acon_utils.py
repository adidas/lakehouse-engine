"""Module to perform validations and resolve the acon."""

from lakehouse_engine.core.definitions import (
    FILE_MANAGER_OPERATIONS,
    TABLE_MANAGER_OPERATIONS,
    DQType,
    InputFormat,
    OutputFormat,
)
from lakehouse_engine.io.exceptions import WrongIOFormatException
from lakehouse_engine.utils.dq_utils import PrismaUtils
from lakehouse_engine.utils.logging_handler import LoggingHandler

_LOGGER = LoggingHandler(__name__).get_logger()


def validate_manager_list(acon: dict) -> list:
    """Function to validate an acon with a list of operations.

    Args:
        acon: Acon to be validated.
    """
    error_list: list[str] = []
    operations: list[dict] = acon.get("operations", [])

    if not operations:
        raise RuntimeError("No operations found in the acon.")

    for operation in operations:
        validate_managers(operation, error_list)
    if error_list:
        error_list_str = "\n" + "\n".join(error_list)
        raise RuntimeError(f"Errors found during validation:{error_list_str}")

    return operations


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
    validate_managers(acon)

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


def validate_managers(acon: dict, error_list: list = None) -> None:
    """Function to validate the managers in the acon.

    Args:
        acon: Acon to be validated.
        error_list: List to collect errors.
    """
    manager_type = acon.get("manager")
    temp_error_list = []
    if not manager_type:
        return

    function_name = acon.get("function")
    if not function_name:
        error = "Missing 'function' parameter for manager"
        temp_error_list.append(error)

    if manager_type == "file":
        operations_dict = FILE_MANAGER_OPERATIONS
    elif manager_type == "table":
        operations_dict = TABLE_MANAGER_OPERATIONS
    else:
        error = f"Manager type not supported: {manager_type}"
        temp_error_list.append(error)

    if function_name not in operations_dict:
        error = f"Function '{function_name}' not supported for {manager_type} manager"
        temp_error_list.append(error)
    else:
        expected_params = operations_dict[function_name]

        missing_mandatory = validate_mandatory_parameters(acon, expected_params)
        if missing_mandatory:
            error = (
                f"Missing mandatory parameters for {manager_type} "
                f"manager function {function_name}: {missing_mandatory}"
            )
            temp_error_list.append(error)

        type_errors = validate_parameter_types(acon, expected_params)

        if type_errors:
            error = (
                f"Type validation errors for {manager_type} "
                f"manager function {function_name}: {type_errors}"
            )
            temp_error_list.append(error)

    if error_list is not None:
        error_list.extend(temp_error_list)
    else:
        if temp_error_list:
            error_list_str = "\n".join(temp_error_list)
            raise RuntimeError(error_list_str)


def validate_mandatory_parameters(acon: dict, expected_params: dict) -> list:
    """Function to validate mandatory parameters in the acon.

    Args:
        acon: Acon to be validated.
        expected_params: Expected parameters with their mandatory status.

    Returns:
        List of missing mandatory parameters.
    """
    missing_mandatory = []
    for param_name, param_info in expected_params.items():
        if param_info["mandatory"] and param_name not in acon:
            missing_mandatory.append(param_name)

    return missing_mandatory


def validate_parameter_types(acon: dict, expected_params: dict) -> list:
    """Function to validate parameter types in the acon.

    Args:
        acon: Acon to be validated.
        expected_params: Expected parameters with their types.

    Returns:
        List of type validation errors.
    """
    type_errors = []
    for param_name, param_value in acon.items():
        if param_name in expected_params:
            expected_type = expected_params[param_name]["type"]
            param_type_name = type(param_value).__name__

            expected_python_type = {
                "str": str,
                "bool": bool,
                "int": int,
                "list": list,
            }.get(expected_type)

            if expected_python_type and not isinstance(
                param_value, expected_python_type
            ):
                type_errors.append(
                    f"Parameter '{param_name}' expected {expected_type}, "
                    f"got {param_type_name}"
                )

    return type_errors


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
