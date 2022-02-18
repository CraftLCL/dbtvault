import pytest

from test import dbtvault_harness_utils

macro_name = "process_sat_payload_columns"


@pytest.mark.macro
def test_process_sat_payload_columns_excluded_columns_as_list_is_successful(request, generate_model):
    var_dict = {'payload_columns': {'exclude_columns': True, 'columns': ['BOOKING_FK']}, 'source_model': 'raw_source'}

    generate_model()

    dbt_logs = dbtvault_harness_utils.run_dbt_models(model_names=[request.node.name],
                                                     args=var_dict)
    actual_sql = dbtvault_harness_utils.retrieve_compiled_model(request.node.name)
    expected_sql = dbtvault_harness_utils.retrieve_expected_sql(request)

    assert dbtvault_harness_utils.is_successful_run(dbt_logs)
    assert actual_sql == expected_sql
