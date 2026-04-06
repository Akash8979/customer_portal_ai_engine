"""End-to-End DPAI MlOps pipeline with sectionwise execution from data import to output upload"""

# import packages
import time
import json
import logging
import traceback
import httpx
from azureml.core import Experiment
from azureml.pipeline.core import PipelineData
from azureml.pipeline.steps import PythonScriptStep
from azureml.pipeline.core import Pipeline
from credentials import *
from metadata import get_blob_meta_data
from utils import (
    get_aml_cluster,
    get_env,
    get_run_config,
    get_container_blob_details,
    get_machine,
    filter_and_sort_events,
    count_dfu_in_mapping,
    patch_with_retries,
)
from workspace import ws
from azureml.core import Workspace
from azureml.pipeline.steps import ParallelRunConfig
from azureml.pipeline.steps import ParallelRunStep
from azureml.pipeline.core import PipelineParameter
from pprint import pprint
from pathlib import Path
from functools import reduce
import os
from auth_token import get_login_response

########################################################################################
logging.basicConfig(level=logging.DEBUG)


########################################################################################
def has_external_features(config: dict) -> bool:
    """
    Check if external features are enabled for forecast.
    Returns True if useFeaturesInForecast exists and has at least one feature.
    """
    forecast_config = (
        config.get("config", {}).get("configuration", {}).get("forecast", {})
    )
    features = forecast_config.get("useFeaturesInForecast", [])
    return isinstance(features, list) and len(features) >= 1


########################################################################################
def forecast_run(config: dict, version: str):
    """
    Function to run each operation of pipeline one by one for a single "key" at a time
    ---
    config : configuration dict for azureML cluster access\n
    version :   ?
    """
    try:
        cluster_name = f"snop-{config['jobid'][-6:]}-cluster"
        container_blob_details = get_container_blob_details(
            url=config["config"]["data"]["historicalSales"]
        )
        size_details = get_blob_meta_data(
            container_name=container_blob_details["container_name"],
            blob_name=container_blob_details["blob_name"],
            connection_string=***_connection_string,
        )
        # select appropriate machine based on data size
        vm_size = get_machine(size_details=size_details)

        # Create or get training cluster
        aml_cluster = get_aml_cluster(ws, vm_size=vm_size, cluster_name=cluster_name)
        aml_cluster.wait_for_completion(show_output=True)

        # get dfu count in mapping master
        dfu_count = count_dfu_in_mapping(
            config["config"]["data"]["mapping"], ***_storage_key
        )
        logging.debug("pat key : {}".format(azure_devops_pat))
        logging.debug(
            "transformation module : {}".format(
                "git+https://***-***:{azure_devops_pat}@dev.azure.com/***-***/***/_git/transform-lib@v0.1.8"
            )
        )
        login_payload = {
            "email": email,
            "password": password,
            "recaptcha_token": recaptcha_token,
        }
        # Create a run configuration
        run_conf = get_run_config(
            packages=[
                f"git+https://***-***:{azure_devops_pat}@dev.azure.com/***-***/***/_git/transform-lib@v0.1.8",
                f"git+https://***-***:{azure_devops_pat}@dev.azure.com/***-***/***/_git/csa-lib@v0.1.2",
                "azure-storage-blob",
                "numpy",
                "pandas",
                "scikit-learn",
                "scipy",
                "sktime",
                "httpx",
                "pmdarima",
                "xgboost",
                "pymannkendall",
                "statsmodels",
                "statistics",
                "prophet",
                "catboost",
                "tbats",
                "ray",
                "greykite",
                "holidays==0.9.12",
                "statsforecast==1.7.5",
                "loguru",
                "dask[complete]",
                "aiohttp",
                "requests",
                "tenacity",
                "EMD-signal",
            ],
            config=config,
        )
        product = PipelineData("product")
        location = PipelineData("location")
        channel = PipelineData("channel")
        mapping = PipelineData("mapping")
        sales = PipelineData("historicalSales")
        calendar = PipelineData("calendar")
        ui_config = PipelineData("UI_config")
        date_patterns = PipelineData("date_patterns")
        model_config = PipelineData("model_config")
        pipeline_setting = PipelineData("pipeline_setting")
        profile_config = PipelineData("profile_config")
        profile_outlier_settings = PipelineData("profile_outlier_settings")
        transform_df = PipelineData("transform_df")
        unit_price_df = PipelineData("unit_price_df")
        mapping_id_name_df = PipelineData("mapping_id_name_df")
        csa_transformed_df = PipelineData("csa_transformed_df")
        config_ds = PipelineData("config_ds")

        transform_df_first_batch_output = PipelineData(
            "transform_df_first_batch_output"
        )
        transform_df_second_batch_output = PipelineData(
            "transform_df_second_batch_output"
        )
        transform_df_third_batch_output = PipelineData(
            "transform_df_third_batch_output"
        )
        # gap_month_forecast_first_batch_output=PipelineData("gap_month_forecast_first_batch_output")
        # gap_month_forecast_second_batch_output=PipelineData("gap_month_forecast_second_batch_output")
        # gap_month_forecast_third_batch_output=PipelineData("gap_month_forecast_third_batch_output")
        # gap_month_baseline_forecast_first_batch_output=PipelineData("gap_month_baseline_forecast_first_batch_output")
        # gap_month_baseline_forecast_second_batch_output=PipelineData("gap_month_baseline_forecast_second_batch_output")
        # gap_month_baseline_forecast_third_batch_output=PipelineData("gap_month_baseline_forecast_third_batch_output")
        outlier_profile_first_batch = PipelineData("outlier_profile_first_batch")
        outlier_profile_second_batch = PipelineData("outlier_profile_second_batch")
        outlier_profile_third_batch = PipelineData("outlier_profile_third_batch")
        forecast_profiling_first_batch_output = PipelineData(
            "forecast_profiling_first_batch_output"
        )
        forecast_profiling_second_batch_output = PipelineData(
            "forecast_profiling_second_batch_output"
        )
        forecast_profiling_third_batch_output = PipelineData(
            "forecast_profiling_third_batch_output"
        )
        forecast_first_batch_output = PipelineData("forecast_first_batch_output")
        forecast_second_batch_output = PipelineData("forecast_second_batch_output")
        forecast_third_batch_output = PipelineData("forecast_third_batch_output")
        outlier_first_batch_output = PipelineData("outlier_first_batch_output")
        outlier_second_batch_output = PipelineData("outlier_second_batch_output")
        outlier_third_batch_output = PipelineData("outlier_third_batch_output")
        profiling_first_batch_output = PipelineData("profiling_first_batch_output")
        profiling_second_batch_output = PipelineData("profiling_second_batch_output")
        profiling_third_batch_output = PipelineData("profiling_third_batch_output")
        forecast_output = PipelineData(f"forecast_output_{config['jobid'][-6:]}")
        outlier_output = PipelineData(f"outlier_output_{config['jobid'][-6:]}")
        profiling_output = PipelineData(f"profiling_output_{config['jobid'][-6:]}")
        baseline_output = PipelineData("baseline_output{}".format(config["jobid"][-6:]))
        residual_first_batch_output = PipelineData("residual_first_batch_output")
        residual_second_batch_output = PipelineData("residual_second_batch_output")
        residual_third_batch_output = PipelineData("residual_third_batch_output")
        baseline_forecast_first_batch_output = PipelineData(
            "baseline_forecast_first_batch_output"
        )
        baseline_forecast_second_batch_output = PipelineData(
            "baseline_forecast_second_batch_output"
        )
        baseline_forecast_third_batch_output = PipelineData(
            "baseline_forecast_third_batch_output"
        )
        residual_output = PipelineData("residual_output{}".format(config["jobid"][-6:]))
        gap_month_forecast_output = PipelineData("gap_month_forecast_output")
        gap_month_baseline_forecast_output = PipelineData(
            "gap_month_baseline_forecast_output"
        )
        # Build paths inside the project like this: BASE_DIR / 'subdir'.
        BASE_DIR = Path(__file__).resolve().parent.parent
        required_path = os.path.join(BASE_DIR, "dpai-mlops")
        if has_external_features(config):
            print("inside multivariate")
            external_raw = PipelineData("external_raw")
            external_transformed = PipelineData("external_transformed")
            if dfu_count < 100:
                # component read from blob
                storage_blob = PythonScriptStep(
                    name="read_from_blob_storage",
                    script_name="read_from_blob.py",
                    source_directory=str(
                        os.path.join(
                            required_path, "multivariate_src/read_from_storage"
                        )
                    ),
                    arguments=[
                        "--product_url",
                        config["config"]["data"]["product"],
                        "--location_url",
                        config["config"]["data"]["location"],
                        "--channel_url",
                        config["config"]["data"]["channel"],
                        "--mapping_url",
                        config["config"]["data"]["mapping"],
                        "--historicalSales_url",
                        config["config"]["data"]["historicalSales"],
                        "--external_df_url",
                        config["config"]["data"]["externalfactors"],
                        "--calendar_url",
                        config["config"]["data"]["calendar"],
                        "--storage_accont_key",
                        ***_storage_key,
                        "--product",
                        product,
                        "--location",
                        location,
                        "--channel",
                        channel,
                        "--mapping",
                        mapping,
                        "--calendar",
                        calendar,
                        "--historicalSales",
                        sales,
                        "--external",
                        external_raw,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--***_connection_string",
                        ***_connection_string,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    outputs=[
                        product,
                        location,
                        channel,
                        mapping,
                        sales,
                        external_raw,
                        calendar,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # component config
                configuaration = PythonScriptStep(
                    name="configuaration",
                    script_name="read_config.py",
                    source_directory=str(
                        os.path.join(required_path, "multivariate_src/configuration")
                    ),
                    arguments=[
                        "--version",
                        version,
                        "--UI_config",
                        ui_config,
                        "--date_patterns",
                        date_patterns,
                        "--model_config",
                        model_config,
                        "--pipeline_setting",
                        pipeline_setting,
                        "--profile_config",
                        profile_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--storage_accont_key",
                        dpai_storage_key,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    outputs=[
                        ui_config,
                        date_patterns,
                        model_config,
                        pipeline_setting,
                        profile_config,
                        profile_outlier_settings,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # component transformation sales
                transform = PythonScriptStep(
                    name="transform",
                    script_name="transform.py",
                    source_directory=str(
                        os.path.join(required_path, "multivariate_src/tramsformation")
                    ),
                    arguments=[
                        "--product",
                        product,
                        "--location",
                        location,
                        "--channel",
                        channel,
                        "--mapping",
                        mapping,
                        "--sales",
                        sales,
                        "--calendar",
                        calendar,
                        "--date_patterns",
                        date_patterns,
                        "--transform_df",
                        transform_df,
                        "--unit_price_df",
                        unit_price_df,
                        "--mapping_id_name_df",
                        mapping_id_name_df,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--csa_transformed_df",
                        csa_transformed_df,
                        "--***_connection_string",
                        ***_connection_string,
                        "--config_ds",
                        config_ds,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        product,
                        location,
                        channel,
                        mapping,
                        sales,
                        date_patterns,
                        calendar,
                    ],
                    outputs=[
                        transform_df,
                        unit_price_df,
                        mapping_id_name_df,
                        csa_transformed_df,
                        config_ds,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )

                # external factor transformation
                transform_external = PythonScriptStep(
                    name="transform_external",
                    script_name="transform.py",
                    source_directory=str(
                        os.path.join(
                            required_path, "multivariate_src/external_transformation"
                        )
                    ),
                    arguments=[
                        "--date_patterns",
                        date_patterns,
                        "--external",
                        external_raw,
                        "--transform_df",
                        transform_df,
                        "--unit_price_df",
                        unit_price_df,
                        "--mapping_id_name_df",
                        mapping_id_name_df,
                        "--external_transformed",
                        external_transformed,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--sales",
                        sales,
                        "--calendar",
                        calendar,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        date_patterns,
                        external_raw,
                        transform_df,
                        unit_price_df,
                        mapping_id_name_df,
                        sales,
                        calendar,
                    ],
                    outputs=[external_transformed],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # create outlier profiling batch
                transform_batch = PythonScriptStep(
                    name="transform_batch",
                    script_name="batch.py",
                    source_directory=str(
                        os.path.join(
                            required_path, "multivariate_src/batch_dataset_transformed"
                        )
                    ),
                    arguments=[
                        "--transform_df",
                        transform_df,
                        "--transform_df_first_batch",
                        transform_df_first_batch_output,
                        "--transform_df_second_batch",
                        transform_df_second_batch_output,
                        "--transform_df_third_batch",
                        transform_df_third_batch_output,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[transform_df],
                    outputs=[
                        transform_df_first_batch_output,
                        transform_df_second_batch_output,
                        transform_df_third_batch_output,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # outlier profiling on first batch
                outlier_profiling_first_batch = PythonScriptStep(
                    name="outlier_profiling_first_batch",
                    script_name="outlier_profile.py",
                    source_directory=str(
                        os.path.join(
                            required_path, "multivariate_src/outlier_profiling"
                        )
                    ),
                    arguments=[
                        "--transform_df_batch",
                        transform_df_first_batch_output,
                        "--ui_config",
                        ui_config,
                        "--profile_config",
                        profile_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--outlier_profile_batch",
                        outlier_profile_first_batch,
                        "--batch",
                        "first",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        transform_df_first_batch_output,
                        ui_config,
                        profile_config,
                        profile_outlier_settings,
                    ],
                    outputs=[outlier_profile_first_batch],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # forecast profiling first batch
                forecast_profiling_first_batch = PythonScriptStep(
                    name="forecast_profiling_first_batch",
                    script_name="forecast_profile.py",
                    source_directory=str(
                        os.path.join(
                            required_path, "multivariate_src/forecast_profiling"
                        )
                    ),
                    arguments=[
                        "--batch",
                        outlier_profile_first_batch,
                        "--ui_config",
                        ui_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--forecast_profiling_output",
                        forecast_profiling_first_batch_output,
                        "--transformed_df",
                        transform_df,
                        "--batch_position",
                        "first",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        outlier_profile_first_batch,
                        ui_config,
                        profile_outlier_settings,
                        transform_df,
                    ],
                    outputs=[forecast_profiling_first_batch_output],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # forecast for first batch
                forecast_first_batch = PythonScriptStep(
                    name="forecast_first_batch",
                    script_name="forecast.py",
                    source_directory=str(
                        os.path.join(
                            required_path, "multivariate_src/forecast_modeling"
                        )
                    ),
                    arguments=[
                        "--batch",
                        forecast_profiling_first_batch_output,
                        "--ui_config",
                        ui_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--date_patterns",
                        date_patterns,
                        "--profile_config",
                        profile_config,
                        "--model_config",
                        model_config,
                        "--pipeline_setting",
                        pipeline_setting,
                        "--forecast_output",
                        forecast_first_batch_output,
                        "--outlier_output",
                        outlier_first_batch_output,
                        "--profiling_output",
                        profiling_first_batch_output,
                        "--residual_output",
                        residual_first_batch_output,
                        "--baseline_output",
                        baseline_forecast_first_batch_output,
                        "--transformed_df",
                        transform_df,
                        "--external_df",
                        external_transformed,
                        "--batch_position",
                        "first",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--***_container_string",
                        ***_connection_string,
                        "--config_ds",
                        config_ds,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        forecast_profiling_first_batch_output,
                        ui_config,
                        profile_outlier_settings,
                        date_patterns,
                        profile_config,
                        model_config,
                        pipeline_setting,
                        transform_df,
                        external_transformed,
                        config_ds,
                    ],
                    outputs=[
                        forecast_first_batch_output,
                        outlier_first_batch_output,
                        profiling_first_batch_output,
                        baseline_forecast_first_batch_output,
                        residual_first_batch_output,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                collect_forecast_result = PythonScriptStep(
                    name="collect_forecast_result",
                    script_name="mini_batch_result.py",
                    source_directory=str(
                        os.path.join(required_path, "multivariate_src/collect_result")
                    ),
                    arguments=[
                        "--forecast_first_batch_output",
                        forecast_first_batch_output,
                        "--outlier_first_batch_output",
                        outlier_first_batch_output,
                        "--profiling_first_batch_output",
                        profiling_first_batch_output,
                        "--baseline_forecast_first_batch_output",
                        baseline_forecast_first_batch_output,
                        "--residual_first_batch_output",
                        residual_first_batch_output,
                        "--forecast_output",
                        forecast_output,
                        "--outlier_output",
                        outlier_output,
                        "--profiling_output",
                        profiling_output,
                        "--baseline_output",
                        baseline_output,
                        "--residual_output",
                        residual_output,
                        "--unit_price",
                        unit_price_df,
                        "--mapping_id",
                        mapping_id_name_df,
                        "--transformed_df",
                        transform_df,
                        "--calendar",
                        calendar,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--gap_month_forecast_output",
                        gap_month_forecast_output,
                        "--config_ds",
                        config_ds,
                        "--gap_month_baseline_forecast_output",
                        gap_month_baseline_forecast_output,
                        "--sales",
                        sales,
                        "--product",
                        product,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        forecast_first_batch_output,
                        outlier_first_batch_output,
                        profiling_first_batch_output,
                        baseline_forecast_first_batch_output,
                        residual_first_batch_output,
                        unit_price_df,
                        mapping_id_name_df,
                        transform_df,
                        calendar,
                        config_ds,
                        sales,
                        product,
                    ],
                    outputs=[
                        forecast_output,
                        outlier_output,
                        profiling_output,
                        baseline_output,
                        residual_output,
                        gap_month_forecast_output,
                        gap_month_baseline_forecast_output,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # upload to storage
                upload_files_to_storage = PythonScriptStep(
                    name="upload_files_to_storage",
                    script_name="upload.py",
                    source_directory=str(
                        os.path.join(
                            required_path, "multivariate_src/upload_to_storage"
                        )
                    ),
                    arguments=[
                        "--forecast",
                        forecast_output,
                        "--outlier",
                        outlier_output,
                        "--profiling",
                        profiling_output,
                        "--transform",
                        transform_df,
                        "--baseline_output",
                        baseline_output,
                        "--residual_output",
                        residual_output,
                        "--external_transformed_output",
                        external_transformed,
                        "--storage_accont_key",
                        ***_connection_string,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--csa_transformed_df",
                        csa_transformed_df,
                        "--gap_month_forecast_output",
                        gap_month_forecast_output,
                        "--gap_month_baseline_forecast_output",
                        gap_month_baseline_forecast_output,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        forecast_output,
                        outlier_output,
                        profiling_output,
                        transform_df,
                        baseline_output,
                        residual_output,
                        external_transformed,
                        csa_transformed_df,
                        gap_month_forecast_output,
                        gap_month_baseline_forecast_output,
                    ],
                    outputs=[],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )

            else:
                # component read from blob
                storage_blob = PythonScriptStep(
                    name="read_from_blob_storage",
                    script_name="read_from_blob.py",
                    source_directory=str(
                        os.path.join(
                            required_path, "multivariate_src/read_from_storage"
                        )
                    ),
                    arguments=[
                        "--product_url",
                        config["config"]["data"]["product"],
                        "--location_url",
                        config["config"]["data"]["location"],
                        "--channel_url",
                        config["config"]["data"]["channel"],
                        "--mapping_url",
                        config["config"]["data"]["mapping"],
                        "--historicalSales_url",
                        config["config"]["data"]["historicalSales"],
                        "--external_df_url",
                        config["config"]["data"]["externalfactors"],
                        "--calendar_url",
                        config["config"]["data"]["calendar"],
                        "--storage_accont_key",
                        ***_storage_key,
                        "--product",
                        product,
                        "--location",
                        location,
                        "--channel",
                        channel,
                        "--mapping",
                        mapping,
                        "--calendar",
                        calendar,
                        "--historicalSales",
                        sales,
                        "--external",
                        external_raw,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--***_connection_string",
                        ***_connection_string,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    outputs=[
                        product,
                        location,
                        channel,
                        mapping,
                        sales,
                        external_raw,
                        calendar,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # component config
                configuaration = PythonScriptStep(
                    name="configuaration",
                    script_name="read_config.py",
                    source_directory=str(
                        os.path.join(required_path, "multivariate_src/configuration")
                    ),
                    arguments=[
                        "--version",
                        version,
                        "--UI_config",
                        ui_config,
                        "--date_patterns",
                        date_patterns,
                        "--model_config",
                        model_config,
                        "--pipeline_setting",
                        pipeline_setting,
                        "--profile_config",
                        profile_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--storage_accont_key",
                        dpai_storage_key,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    outputs=[
                        ui_config,
                        date_patterns,
                        model_config,
                        pipeline_setting,
                        profile_config,
                        profile_outlier_settings,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # component transformation sales
                transform = PythonScriptStep(
                    name="transform",
                    script_name="transform.py",
                    source_directory=str(
                        os.path.join(required_path, "multivariate_src/tramsformation")
                    ),
                    arguments=[
                        "--product",
                        product,
                        "--location",
                        location,
                        "--channel",
                        channel,
                        "--mapping",
                        mapping,
                        "--sales",
                        sales,
                        "--calendar",
                        calendar,
                        "--date_patterns",
                        date_patterns,
                        "--transform_df",
                        transform_df,
                        "--unit_price_df",
                        unit_price_df,
                        "--mapping_id_name_df",
                        mapping_id_name_df,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--csa_transformed_df",
                        csa_transformed_df,
                        "--***_connection_string",
                        ***_connection_string,
                        "--config_ds",
                        config_ds,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        product,
                        location,
                        channel,
                        mapping,
                        sales,
                        date_patterns,
                        calendar,
                    ],
                    outputs=[
                        transform_df,
                        unit_price_df,
                        mapping_id_name_df,
                        csa_transformed_df,
                        config_ds,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )

                # external factor transformation
                transform_external = PythonScriptStep(
                    name="transform_external",
                    script_name="transform.py",
                    source_directory=str(
                        os.path.join(
                            required_path, "multivariate_src/external_transformation"
                        )
                    ),
                    arguments=[
                        "--date_patterns",
                        date_patterns,
                        "--external",
                        external_raw,
                        "--transform_df",
                        transform_df,
                        "--unit_price_df",
                        unit_price_df,
                        "--mapping_id_name_df",
                        mapping_id_name_df,
                        "--external_transformed",
                        external_transformed,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--sales",
                        sales,
                        "--calendar",
                        calendar,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        date_patterns,
                        external_raw,
                        transform_df,
                        unit_price_df,
                        mapping_id_name_df,
                        sales,
                        calendar,
                    ],
                    outputs=[external_transformed],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # create outlier profiling batch
                transform_batch = PythonScriptStep(
                    name="transform_batch",
                    script_name="batch.py",
                    source_directory=str(
                        os.path.join(
                            required_path, "multivariate_src/batch_dataset_transformed"
                        )
                    ),
                    arguments=[
                        "--transform_df",
                        transform_df,
                        "--transform_df_first_batch",
                        transform_df_first_batch_output,
                        "--transform_df_second_batch",
                        transform_df_second_batch_output,
                        "--transform_df_third_batch",
                        transform_df_third_batch_output,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[transform_df],
                    outputs=[
                        transform_df_first_batch_output,
                        transform_df_second_batch_output,
                        transform_df_third_batch_output,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # outlier profiling on first batch
                outlier_profiling_first_batch = PythonScriptStep(
                    name="outlier_profiling_first_batch",
                    script_name="outlier_profile.py",
                    source_directory=str(
                        os.path.join(
                            required_path, "multivariate_src/outlier_profiling"
                        )
                    ),
                    arguments=[
                        "--transform_df_batch",
                        transform_df_first_batch_output,
                        "--ui_config",
                        ui_config,
                        "--profile_config",
                        profile_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--outlier_profile_batch",
                        outlier_profile_first_batch,
                        "--batch",
                        "first",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        transform_df_first_batch_output,
                        ui_config,
                        profile_config,
                        profile_outlier_settings,
                    ],
                    outputs=[outlier_profile_first_batch],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # outlier profiling on second batch
                outlier_profiling_second_batch = PythonScriptStep(
                    name="outlier_profiling_second_batch",
                    script_name="outlier_profile.py",
                    source_directory=str(
                        os.path.join(
                            required_path, "multivariate_src/outlier_profiling"
                        )
                    ),
                    arguments=[
                        "--transform_df_batch",
                        transform_df_second_batch_output,
                        "--ui_config",
                        ui_config,
                        "--profile_config",
                        profile_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--outlier_profile_batch",
                        outlier_profile_second_batch,
                        "--batch",
                        "second",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        transform_df_second_batch_output,
                        ui_config,
                        profile_config,
                        profile_outlier_settings,
                    ],
                    outputs=[outlier_profile_second_batch],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # outlier profiling on third batch
                outlier_profiling_third_batch = PythonScriptStep(
                    name="outlier_profiling_third_batch",
                    script_name="outlier_profile.py",
                    source_directory=str(
                        os.path.join(
                            required_path, "multivariate_src/outlier_profiling"
                        )
                    ),
                    arguments=[
                        "--transform_df_batch",
                        transform_df_third_batch_output,
                        "--ui_config",
                        ui_config,
                        "--profile_config",
                        profile_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--outlier_profile_batch",
                        outlier_profile_third_batch,
                        "--batch",
                        "third",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        transform_df_third_batch_output,
                        ui_config,
                        profile_config,
                        profile_outlier_settings,
                    ],
                    outputs=[outlier_profile_third_batch],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # forecast profiling first batch
                forecast_profiling_first_batch = PythonScriptStep(
                    name="forecast_profiling_first_batch",
                    script_name="forecast_profile.py",
                    source_directory=str(
                        os.path.join(
                            required_path, "multivariate_src/forecast_profiling"
                        )
                    ),
                    arguments=[
                        "--batch",
                        outlier_profile_first_batch,
                        "--ui_config",
                        ui_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--forecast_profiling_output",
                        forecast_profiling_first_batch_output,
                        "--transformed_df",
                        transform_df,
                        "--batch_position",
                        "first",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        outlier_profile_first_batch,
                        ui_config,
                        profile_outlier_settings,
                        transform_df,
                    ],
                    outputs=[forecast_profiling_first_batch_output],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # forecast profiling second batch
                forecast_profiling_second_batch = PythonScriptStep(
                    name="forecast_profiling_second_batch",
                    script_name="forecast_profile.py",
                    source_directory=str(
                        os.path.join(
                            required_path, "multivariate_src/forecast_profiling"
                        )
                    ),
                    arguments=[
                        "--batch",
                        outlier_profile_second_batch,
                        "--ui_config",
                        ui_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--forecast_profiling_output",
                        forecast_profiling_second_batch_output,
                        "--transformed_df",
                        transform_df,
                        "--batch_position",
                        "second",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        outlier_profile_second_batch,
                        ui_config,
                        profile_outlier_settings,
                        transform_df,
                    ],
                    outputs=[forecast_profiling_second_batch_output],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # forecast profiling third batch
                forecast_profiling_third_batch = PythonScriptStep(
                    name="forecast_profiling_third_batch",
                    script_name="forecast_profile.py",
                    source_directory=str(
                        os.path.join(
                            required_path, "multivariate_src/forecast_profiling"
                        )
                    ),
                    arguments=[
                        "--batch",
                        outlier_profile_third_batch,
                        "--ui_config",
                        ui_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--forecast_profiling_output",
                        forecast_profiling_third_batch_output,
                        "--transformed_df",
                        transform_df,
                        "--batch_position",
                        "third",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        outlier_profile_third_batch,
                        ui_config,
                        profile_outlier_settings,
                        transform_df,
                    ],
                    outputs=[forecast_profiling_third_batch_output],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # forecast for first batch
                forecast_first_batch = PythonScriptStep(
                    name="forecast_first_batch",
                    script_name="forecast.py",
                    source_directory=str(
                        os.path.join(
                            required_path, "multivariate_src/forecast_modeling"
                        )
                    ),
                    arguments=[
                        "--batch",
                        forecast_profiling_first_batch_output,
                        "--ui_config",
                        ui_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--date_patterns",
                        date_patterns,
                        "--profile_config",
                        profile_config,
                        "--model_config",
                        model_config,
                        "--pipeline_setting",
                        pipeline_setting,
                        "--forecast_output",
                        forecast_first_batch_output,
                        "--outlier_output",
                        outlier_first_batch_output,
                        "--profiling_output",
                        profiling_first_batch_output,
                        "--residual_output",
                        residual_first_batch_output,
                        "--baseline_output",
                        baseline_forecast_first_batch_output,
                        "--transformed_df",
                        transform_df,
                        "--external_df",
                        external_transformed,
                        "--batch_position",
                        "first",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--***_container_string",
                        ***_connection_string,
                        "--config_ds",
                        config_ds,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        forecast_profiling_first_batch_output,
                        ui_config,
                        profile_outlier_settings,
                        date_patterns,
                        profile_config,
                        model_config,
                        pipeline_setting,
                        transform_df,
                        external_transformed,
                        config_ds,
                    ],
                    outputs=[
                        forecast_first_batch_output,
                        outlier_first_batch_output,
                        profiling_first_batch_output,
                        baseline_forecast_first_batch_output,
                        residual_first_batch_output,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # forecast for second batch
                forecast_second_batch = PythonScriptStep(
                    name="forecast_second_batch",
                    script_name="forecast.py",
                    source_directory=str(
                        os.path.join(
                            required_path, "multivariate_src/forecast_modeling"
                        )
                    ),
                    arguments=[
                        "--batch",
                        forecast_profiling_second_batch_output,
                        "--ui_config",
                        ui_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--date_patterns",
                        date_patterns,
                        "--profile_config",
                        profile_config,
                        "--model_config",
                        model_config,
                        "--pipeline_setting",
                        pipeline_setting,
                        "--forecast_output",
                        forecast_second_batch_output,
                        "--outlier_output",
                        outlier_second_batch_output,
                        "--profiling_output",
                        profiling_second_batch_output,
                        "--residual_output",
                        residual_second_batch_output,
                        "--baseline_output",
                        baseline_forecast_second_batch_output,
                        "--transformed_df",
                        transform_df,
                        "--external_df",
                        external_transformed,
                        "--batch_position",
                        "second",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--***_container_string",
                        ***_connection_string,
                        "--config_ds",
                        config_ds,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        forecast_profiling_second_batch_output,
                        ui_config,
                        profile_outlier_settings,
                        date_patterns,
                        profile_config,
                        model_config,
                        pipeline_setting,
                        transform_df,
                        external_transformed,
                        config_ds,
                    ],
                    outputs=[
                        forecast_second_batch_output,
                        outlier_second_batch_output,
                        profiling_second_batch_output,
                        residual_second_batch_output,
                        baseline_forecast_second_batch_output,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # forecast for third batch
                forecast_third_batch = PythonScriptStep(
                    name="forecast_third_batch",
                    script_name="forecast.py",
                    source_directory=str(
                        os.path.join(
                            required_path, "multivariate_src/forecast_modeling"
                        )
                    ),
                    arguments=[
                        "--batch",
                        forecast_profiling_third_batch_output,
                        "--ui_config",
                        ui_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--date_patterns",
                        date_patterns,
                        "--profile_config",
                        profile_config,
                        "--model_config",
                        model_config,
                        "--pipeline_setting",
                        pipeline_setting,
                        "--forecast_output",
                        forecast_third_batch_output,
                        "--outlier_output",
                        outlier_third_batch_output,
                        "--profiling_output",
                        profiling_third_batch_output,
                        "--residual_output",
                        residual_third_batch_output,
                        "--baseline_output",
                        baseline_forecast_third_batch_output,
                        "--transformed_df",
                        transform_df,
                        "--external_df",
                        external_transformed,
                        "--batch_position",
                        "third",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--***_container_string",
                        ***_connection_string,
                        "--config_ds",
                        config_ds,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        forecast_profiling_third_batch_output,
                        ui_config,
                        profile_outlier_settings,
                        date_patterns,
                        profile_config,
                        model_config,
                        pipeline_setting,
                        transform_df,
                        external_transformed,
                        config_ds,
                    ],
                    outputs=[
                        forecast_third_batch_output,
                        outlier_third_batch_output,
                        profiling_third_batch_output,
                        baseline_forecast_third_batch_output,
                        residual_third_batch_output,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                collect_forecast_result = PythonScriptStep(
                    name="collect_forecast_result",
                    script_name="result.py",
                    source_directory=str(
                        os.path.join(required_path, "multivariate_src/collect_result")
                    ),
                    arguments=[
                        "--forecast_first_batch_output",
                        forecast_first_batch_output,
                        "--outlier_first_batch_output",
                        outlier_first_batch_output,
                        "--profiling_first_batch_output",
                        profiling_first_batch_output,
                        "--forecast_second_batch_output",
                        forecast_second_batch_output,
                        "--outlier_second_batch_output",
                        outlier_second_batch_output,
                        "--profiling_second_batch_output",
                        profiling_second_batch_output,
                        "--forecast_third_batch_output",
                        forecast_third_batch_output,
                        "--outlier_third_batch_output",
                        outlier_third_batch_output,
                        "--profiling_third_batch_output",
                        profiling_third_batch_output,
                        "--baseline_forecast_first_batch_output",
                        baseline_forecast_first_batch_output,
                        "--residual_first_batch_output",
                        residual_first_batch_output,
                        "--baseline_forecast_second_batch_output",
                        baseline_forecast_second_batch_output,
                        "--residual_second_batch_output",
                        residual_second_batch_output,
                        "--baseline_forecast_third_batch_output",
                        baseline_forecast_third_batch_output,
                        "--residual_third_batch_output",
                        residual_third_batch_output,
                        "--forecast_output",
                        forecast_output,
                        "--outlier_output",
                        outlier_output,
                        "--profiling_output",
                        profiling_output,
                        "--baseline_output",
                        baseline_output,
                        "--residual_output",
                        residual_output,
                        "--unit_price",
                        unit_price_df,
                        "--mapping_id",
                        mapping_id_name_df,
                        "--transformed_df",
                        transform_df,
                        "--calendar",
                        calendar,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--gap_month_forecast_output",
                        gap_month_forecast_output,
                        "--config_ds",
                        config_ds,
                        "--gap_month_baseline_forecast_output",
                        gap_month_baseline_forecast_output,
                        "--sales",
                        sales,
                        "--product",
                        product,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        forecast_first_batch_output,
                        outlier_first_batch_output,
                        profiling_first_batch_output,
                        forecast_second_batch_output,
                        outlier_second_batch_output,
                        profiling_second_batch_output,
                        forecast_third_batch_output,
                        outlier_third_batch_output,
                        profiling_third_batch_output,
                        baseline_forecast_first_batch_output,
                        residual_first_batch_output,
                        baseline_forecast_second_batch_output,
                        residual_second_batch_output,
                        baseline_forecast_third_batch_output,
                        residual_third_batch_output,
                        unit_price_df,
                        mapping_id_name_df,
                        transform_df,
                        calendar,
                        config_ds,
                        sales,
                        product,
                    ],
                    outputs=[
                        forecast_output,
                        outlier_output,
                        profiling_output,
                        baseline_output,
                        residual_output,
                        gap_month_forecast_output,
                        gap_month_baseline_forecast_output,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # upload to storage
                upload_files_to_storage = PythonScriptStep(
                    name="upload_files_to_storage",
                    script_name="upload.py",
                    source_directory=str(
                        os.path.join(
                            required_path, "multivariate_src/upload_to_storage"
                        )
                    ),
                    arguments=[
                        "--forecast",
                        forecast_output,
                        "--outlier",
                        outlier_output,
                        "--profiling",
                        profiling_output,
                        "--transform",
                        transform_df,
                        "--baseline_output",
                        baseline_output,
                        "--residual_output",
                        residual_output,
                        "--external_transformed_output",
                        external_transformed,
                        "--storage_accont_key",
                        ***_connection_string,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--csa_transformed_df",
                        csa_transformed_df,
                        "--gap_month_forecast_output",
                        gap_month_forecast_output,
                        "--gap_month_baseline_forecast_output",
                        gap_month_baseline_forecast_output,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        forecast_output,
                        outlier_output,
                        profiling_output,
                        transform_df,
                        baseline_output,
                        residual_output,
                        external_transformed,
                        csa_transformed_df,
                        gap_month_forecast_output,
                        gap_month_baseline_forecast_output,
                    ],
                    outputs=[],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )

        else:
            if dfu_count < 100:
                # component read from blob
                storage_blob = PythonScriptStep(
                    name="read_from_blob_storage",
                    script_name="read_from_blob.py",
                    source_directory=str(
                        os.path.join(required_path, "univariate_src/read_from_storage")
                    ),
                    arguments=[
                        "--product_url",
                        config["config"]["data"]["product"],
                        "--location_url",
                        config["config"]["data"]["location"],
                        "--channel_url",
                        config["config"]["data"]["channel"],
                        "--mapping_url",
                        config["config"]["data"]["mapping"],
                        "--historicalSales_url",
                        config["config"]["data"]["historicalSales"],
                        "--calendar_url",
                        config["config"]["data"]["calendar"],
                        "--storage_accont_key",
                        ***_storage_key,
                        "--product",
                        product,
                        "--location",
                        location,
                        "--channel",
                        channel,
                        "--mapping",
                        mapping,
                        "--calendar",
                        calendar,
                        "--historicalSales",
                        sales,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--***_connection_string",
                        ***_connection_string,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    outputs=[product, location, channel, mapping, sales, calendar],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # component config
                configuaration = PythonScriptStep(
                    name="configuaration",
                    script_name="read_config.py",
                    source_directory=str(
                        os.path.join(required_path, "univariate_src/configuration")
                    ),
                    arguments=[
                        "--version",
                        version,
                        "--UI_config",
                        ui_config,
                        "--date_patterns",
                        date_patterns,
                        "--model_config",
                        model_config,
                        "--pipeline_setting",
                        pipeline_setting,
                        "--profile_config",
                        profile_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--storage_accont_key",
                        dpai_storage_key,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    outputs=[
                        ui_config,
                        date_patterns,
                        model_config,
                        pipeline_setting,
                        profile_config,
                        profile_outlier_settings,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # component transformation sales
                transform = PythonScriptStep(
                    name="transform",
                    script_name="transform.py",
                    source_directory=str(
                        os.path.join(required_path, "univariate_src/tramsformation")
                    ),
                    arguments=[
                        "--product",
                        product,
                        "--location",
                        location,
                        "--channel",
                        channel,
                        "--mapping",
                        mapping,
                        "--sales",
                        sales,
                        "--calendar",
                        calendar,
                        "--date_patterns",
                        date_patterns,
                        "--transform_df",
                        transform_df,
                        "--unit_price_df",
                        unit_price_df,
                        "--mapping_id_name_df",
                        mapping_id_name_df,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--csa_transformed_df",
                        csa_transformed_df,
                        "--***_connection_string",
                        ***_connection_string,
                        "--config_ds",
                        config_ds,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        product,
                        location,
                        channel,
                        mapping,
                        sales,
                        date_patterns,
                        calendar,
                    ],
                    outputs=[
                        transform_df,
                        unit_price_df,
                        mapping_id_name_df,
                        csa_transformed_df,
                        config_ds,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # create outlier profiling batch
                transform_batch = PythonScriptStep(
                    name="transform_batch",
                    script_name="batch.py",
                    source_directory=str(
                        os.path.join(
                            required_path, "univariate_src/batch_dataset_transformed"
                        )
                    ),
                    arguments=[
                        "--transform_df",
                        transform_df,
                        "--transform_df_first_batch",
                        transform_df_first_batch_output,
                        "--transform_df_second_batch",
                        transform_df_second_batch_output,
                        "--transform_df_third_batch",
                        transform_df_third_batch_output,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[transform_df],
                    outputs=[
                        transform_df_first_batch_output,
                        transform_df_second_batch_output,
                        transform_df_third_batch_output,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # outlier profiling on first batch
                outlier_profiling_first_batch = PythonScriptStep(
                    name="outlier_profiling_first_batch",
                    script_name="outlier_profile.py",
                    source_directory=str(
                        os.path.join(required_path, "univariate_src/outlier_profiling")
                    ),
                    arguments=[
                        "--transform_df_batch",
                        transform_df_first_batch_output,
                        "--ui_config",
                        ui_config,
                        "--profile_config",
                        profile_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--outlier_profile_batch",
                        outlier_profile_first_batch,
                        "--batch",
                        "first",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        transform_df_first_batch_output,
                        ui_config,
                        profile_config,
                        profile_outlier_settings,
                    ],
                    outputs=[outlier_profile_first_batch],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # forecast profiling first batch
                forecast_profiling_first_batch = PythonScriptStep(
                    name="forecast_profiling_first_batch",
                    script_name="forecast_profile.py",
                    source_directory=str(
                        os.path.join(required_path, "univariate_src/forecast_profiling")
                    ),
                    arguments=[
                        "--batch",
                        outlier_profile_first_batch,
                        "--ui_config",
                        ui_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--forecast_profiling_output",
                        forecast_profiling_first_batch_output,
                        "--transformed_df",
                        transform_df,
                        "--batch_position",
                        "first",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        outlier_profile_first_batch,
                        ui_config,
                        profile_outlier_settings,
                        transform_df,
                    ],
                    outputs=[forecast_profiling_first_batch_output],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # forecast for first batch
                forecast_first_batch = PythonScriptStep(
                    name="forecast_first_batch",
                    script_name="forecast.py",
                    source_directory=str(
                        os.path.join(required_path, "univariate_src/forecast_modeling")
                    ),
                    arguments=[
                        "--batch",
                        forecast_profiling_first_batch_output,
                        "--ui_config",
                        ui_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--date_patterns",
                        date_patterns,
                        "--profile_config",
                        profile_config,
                        "--model_config",
                        model_config,
                        "--pipeline_setting",
                        pipeline_setting,
                        "--forecast_output",
                        forecast_first_batch_output,
                        "--outlier_output",
                        outlier_first_batch_output,
                        "--profiling_output",
                        profiling_first_batch_output,
                        "--residual_output",
                        residual_first_batch_output,
                        "--baseline_output",
                        baseline_forecast_first_batch_output,
                        "--transformed_df",
                        transform_df,
                        "--batch_position",
                        "first",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--***_container_string",
                        ***_connection_string,
                        "--config_ds",
                        config_ds,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        forecast_profiling_first_batch_output,
                        ui_config,
                        profile_outlier_settings,
                        date_patterns,
                        profile_config,
                        model_config,
                        pipeline_setting,
                        transform_df,
                        config_ds,
                    ],
                    outputs=[
                        forecast_first_batch_output,
                        outlier_first_batch_output,
                        profiling_first_batch_output,
                        baseline_forecast_first_batch_output,
                        residual_first_batch_output,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                collect_forecast_result = PythonScriptStep(
                    name="collect_forecast_result",
                    script_name="mini_batch_result.py",
                    source_directory=str(
                        os.path.join(required_path, "univariate_src/collect_result")
                    ),
                    arguments=[
                        "--forecast_first_batch_output",
                        forecast_first_batch_output,
                        "--outlier_first_batch_output",
                        outlier_first_batch_output,
                        "--profiling_first_batch_output",
                        profiling_first_batch_output,
                        "--baseline_forecast_first_batch_output",
                        baseline_forecast_first_batch_output,
                        "--residual_first_batch_output",
                        residual_first_batch_output,
                        "--forecast_output",
                        forecast_output,
                        "--outlier_output",
                        outlier_output,
                        "--profiling_output",
                        profiling_output,
                        "--baseline_output",
                        baseline_output,
                        "--residual_output",
                        residual_output,
                        "--unit_price",
                        unit_price_df,
                        "--mapping_id",
                        mapping_id_name_df,
                        "--transformed_df",
                        transform_df,
                        "--calendar",
                        calendar,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--gap_month_forecast_output",
                        gap_month_forecast_output,
                        "--config_ds",
                        config_ds,
                        "--gap_month_baseline_forecast_output",
                        gap_month_baseline_forecast_output,
                        "--sales",
                        sales,
                        "--product",
                        product,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        forecast_first_batch_output,
                        outlier_first_batch_output,
                        profiling_first_batch_output,
                        baseline_forecast_first_batch_output,
                        residual_first_batch_output,
                        unit_price_df,
                        mapping_id_name_df,
                        transform_df,
                        calendar,
                        config_ds,
                        sales,
                        product,
                    ],
                    outputs=[
                        forecast_output,
                        outlier_output,
                        profiling_output,
                        baseline_output,
                        residual_output,
                        gap_month_forecast_output,
                        gap_month_baseline_forecast_output,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # upload to storage
                upload_files_to_storage = PythonScriptStep(
                    name="upload_files_to_storage",
                    script_name="upload.py",
                    source_directory=str(
                        os.path.join(required_path, "univariate_src/upload_to_storage")
                    ),
                    arguments=[
                        "--forecast",
                        forecast_output,
                        "--outlier",
                        outlier_output,
                        "--profiling",
                        profiling_output,
                        "--transform",
                        transform_df,
                        "--residual_output",
                        residual_output,
                        "--baseline_output",
                        baseline_output,
                        "--storage_accont_key",
                        ***_connection_string,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--csa_transformed_df",
                        csa_transformed_df,
                        "--gap_month_forecast_output",
                        gap_month_forecast_output,
                        "--gap_month_baseline_forecast_output",
                        gap_month_baseline_forecast_output,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        forecast_output,
                        outlier_output,
                        profiling_output,
                        transform_df,
                        residual_output,
                        baseline_output,
                        csa_transformed_df,
                        gap_month_forecast_output,
                        gap_month_baseline_forecast_output,
                    ],
                    outputs=[],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
            else:
                # component read from blob
                storage_blob = PythonScriptStep(
                    name="read_from_blob_storage",
                    script_name="read_from_blob.py",
                    source_directory=str(
                        os.path.join(required_path, "univariate_src/read_from_storage")
                    ),
                    arguments=[
                        "--product_url",
                        config["config"]["data"]["product"],
                        "--location_url",
                        config["config"]["data"]["location"],
                        "--channel_url",
                        config["config"]["data"]["channel"],
                        "--mapping_url",
                        config["config"]["data"]["mapping"],
                        "--historicalSales_url",
                        config["config"]["data"]["historicalSales"],
                        "--calendar_url",
                        config["config"]["data"]["calendar"],
                        "--storage_accont_key",
                        ***_storage_key,
                        "--product",
                        product,
                        "--location",
                        location,
                        "--channel",
                        channel,
                        "--mapping",
                        mapping,
                        "--calendar",
                        calendar,
                        "--historicalSales",
                        sales,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--***_connection_string",
                        ***_connection_string,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    outputs=[product, location, channel, mapping, sales, calendar],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # component config
                configuaration = PythonScriptStep(
                    name="configuaration",
                    script_name="read_config.py",
                    source_directory=str(
                        os.path.join(required_path, "univariate_src/configuration")
                    ),
                    arguments=[
                        "--version",
                        version,
                        "--UI_config",
                        ui_config,
                        "--date_patterns",
                        date_patterns,
                        "--model_config",
                        model_config,
                        "--pipeline_setting",
                        pipeline_setting,
                        "--profile_config",
                        profile_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--storage_accont_key",
                        dpai_storage_key,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    outputs=[
                        ui_config,
                        date_patterns,
                        model_config,
                        pipeline_setting,
                        profile_config,
                        profile_outlier_settings,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # component transformation sales
                transform = PythonScriptStep(
                    name="transform",
                    script_name="transform.py",
                    source_directory=str(
                        os.path.join(required_path, "univariate_src/tramsformation")
                    ),
                    arguments=[
                        "--product",
                        product,
                        "--location",
                        location,
                        "--channel",
                        channel,
                        "--mapping",
                        mapping,
                        "--sales",
                        sales,
                        "--calendar",
                        calendar,
                        "--date_patterns",
                        date_patterns,
                        "--transform_df",
                        transform_df,
                        "--unit_price_df",
                        unit_price_df,
                        "--mapping_id_name_df",
                        mapping_id_name_df,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--csa_transformed_df",
                        csa_transformed_df,
                        "--***_connection_string",
                        ***_connection_string,
                        "--config_ds",
                        config_ds,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        product,
                        location,
                        channel,
                        mapping,
                        sales,
                        date_patterns,
                        calendar,
                    ],
                    outputs=[
                        transform_df,
                        unit_price_df,
                        mapping_id_name_df,
                        csa_transformed_df,
                        config_ds,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # create outlier profiling batch
                transform_batch = PythonScriptStep(
                    name="transform_batch",
                    script_name="batch.py",
                    source_directory=str(
                        os.path.join(
                            required_path, "univariate_src/batch_dataset_transformed"
                        )
                    ),
                    arguments=[
                        "--transform_df",
                        transform_df,
                        "--transform_df_first_batch",
                        transform_df_first_batch_output,
                        "--transform_df_second_batch",
                        transform_df_second_batch_output,
                        "--transform_df_third_batch",
                        transform_df_third_batch_output,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[transform_df],
                    outputs=[
                        transform_df_first_batch_output,
                        transform_df_second_batch_output,
                        transform_df_third_batch_output,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # outlier profiling on first batch
                outlier_profiling_first_batch = PythonScriptStep(
                    name="outlier_profiling_first_batch",
                    script_name="outlier_profile.py",
                    source_directory=str(
                        os.path.join(required_path, "univariate_src/outlier_profiling")
                    ),
                    arguments=[
                        "--transform_df_batch",
                        transform_df_first_batch_output,
                        "--ui_config",
                        ui_config,
                        "--profile_config",
                        profile_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--outlier_profile_batch",
                        outlier_profile_first_batch,
                        "--batch",
                        "first",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        transform_df_first_batch_output,
                        ui_config,
                        profile_config,
                        profile_outlier_settings,
                    ],
                    outputs=[outlier_profile_first_batch],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # outlier profiling on second batch
                outlier_profiling_second_batch = PythonScriptStep(
                    name="outlier_profiling_second_batch",
                    script_name="outlier_profile.py",
                    source_directory=str(
                        os.path.join(required_path, "univariate_src/outlier_profiling")
                    ),
                    arguments=[
                        "--transform_df_batch",
                        transform_df_second_batch_output,
                        "--ui_config",
                        ui_config,
                        "--profile_config",
                        profile_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--outlier_profile_batch",
                        outlier_profile_second_batch,
                        "--batch",
                        "second",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        transform_df_second_batch_output,
                        ui_config,
                        profile_config,
                        profile_outlier_settings,
                    ],
                    outputs=[outlier_profile_second_batch],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # outlier profiling on third batch
                outlier_profiling_third_batch = PythonScriptStep(
                    name="outlier_profiling_third_batch",
                    script_name="outlier_profile.py",
                    source_directory=str(
                        os.path.join(required_path, "univariate_src/outlier_profiling")
                    ),
                    arguments=[
                        "--transform_df_batch",
                        transform_df_third_batch_output,
                        "--ui_config",
                        ui_config,
                        "--profile_config",
                        profile_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--outlier_profile_batch",
                        outlier_profile_third_batch,
                        "--batch",
                        "third",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        transform_df_third_batch_output,
                        ui_config,
                        profile_config,
                        profile_outlier_settings,
                    ],
                    outputs=[outlier_profile_third_batch],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # forecast profiling first batch
                forecast_profiling_first_batch = PythonScriptStep(
                    name="forecast_profiling_first_batch",
                    script_name="forecast_profile.py",
                    source_directory=str(
                        os.path.join(required_path, "univariate_src/forecast_profiling")
                    ),
                    arguments=[
                        "--batch",
                        outlier_profile_first_batch,
                        "--ui_config",
                        ui_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--forecast_profiling_output",
                        forecast_profiling_first_batch_output,
                        "--transformed_df",
                        transform_df,
                        "--batch_position",
                        "first",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        outlier_profile_first_batch,
                        ui_config,
                        profile_outlier_settings,
                        transform_df,
                    ],
                    outputs=[forecast_profiling_first_batch_output],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # forecast profiling second batch
                forecast_profiling_second_batch = PythonScriptStep(
                    name="forecast_profiling_second_batch",
                    script_name="forecast_profile.py",
                    source_directory=str(
                        os.path.join(required_path, "univariate_src/forecast_profiling")
                    ),
                    arguments=[
                        "--batch",
                        outlier_profile_second_batch,
                        "--ui_config",
                        ui_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--forecast_profiling_output",
                        forecast_profiling_second_batch_output,
                        "--transformed_df",
                        transform_df,
                        "--batch_position",
                        "second",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        outlier_profile_second_batch,
                        ui_config,
                        profile_outlier_settings,
                        transform_df,
                    ],
                    outputs=[forecast_profiling_second_batch_output],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # forecast profiling third batch
                forecast_profiling_third_batch = PythonScriptStep(
                    name="forecast_profiling_third_batch",
                    script_name="forecast_profile.py",
                    source_directory=str(
                        os.path.join(required_path, "univariate_src/forecast_profiling")
                    ),
                    arguments=[
                        "--batch",
                        outlier_profile_third_batch,
                        "--ui_config",
                        ui_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--forecast_profiling_output",
                        forecast_profiling_third_batch_output,
                        "--transformed_df",
                        transform_df,
                        "--batch_position",
                        "third",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        outlier_profile_third_batch,
                        ui_config,
                        profile_outlier_settings,
                        transform_df,
                    ],
                    outputs=[forecast_profiling_third_batch_output],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # forecast for first batch
                forecast_first_batch = PythonScriptStep(
                    name="forecast_first_batch",
                    script_name="forecast.py",
                    source_directory=str(
                        os.path.join(required_path, "univariate_src/forecast_modeling")
                    ),
                    arguments=[
                        "--batch",
                        forecast_profiling_first_batch_output,
                        "--ui_config",
                        ui_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--date_patterns",
                        date_patterns,
                        "--profile_config",
                        profile_config,
                        "--model_config",
                        model_config,
                        "--pipeline_setting",
                        pipeline_setting,
                        "--forecast_output",
                        forecast_first_batch_output,
                        "--outlier_output",
                        outlier_first_batch_output,
                        "--profiling_output",
                        profiling_first_batch_output,
                        "--residual_output",
                        residual_first_batch_output,
                        "--baseline_output",
                        baseline_forecast_first_batch_output,
                        "--transformed_df",
                        transform_df,
                        "--batch_position",
                        "first",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--***_container_string",
                        ***_connection_string,
                        "--config_ds",
                        config_ds,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        forecast_profiling_first_batch_output,
                        ui_config,
                        profile_outlier_settings,
                        date_patterns,
                        profile_config,
                        model_config,
                        pipeline_setting,
                        transform_df,
                        config_ds,
                    ],
                    outputs=[
                        forecast_first_batch_output,
                        outlier_first_batch_output,
                        profiling_first_batch_output,
                        baseline_forecast_first_batch_output,
                        residual_first_batch_output,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # forecast for second batch
                forecast_second_batch = PythonScriptStep(
                    name="forecast_second_batch",
                    script_name="forecast.py",
                    source_directory=str(
                        os.path.join(required_path, "univariate_src/forecast_modeling")
                    ),
                    arguments=[
                        "--batch",
                        forecast_profiling_second_batch_output,
                        "--ui_config",
                        ui_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--date_patterns",
                        date_patterns,
                        "--profile_config",
                        profile_config,
                        "--model_config",
                        model_config,
                        "--pipeline_setting",
                        pipeline_setting,
                        "--forecast_output",
                        forecast_second_batch_output,
                        "--outlier_output",
                        outlier_second_batch_output,
                        "--profiling_output",
                        profiling_second_batch_output,
                        "--residual_output",
                        residual_second_batch_output,
                        "--baseline_output",
                        baseline_forecast_second_batch_output,
                        "--transformed_df",
                        transform_df,
                        "--batch_position",
                        "second",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--***_container_string",
                        ***_connection_string,
                        "--config_ds",
                        config_ds,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        forecast_profiling_second_batch_output,
                        ui_config,
                        profile_outlier_settings,
                        date_patterns,
                        profile_config,
                        model_config,
                        pipeline_setting,
                        transform_df,
                        config_ds,
                    ],
                    outputs=[
                        forecast_second_batch_output,
                        outlier_second_batch_output,
                        profiling_second_batch_output,
                        residual_second_batch_output,
                        baseline_forecast_second_batch_output,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # forecast for third batch
                forecast_third_batch = PythonScriptStep(
                    name="forecast_third_batch",
                    script_name="forecast.py",
                    source_directory=str(
                        os.path.join(required_path, "univariate_src/forecast_modeling")
                    ),
                    arguments=[
                        "--batch",
                        forecast_profiling_third_batch_output,
                        "--ui_config",
                        ui_config,
                        "--profile_outlier_settings",
                        profile_outlier_settings,
                        "--date_patterns",
                        date_patterns,
                        "--profile_config",
                        profile_config,
                        "--model_config",
                        model_config,
                        "--pipeline_setting",
                        pipeline_setting,
                        "--forecast_output",
                        forecast_third_batch_output,
                        "--outlier_output",
                        outlier_third_batch_output,
                        "--profiling_output",
                        profiling_third_batch_output,
                        "--residual_output",
                        residual_third_batch_output,
                        "--baseline_output",
                        baseline_forecast_third_batch_output,
                        "--transformed_df",
                        transform_df,
                        "--batch_position",
                        "third",
                        "--orchestrator_url",
                        orchestrator_url,
                        "--***_container_string",
                        ***_connection_string,
                        "--config_ds",
                        config_ds,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        forecast_profiling_third_batch_output,
                        ui_config,
                        profile_outlier_settings,
                        date_patterns,
                        profile_config,
                        model_config,
                        pipeline_setting,
                        transform_df,
                        config_ds,
                    ],
                    outputs=[
                        forecast_third_batch_output,
                        outlier_third_batch_output,
                        profiling_third_batch_output,
                        baseline_forecast_third_batch_output,
                        residual_third_batch_output,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                collect_forecast_result = PythonScriptStep(
                    name="collect_forecast_result",
                    script_name="result.py",
                    source_directory=str(
                        os.path.join(required_path, "univariate_src/collect_result")
                    ),
                    arguments=[
                        "--forecast_first_batch_output",
                        forecast_first_batch_output,
                        "--outlier_first_batch_output",
                        outlier_first_batch_output,
                        "--profiling_first_batch_output",
                        profiling_first_batch_output,
                        "--forecast_second_batch_output",
                        forecast_second_batch_output,
                        "--outlier_second_batch_output",
                        outlier_second_batch_output,
                        "--profiling_second_batch_output",
                        profiling_second_batch_output,
                        "--forecast_third_batch_output",
                        forecast_third_batch_output,
                        "--outlier_third_batch_output",
                        outlier_third_batch_output,
                        "--profiling_third_batch_output",
                        profiling_third_batch_output,
                        "--baseline_forecast_first_batch_output",
                        baseline_forecast_first_batch_output,
                        "--residual_first_batch_output",
                        residual_first_batch_output,
                        "--baseline_forecast_second_batch_output",
                        baseline_forecast_second_batch_output,
                        "--residual_second_batch_output",
                        residual_second_batch_output,
                        "--baseline_forecast_third_batch_output",
                        baseline_forecast_third_batch_output,
                        "--residual_third_batch_output",
                        residual_third_batch_output,
                        "--forecast_output",
                        forecast_output,
                        "--outlier_output",
                        outlier_output,
                        "--profiling_output",
                        profiling_output,
                        "--baseline_output",
                        baseline_output,
                        "--residual_output",
                        residual_output,
                        "--unit_price",
                        unit_price_df,
                        "--mapping_id",
                        mapping_id_name_df,
                        "--transformed_df",
                        transform_df,
                        "--calendar",
                        calendar,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--gap_month_forecast_output",
                        gap_month_forecast_output,
                        "--config_ds",
                        config_ds,
                        "--gap_month_baseline_forecast_output",
                        gap_month_baseline_forecast_output,
                        "--sales",
                        sales,
                        "--product",
                        product,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        forecast_first_batch_output,
                        outlier_first_batch_output,
                        profiling_first_batch_output,
                        forecast_second_batch_output,
                        outlier_second_batch_output,
                        profiling_second_batch_output,
                        forecast_third_batch_output,
                        outlier_third_batch_output,
                        profiling_third_batch_output,
                        baseline_forecast_first_batch_output,
                        residual_first_batch_output,
                        baseline_forecast_second_batch_output,
                        residual_second_batch_output,
                        baseline_forecast_third_batch_output,
                        residual_third_batch_output,
                        unit_price_df,
                        mapping_id_name_df,
                        transform_df,
                        calendar,
                        config_ds,
                        sales,
                        product,
                    ],
                    outputs=[
                        forecast_output,
                        outlier_output,
                        profiling_output,
                        baseline_output,
                        residual_output,
                        gap_month_forecast_output,
                        gap_month_baseline_forecast_output,
                    ],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )
                # upload to storage
                upload_files_to_storage = PythonScriptStep(
                    name="upload_files_to_storage",
                    script_name="upload.py",
                    source_directory=str(
                        os.path.join(required_path, "univariate_src/upload_to_storage")
                    ),
                    arguments=[
                        "--forecast",
                        forecast_output,
                        "--outlier",
                        outlier_output,
                        "--profiling",
                        profiling_output,
                        "--transform",
                        transform_df,
                        "--residual_output",
                        residual_output,
                        "--baseline_output",
                        baseline_output,
                        "--storage_accont_key",
                        ***_connection_string,
                        "--orchestrator_url",
                        orchestrator_url,
                        "--csa_transformed_df",
                        csa_transformed_df,
                        "--gap_month_forecast_output",
                        gap_month_forecast_output,
                        "--gap_month_baseline_forecast_output",
                        gap_month_baseline_forecast_output,
                        "--login_payload",
                        json.dumps(login_payload),
                        "--iam_login_url",
                        iam_login_url,
                    ],
                    inputs=[
                        forecast_output,
                        outlier_output,
                        profiling_output,
                        transform_df,
                        residual_output,
                        baseline_output,
                        csa_transformed_df,
                        gap_month_forecast_output,
                        gap_month_baseline_forecast_output,
                    ],
                    outputs=[],
                    runconfig=run_conf,
                    compute_target=aml_cluster,
                    allow_reuse=True,
                )

        # Create a pipeline with defined components for univariate and multivariate forecast
        if (
            "useFeaturesInForecast" in config["config"]["configuration"]["forecast"]
        ) and (
            len(config["config"]["configuration"]["forecast"]["useFeaturesInForecast"])
            >= 1
        ):
            if dfu_count < 100:
                steps = [
                    [storage_blob, configuaration],
                    [transform],
                    [transform_external],
                    [transform_batch],
                    [outlier_profiling_first_batch],
                    [forecast_profiling_first_batch],
                    [forecast_first_batch],
                    [collect_forecast_result],
                    [upload_files_to_storage],
                ]
            else:
                steps = [
                    [storage_blob, configuaration],
                    [transform],
                    [transform_external],
                    [transform_batch],
                    [
                        outlier_profiling_first_batch,
                        outlier_profiling_second_batch,
                        outlier_profiling_third_batch,
                    ],
                    [
                        forecast_profiling_first_batch,
                        forecast_profiling_second_batch,
                        forecast_profiling_third_batch,
                    ],
                    [forecast_first_batch, forecast_second_batch, forecast_third_batch],
                    [collect_forecast_result],
                    [upload_files_to_storage],
                ]
        else:
            if dfu_count < 100:
                steps = [
                    [storage_blob, configuaration],
                    [transform],
                    [transform_batch],
                    [outlier_profiling_first_batch],
                    [forecast_profiling_first_batch],
                    [forecast_first_batch],
                    [collect_forecast_result],
                    [upload_files_to_storage],
                ]
            else:
                steps = [
                    [storage_blob, configuaration],
                    [transform],
                    [transform_batch],
                    [
                        outlier_profiling_first_batch,
                        outlier_profiling_second_batch,
                        outlier_profiling_third_batch,
                    ],
                    [
                        forecast_profiling_first_batch,
                        forecast_profiling_second_batch,
                        forecast_profiling_third_batch,
                    ],
                    [forecast_first_batch, forecast_second_batch, forecast_third_batch],
                    [collect_forecast_result],
                    [upload_files_to_storage],
                ]
        pipeline = Pipeline(
            ws,
            steps,
        )
        # validate pipeline
        pipeline.validate()
        # create a experiment
        exp = Experiment(ws, config["jobid"])
        # subimit experiment to run
        Run = exp.submit(pipeline)
        while Run.get_status() not in ["Finished", "Failed"]:
            logging.debug("Pipeline with RunID : %s is Running", Run.id)
            time.sleep(30)
        response = {"experiment": config["jobid"], "status": Run.get_status()}
        # delete the cluster once job is complete
        cluster = ws.compute_targets[cluster_name]
        cluster.delete()
        logging.debug("Status : %s", response)
        headers = None
        try:
            successful_response = get_login_response(iam_login_url, login_payload)
            access_token = successful_response.cookies["access_token"]
            headers = {"Cookie": f"access_token={access_token}"}

        except Exception as e:
            print(f"Failed to get token after all retries: {e}")
        if response["status"] == "Finished" and headers is not None:
            res = httpx.get(
                "{}/{}".format(orchestrator_url, config["jobid"]),
                headers=headers,
                timeout=None,
                verify=False,
            )
            status_list = res.json()
            filtered_and_sorted_events_list = filter_and_sort_events(
                status_list["status_updates"]
            )
            has_failed = False
            for item in filtered_and_sorted_events_list:
                if item["status"].split(":")[-1] == "FAILED":
                    has_failed = True
                    break
            if has_failed:
                logging.debug("At least one status is FAILED.")
                payload = {
                    "pipeline": "executed",
                    "status": "FAILED",
                    "logs": "please check componet logs for details",
                }
            else:
                logging.debug("All statuses are successful.")
                payload = {
                    "pipeline": "executed",
                    "status": "SUCCESS",
                    "result": {
                        "forecast_file_path": f"{output_url}/forecast_result/{config['jobid']}/forecast.csv",
                        "outlier_file_path": f"{output_url}/outlier_result/{config['jobid']}/outlier.csv",
                        "model_profile_bag_file_path": f"{output_url}/profile_result/{config['jobid']}/profiling.csv",
                        "transformation_file_path": f"{output_url}/transform_result/{config['jobid']}/wide_df.csv",
                        "baseline_forecast_file_path": f"{output_url}/baseline_forecast_result/{config['jobid']}/baseline_forecast.csv",
                        "residual_file_path": f"{output_url}/residual_result/{config['jobid']}/residual.csv",
                        "classification_file_path": f"{output_url}/csa_transformed_result/{config['jobid']}/wide_csa_df.csv",
                    },
                }

                if reduce(
                    lambda d, k: d.get(k, {}),
                    ["config", "configuration", "forecast"],
                    config,
                ).get("gap_month", False):
                    payload["result"]["gap_month_baseline_forecast_file_path"] = (
                        f"{output_url}/gap_month_baseline_forecast_output_result/{config['jobid']}/gap_month_baseline_forecast.csv"
                    )
                    payload["result"]["gap_month_forecast_file_path"] = (
                        f"{output_url}/gap_month_forecast_output_result/{config['jobid']}/gap_month_forecast.csv"
                    )
            if has_external_features(config):
                payload["result"]["externalfactor_transformation_file_path"] = (
                    f"{output_url}/transform_external_result/{config['jobid']}/external_transformed_df.csv"
                )
        else:
            payload = {
                "pipeline": "executed",
                "status": "FAILED",
                "logs": "please check componet logs for details",
            }
        # httpx.patch("http://52.172.92.11:8000/forecast/{}".format(config["jobid"]),json=payload,timeout=None)
        if headers is not None:
            patch_with_retries(
                "{}/{}".format(orchestrator_url, config["jobid"]),
                headers=headers,
                json=payload,
                timeout=None,
                verify=False,
            )
        else:
            logging.error(
                "Cannot send patch request: headers is None due to authentication failure"
            )
    except Exception:
        # Get the traceback as a string
        TRACEBACK_STRING = traceback.format_exc()
        # Log the traceback
        logging.error(TRACEBACK_STRING)
        # Display the traceback to the user
        logging.debug(TRACEBACK_STRING)
        payload = {
            "pipeline": "not executed",
            "status": "FAILED",
            "logs": str(TRACEBACK_STRING),
        }

        try:
            successful_response = get_login_response(iam_login_url, login_payload)
            access_token = successful_response.cookies["access_token"]
            headers = {"Cookie": f"access_token={access_token}"}

        except Exception as e:
            print(f"Failed to get token after all retries: {e}")

        # httpx.patch("http://52.172.92.11:8000/forecast/{}".format(config["jobid"]),json=payload,timeout=None)
        if headers is not None:
            patch_with_retries(
                "{}/{}".format(orchestrator_url, config["jobid"]),
                headers=headers,
                json=payload,
                timeout=None,
                verify=False,
            )
        else:
            logging.error(
                "Cannot send patch request in exception handler: headers is None due to authentication failure"
            )
