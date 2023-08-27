import os
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Dict, List

import great_expectations as gx
from pyspark.sql import DataFrame


@dataclass
class DeltaDataSet:
    name: str
    curr_data: DataFrame
    primary_keys: List[str]
    storage_path: str
    table_name: str
    data_type: str
    schema: str


class StandardETL(ABC):
    def run_data_validations(self, input_datasets: List[DeltaDataSet]):
        context = gx.get_context(
            context_root_dir=os.path.join(
                os.getcwd(),
                "adventureworks",  # make this changeable
                "great_expectations",  # make this changeable
            )
        )

        validations = []
        for input_dataset in input_datasets:
            validations.append(
                {
                    "batch_request": context.get_datasource("spark_datasource")
                    .get_asset(input_dataset.name)
                    .build_batch_request(dataframe=input_dataset.curr_data),
                    "expectation_suite_name": input_dataset.name,
                }
            )
        return context.run_checkpoint(
            checkpoint_name="dq_checkpoint", validations=validations
        ).list_validation_results()

    @abstractmethod
    def validate_data(self, input_datasets: List[DeltaDataSet]) -> bool:
        results = {}
        for validation in self.run_data_validations(input_datasets).items():
            results = {'input': validation.get('success')}
        return results

    @abstractmethod
    def publish_data(
        self, input_datasets: List[DeltaDataSet], spark, **kwargs
    ) -> None:
        for input_dataset in input_datasets:
            targetDF = spark.read.table(
                f'{input_dataset.schema}.{input_dataset.table_name}'
            )
            (
                targetDF.alias("target")
                .merge(
                    input_dataset.curr_data.alias("source"),
                    input_dataset.primary_keys,
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .whenNotMatchedBySourceDelete()
                .execute()
            )

    @abstractmethod
    def get_bronze_datasets(self, spark, **kwargs) -> List[DeltaDataSet]:
        pass

    # @abstractmethod
    # def get_silver_datasets(
    #    self, input_datasets: List[Dict[str, DataFrame]], **kwargs
    # ) -> List[Dict[str, DataFrame]]:
    #    pass

    # @abstractmethod
    # def get_gold_datasets(
    #    self, input_datasets: List[Dict[str, DataFrame]], **kwargs
    # ) -> List[Dict[str, DataFrame]]:
    #    pass

    def run(self, spark):
        bronze_data_sets = self.get_bronze_datasets(spark)
        self.validate_data(bronze_data_sets)
        self.publish_data(bronze_data_sets, spark)

        # silver_data_sets = self.get_silver_datasets()
        # self.validate_data(silver_data_sets)
        # self.publish_data(silver_data_sets)

        # gold_data_sets = self.get_gold_datasets()
        # self.validate_data(gold_data_sets)
        # self.publish_data(gold_data_sets)
