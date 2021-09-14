from pyspark.sql import DataFrame

from minsait.ttaa.datio.common.Constants import *
from minsait.ttaa.datio.common.naming.PlayerInput import *


class Writer:
    #exercise 6
    def write(self, df: DataFrame):
        df \
            .coalesce(1) \
            .write \
            .partitionBy(nationality.name) \
            .mode(OVERWRITE) \
            .parquet(OUTPUT_PATH);