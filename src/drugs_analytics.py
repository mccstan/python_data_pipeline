# Remove trailing comma from a json.

from argparse import ArgumentParser
from pyspark.sql import SparkSession, functions, DataFrame
from src.utils.spark_utils import json_without_trailing_comma

# parse arguments
parser = ArgumentParser()
parser.add_argument('--drugs-references-path', help='Drugs references path, path to the Drugs references')
parser.add_argument('--analytics-path', help='Analytics output path, path to the analytics results')
args = parser.parse_args()

# initialization
spark = SparkSession.builder.getOrCreate()


def compute_journal_with_most_drugs(drugs_references_dataframe: DataFrame) -> DataFrame:
    """
    Find journals with most unique drugs references
    :type drugs_references_dataframe: object
    """

    return drugs_references_dataframe \
        .select("journal", "drug").where("journal is not null") \
        .distinct() \
        .groupBy("journal").count() \
        .orderBy(functions.col("count").desc()) \
        .limit(1)


drugs_references = spark \
    .read \
    .option("multiline", "true") \
    .json(args.drugs_references_path)

journal_with_most_drugs = compute_journal_with_most_drugs(drugs_references)

journal_with_most_drugs \
    .toPandas() \
    .to_json(f"{args.analytics_path}/journal_with_most_drugs.json", orient="records", date_format="iso")
