# Remove trailing comma from a json.

from argparse import ArgumentParser
from pyspark.sql import SparkSession
from src.utils.spark_utils import json_without_trailing_comma

# parse arguments
parser = ArgumentParser()
parser.add_argument('--input-file', help='Input path, path to the file containing trailing commas')
parser.add_argument('--output-path', help='Output path, the valid json file will be store in this path')
args = parser.parse_args()

# initialization
spark = SparkSession.builder.getOrCreate()

input_data_as_text = spark \
    .read \
    .option("multiline", "true") \
    .text(args.input_file)

input_data_without_trailing_comma = json_without_trailing_comma(input_data_as_text)

input_data_without_trailing_comma \
    .write \
    .text(args.output_path)
