from pyspark import Row
from pyspark.sql import SparkSession, DataFrame, Window, functions
from argparse import ArgumentParser
import re


# parse arguments
parser = ArgumentParser()
parser.add_argument('--input-file', help='Input path, path to the file containing trailing commas')
parser.add_argument('--output-path', help='Output path, the valid json file will be store in this path')
args = parser.parse_args()
spark = SparkSession.builder.getOrCreate()


def remove_trailing_comma(record: Row) -> Row:
    """Remove trailing commas from an RDD Row
    Args:
      record (Row): the line to fix
    Returns:
      :`Row`: fixed line
    """
    line = "" if record.line is None else record.line.value
    next_line = "" if record.next_line is None else record.next_line.value
    if bool(re.search(r"}\s*,\s*]", line)):
        fixed = re.sub(r"}\s*,\s*]", "}]", line)
        return Row(line=Row(value=fixed), id=record.id, next_line=record.next_line)
    elif line.strip().endswith("},") and next_line.strip().startswith("]"):
        return Row(line=Row(value=line.replace("},", "}")), id=record.id, next_line=record.next_line)
    else:
        return record



index_window = Window.partitionBy().orderBy("id")
columns = ["line", "id"]

input_data_as_text = spark \
    .read \
    .option("multiline", "true") \
    .text(args.input_file)

input_data_with_next_line = input_data_as_text \
    .rdd \
    .zipWithIndex() \
    .toDF(columns) \
    .withColumn("next_line", functions.lead("line").over(index_window))

input_data_without_trailing_comma = input_data_with_next_line \
    .rdd \
    .map(remove_trailing_comma) \
    .toDF() \
    .select("line.value") \


input_data_without_trailing_comma\
    .write\
    .text(args.output_path)




