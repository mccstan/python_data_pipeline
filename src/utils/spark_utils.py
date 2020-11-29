import re

from pyspark import Row
from pyspark.sql import Column, functions, DataFrame, Window


def format_dates(col: str, formats=("dd/MM/yyyy",
                                    "dd/MM/yyyy",
                                    "yyyy-MM-dd",
                                    "yyyy/MM/dd",
                                    "MM/dd/yyyy",
                                    "MM/dd/yyyy",
                                    "d MMMM yyyy",
                                    "dd MMMM yyyy")) -> Column:
    """ Formats dates from a column containing multiple formats
    Example of use : dataframe.select(to_date_("date").alias("formatted_date"))
    Args:
      col (Any): the column to format
    Returns:
      :param col: column containing multiple dates
      :param formats: possible formats
    """
    return functions.coalesce(*[functions.to_date(col, f) for f in formats])


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


def json_without_trailing_comma(json_as_text: DataFrame) -> DataFrame:
    """Remove trailing commas from a dataframe representing a json file read as text.
    Args:
      json_as_text (DataFrame): the input json as dataframe of strings
    Returns:
      :DataFrame: a dataframe of strings representing a valid json without trailing commas
    """
    index_window = Window.partitionBy().orderBy("id")
    columns = ["line", "id"]

    dataframe_with_next_record = json_as_text \
        .rdd \
        .zipWithIndex() \
        .toDF(columns) \
        .withColumn("next_line", functions.lead("line").over(index_window))

    dataframe_with_valid_lines = dataframe_with_next_record \
        .rdd \
        .map(remove_trailing_comma) \
        .toDF() \
        .select("line.value")

    return dataframe_with_valid_lines
