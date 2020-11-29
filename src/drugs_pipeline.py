# Build drugs references from pubmed and clinical trials

from argparse import ArgumentParser

from pyspark.sql import SparkSession, functions

from src.utils.spark_utils import format_dates

# parse arguments
parser = ArgumentParser()
parser.add_argument('--pubmed-csv-path', help='Pubmed csv path, the path to pubmed csv file')
parser.add_argument('--pubmed-json-path', help='Pubmed json path, the path to pubmed json file')
parser.add_argument('--clinical-trials-csv-path', help='Clinical trials csv path, the path to clinical trials csv file')
parser.add_argument('--drugs-csv-path', help='Drugs csv path, the path to drugs csv file')
parser.add_argument('--drugs-references-path', help='Drugs reference path, the path to drugs computed reference')
parser.add_argument('--unique-file', help='Unique file ?, Write the final result to a unique file or distributed mode')

args = parser.parse_args()

# initialization
spark = SparkSession.builder.getOrCreate()
pubmed_columns = ["id", "title", "date", "journal"]

# LOAD SOURCES
pubmed = spark \
    .read.options(header=True) \
    .csv(args.pubmed_csv_path) \
    .union(spark
           .read
           .options(header=True, multiline=True)
           .json(args.pubmed_json_path)[pubmed_columns]
           )

clinical_trials = spark \
    .read.options(header=True) \
    .csv(args.clinical_trials_csv_path)

drugs = spark \
    .read.options(header=True) \
    .csv(args.drugs_csv_path)

# ENRICH DATA

# References of drugs in pubmed
drugs_with_pubmed = drugs \
    .join(pubmed, functions.lower(pubmed.title)
          .contains(functions.lower(drugs.drug)), "left")

# References of drugs in clinical trials
drugs_with_clinical_trials = drugs \
    .join(clinical_trials, functions.lower(clinical_trials.scientific_title)
          .contains(functions.lower(drugs.drug)), "left")

# Add new column type (pubmed|clinical trials), set same schema for references
drugs_with_pubmed_type = drugs_with_pubmed \
    .withColumn("type", functions.lit("pubmed"))
drugs_with_clinical_trials_type = drugs_with_clinical_trials \
    .withColumn("type", functions.lit("clinical trials")) \
    .withColumnRenamed("scientific_title", "title")

# Addition all references  : drugs with pubmed and journals, drugs with clinical trials and journals
drugs_with_pubmed_and_clinical = drugs_with_pubmed_type \
    .union(drugs_with_clinical_trials_type)

# Format dates
drugs_with_references = drugs_with_pubmed_and_clinical \
    .withColumn("reference_date", format_dates("date"))\
    .drop("date")

# Write to sink
if args.unique_file:
    drugs_with_references \
        .toPandas() \
        .to_json(f"{args.drugs_references_path}/drugs_references.json", orient="records", date_format="iso")
else:
    drugs_with_references \
        .write \
        .json(args.drugs_references_path)


