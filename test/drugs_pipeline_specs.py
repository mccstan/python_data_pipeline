import shutil
import subprocess
import unittest
from pathlib import Path

from pyspark.sql import SparkSession


class DrugsPipelineSpecs(unittest.TestCase):
    PUBMED_CSV_PATH = "../data/raw/pubmed.csv"
    PUBMED_JSON_PATH = "../data/raw/pubmed.json"
    PUBMED_VALIDATED_JSON_PATH = "../data/raw/validated"
    CLINICAL_TRIALS_CSV_PATH = "../data/raw/clinical_trials.csv"
    DRUGS_CSV_PATH = "../data/raw/drugs.csv"
    DRUGS_REFERENCES_PATH = "../data/gold"
    UNIQUE_FILE = "True"


    def test_should_compute_drugs_references(self):
        validate_json_code = self.run_validate_json()
        drugs_pipeline = self.run_drugs_pipeline()

        self.assertEqual(validate_json_code, 0)
        self.assertEqual(drugs_pipeline, 0)

    def run_drugs_pipeline(self):
        return subprocess.run(
            [
                "spark-submit",
                "--conf", "spark.sql.shuffle.partitions=1",
                "../src/drugs_pipeline.py",
                "--pubmed-csv-path", self.PUBMED_CSV_PATH,
                "--pubmed-json-path", f"{self.PUBMED_VALIDATED_JSON_PATH}/part-*",
                "--clinical-trials-csv-path", self.CLINICAL_TRIALS_CSV_PATH,
                "--drugs-csv-path", self.DRUGS_CSV_PATH,
                "--drugs-references-path", self.DRUGS_REFERENCES_PATH,
                "--unique-file", self.UNIQUE_FILE
            ],
            stderr=subprocess.DEVNULL,
        ).returncode

    def run_validate_json(self):
        return subprocess.run(
            [
                "spark-submit",
                "--conf", "spark.sql.shuffle.partitions=1",
                "../src/utils/validate_json.py",
                "--input-file", self.PUBMED_JSON_PATH,
                "--output-path", self.PUBMED_VALIDATED_JSON_PATH
            ],
            stderr=subprocess.DEVNULL,
        ).returncode

    def setUp(self):
        shutil.rmtree(self.PUBMED_VALIDATED_JSON_PATH, True)
        for path in [self.PUBMED_VALIDATED_JSON_PATH, self.DRUGS_REFERENCES_PATH]:
            [f.unlink() for f in Path(path).glob("*") if f.is_file()]

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


if __name__ == '__main__':
    unittest.main()
