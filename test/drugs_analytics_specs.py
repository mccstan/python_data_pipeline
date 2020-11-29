import os
import shutil
import subprocess
import unittest

from pyspark.sql import SparkSession


class ValidateJsonSpecs(unittest.TestCase):
    DRUGS_REFERENCES_PATH = "../data/gold/drugs_references.json"
    ANALYTICS_PATH = "../data/gold/"

    def test_should_compute_drugs_analytics(self):
        drugs_analytics_code = self.run_drugs_analytics()
        self.assertEqual(drugs_analytics_code, 0)

    def run_drugs_analytics(self):
        return subprocess.run(
            [
                "spark-submit",
                "--conf", "spark.sql.shuffle.partitions=1",
                "../src/drugs_analytics.py",
                "--drugs-references-path", self.DRUGS_REFERENCES_PATH,
                "--analytics-path", self.ANALYTICS_PATH
            ],
            stderr=subprocess.DEVNULL,
        ).returncode

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


if __name__ == '__main__':
    unittest.main()
