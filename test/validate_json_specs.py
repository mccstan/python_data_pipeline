import os
import shutil
import subprocess
import unittest

from pyspark.sql import SparkSession


class ValidateJsonSpecs(unittest.TestCase):
    INPUT_FILE = "/tmp/input_file"
    OUTPUT_FILE = "/tmp/output_file"

    def test_should_output_validate_json(self):
        self.generate_input()
        exit_code = self.run_app()
        self.assertEqual(exit_code, 0)

        valid_json = self.spark \
            .read \
            .option("multiline", "true") \
            .json(f'{self.OUTPUT_FILE}/part-*')

        expected_columns = ['date', 'id', 'journal', 'title']
        self.assertEqual(valid_json.columns.sort(), expected_columns.sort())

    def generate_input(self):
        content = """[ { "id": "", "title": "Comparison of pressure BETAMETHASONE release, phonophoresis and dry needling in treatment of latent myofascial trigger point of upper trapezius ATROPINE muscle.", 
        "date": "01/03/2020", "journal": "The journal of maternal-fetal & neonatal medicine" }, ] """
        with open(f'{self.INPUT_FILE}/sample.json', 'a+') as file:
            file.write(content)

    def run_app(self):
        return subprocess.run(
            [
                "spark-submit",
                "--conf", "spark.sql.shuffle.partitions=1",
                "../src/utils/validate_json.py",
                "--input-file", self.INPUT_FILE,
                "--output-path", self.OUTPUT_FILE
            ],
            stderr=subprocess.DEVNULL,
        ).returncode

    def setUp(self):
        for path in [self.INPUT_FILE, self.OUTPUT_FILE]:
            shutil.rmtree(path, True)
        os.makedirs(self.INPUT_FILE)

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


if __name__ == '__main__':
    unittest.main()
