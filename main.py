import luigi
import os
import requests
import tarfile
import gzip
import shutil


class DownloadDataset(luigi.Task):
    dataset_name = luigi.Parameter()
    output_dir = luigi.Parameter(default="data")

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, f"{self.dataset_name}_RAW.tar"))

    def run(self):
        url = f"https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.dataset_name}&format=file"
        os.makedirs(self.output_dir, exist_ok=True)
        response = requests.get(url, stream=True)
        with open(self.output().path, "wb") as f:
            f.write(response.content)


class ExtractArchive(luigi.Task):
    dataset_name = luigi.Parameter()
    output_dir = luigi.Parameter(default="data")

    def requires(self):
        return DownloadDataset(self.dataset_name, self.output_dir)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, self.dataset_name, "extracted"))

    def run(self):
        extracted_dir = self.output().path
        os.makedirs(extracted_dir, exist_ok=True)
        with tarfile.open(self.input().path, "r") as tar:
            tar.extractall(extracted_dir)
        for file in os.listdir(extracted_dir):
            if file.endswith(".gz"):
                filepath = os.path.join(extracted_dir, file)
                with gzip.open(filepath, "rb") as f_in:
                    with open(filepath[:-3], "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                os.remove(filepath)


if __name__ == "__main__":
    luigi.run()
