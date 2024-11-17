import luigi
import os
import requests


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


if __name__ == "__main__":
    luigi.run()
