import luigi
import os
import requests
import tarfile
import gzip
import shutil
import io
import pandas as pd


class DownloadDataset(luigi.Task):
    """
        Загрузка датасета из источника
    """
    dataset_name = luigi.Parameter()
    output_dir = luigi.Parameter(default="data")

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, f"{self.dataset_name}_RAW.tar"))

    def run(self):
        url = f"https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.dataset_name}&format=file"
        os.makedirs(self.output_dir, exist_ok=True)
        # Загружаем датасет
        response = requests.get(url, stream=True)

        # Сохраняем его в output_dir
        with open(self.output().path, "wb") as f:
            f.write(response.content)


class ExtractArchive(luigi.Task):
    """
        Подготовка загруженных данных для дальнейшей обработки
    """
    dataset_name = luigi.Parameter()
    output_dir = luigi.Parameter(default="data")

    def requires(self):
        # Указываем что эта задача зависит от скачивания архива
        return DownloadDataset(self.dataset_name, self.output_dir)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, self.dataset_name, "extracted"))

    def run(self):
        extracted_dir = self.output().path
        os.makedirs(extracted_dir, exist_ok=True)

        # Разархивация основного архива
        with tarfile.open(self.input().path, "r") as tar:
            tar.extractall(extracted_dir)

        # Распаковываем каждый .gz файл в этой папке
        for file in os.listdir(extracted_dir):
            if file.endswith(".gz"):
                filepath = os.path.join(extracted_dir, file)

                with gzip.open(filepath, "rb") as f_in:

                    with open(filepath[:-3], "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)

                # Удаляем оригинальный .gz файл после распаковки
                os.remove(filepath)


class ProcessTables(luigi.Task):
    """
        Обработка данных в удобное для нас представление
    """
    dataset_name = luigi.Parameter()
    output_dir = luigi.Parameter(default="data")

    def requires(self):
        # Указываем что эта задача зависит от распаковки архива
        return ExtractArchive(self.dataset_name, self.output_dir)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, self.dataset_name))

    def run(self):
        processed_dir = self.output().path
        os.makedirs(processed_dir, exist_ok=True)
        extracted_dir = self.input().path

        for file in os.listdir(extracted_dir):
            if file.endswith(".txt"):
                input_path = os.path.join(extracted_dir, file)
                dfs = {}
                with open(input_path, "r") as f:
                    write_key = None
                    fio = io.StringIO()
                    for line in f.readlines():
                        if line.startswith('['):
                            if write_key:
                                fio.seek(0)
                                header = None if write_key == 'Heading' else 'infer'
                                dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)
                            fio = io.StringIO()
                            write_key = line.strip('[]\n')
                            continue
                        if write_key:
                            fio.write(line)
                    if write_key:
                        fio.seek(0)
                        dfs[write_key] = pd.read_csv(fio, sep='\t')

                for key, df in dfs.items():
                    output_file = os.path.join(processed_dir, f"{key}.tsv")
                    df.to_csv(output_file, sep='\t', index=False)

                # Если среди таблиц есть "Probes", создаем её урезанную версию
                if "Probes" in dfs:
                    reduced_df = dfs["Probes"].drop(columns=[
                        "Definition", "Ontology_Component", "Ontology_Process",
                        "Ontology_Function", "Synonyms", "Obsolete_Probe_Id", "Probe_Sequence"
                    ], errors="ignore")
                    reduced_file = os.path.join(processed_dir, "Probes_reduced.tsv")
                    reduced_df.to_csv(reduced_file, sep='\t', index=False)

                # Удаляем оригинальный текстовый файл после обработки
                os.remove(input_path)


class FinalTask(luigi.Task):
    """
        Удаление промежуточных файлов
    """
    dataset_name = luigi.Parameter()
    output_dir = luigi.Parameter(default="data")

    def requires(self):
        # Указываем что эта задача зависит от обработки таблиц
        return ProcessTables(self.dataset_name, self.output_dir)

    def run(self):
        # Удаляем папку с промежуточными распакованными данными
        extracted_dir = os.path.join(self.output_dir, self.dataset_name, "extracted")
        if os.path.exists(extracted_dir):
            shutil.rmtree(extracted_dir)


if __name__ == "__main__":
    luigi.run()
