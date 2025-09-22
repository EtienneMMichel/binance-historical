import requests, zipfile, io
import shutil

def download_zip(url, saving_dir):
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(saving_dir)

def delete_temp_data(path):
    shutil.rmtree(path)