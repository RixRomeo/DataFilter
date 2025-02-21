from setuptools import setup, find_packages

setup(
    name="data_filter_lib",
    version="1.0.0",
    packages=find_packages(),
    install_requires=["pymongo"],
    description="Libreria per il filtraggio dei dati da MongoDB",
    author="Riccardo Romeo",
    url=""
)
