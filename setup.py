from setuptools import setup, find_packages

install_requires = [
    "pandas",
    "pyarrow",
]

config = {
    "description": "LSARP Data Management",
    "author": "Soren Wacker",
    "url": "https://github.com/soerendip",
    "download_url": "https://github.com/soerendip/LSARP",
    "author_email": "swacker@ucalgary.ca",
    "version": "0.0.2",
    "install_requires": install_requires,
    "packages": find_packages(),
    "scripts": [],
    "name": "LSARP",
}

setup(**config)
