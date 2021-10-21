from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

setup(
    name='floorist',
    version='0.0.1',
    description='Helper for dumping SQL queries from a PostgreSQL database into S3 buckets in parquet format',
    url='https://github.com/RedHatInsights/floorist',
    author='Dávid Halász (@skateman)',
    author_email='',
    keywords='S3, Parquet, PostgreSQL',
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    python_requires='>=3.6, <4',
    install_require=[
        'app-common-python'
    ],
    extras_require={
        'test': ['pytest'],
    },
)
