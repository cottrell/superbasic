from setuptools import setup

setup(name='superbasic',
      version='0.1',
      description='A simple dict_of_things store backend for joblib that supports pandas->parquet and json jobs without using pickle. No tests.',
      packages=['superbasic'],
      zip_safe=False)
