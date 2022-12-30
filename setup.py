from setuptools import setup, find_packages

setup(name='inzata_python_utils',
      version='0.8',
      description='InZata Python utility libraries',
      packages=find_packages(),
      install_requires=['streamsets>=4.3.0'],
      zip_safe=False)