from setuptools import setup, find_packages

setup(
    name='my_script_package',
    version='0.1.0',
    packages=find_packages(),
    install_requires=["elasticsearch", "pandas", "numpy", "json"],
    author='Your Name',
    description='Package for SQL Server external script',
)
