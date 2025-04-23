from setuptools import setup, find_packages

setup(
    name='my_script_package',
    version='0.1.0',
    py_modules=['znodeelastic'],
    install_requires=["elasticsearch", "pandas", "numpy", "json"],
    author='Your Name',
    description='Package for SQL Server external script',
)
