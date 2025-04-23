from setuptools import setup, find_packages

setup(
    name='zmode_python_elastic',
    version='0.1.0',
    py_modules=['znodeelastic'],
    install_requires=["elasticsearch", "pandas", "numpy"],
    author='Your Name',
    description='Package for SQL Server external script',
)
