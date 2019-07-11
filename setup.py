from setuptools import setup, find_packages

setup(
    name='newln2sql',
    version='0.2',
    url='https://github.com/harsh97mah/newln2sql',
    license='GNU',
    author='Harshawardhan Mahajan',
    author_email='humahajanrum007@gmail.com',
    description='Convert Natural Language to SQL queries',
    packages=find_packages(exclude=['tests']),
    long_description=open('README.md').read(),
    setup_requires=['pytest'],
)
