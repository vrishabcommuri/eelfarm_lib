"""
Setuptools bootstrap module:
https://setuptools.readthedocs.io

Writing setup.py:
https://setuptools.readthedocs.io/en/latest/setuptools.html#basic-use

Also requires Prosody-Lab Aligner:
https://github.com/prosodylab/Prosodylab-Aligner
"""
from pathlib import Path
import re
from setuptools import setup


with open(Path(__file__).parent / 'eelfarm/_version.py', 'r') as fid:
    version = re.match(r"version = '(\d+\w*)'", fid.readline()).group(1)

setup(
    name='eelfarm',
    version=version,
    description="Distributed processing for Eelbrain",
    author="Christian Brodbeck",
    author_email='brodbeck@umd.edu',
    python_requires='>=3.8',
    install_requires=[],
    packages=(
        'eelfarm',
    ),
    entry_points={
        'console_scripts': (
            'eelfarm = eelfarm._main:main',
        ),
    },
)
