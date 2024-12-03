from setuptools import setup

from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name='pyrsqrd',
    version='0.0.1',
    install_requires=[
    ],
	include_package_data=True,
	long_description=long_description,
    long_description_content_type='text/markdown',
)


