from setuptools import setup

NAME = "feast-custom-stores-tikv"
REQUIRES_PYTHON = ">=3.7.0"

setup(
    name="feast_custom_stores_tikv",
    description=open("README.md").read(),
    version="0.0.1",
    long_description_content_type="text/markdown",
    python_requires=REQUIRES_PYTHON,
    install_requires=[
        "tikv-client",
        "feast==0.12.1"
    ],
    license="Apache",
)
