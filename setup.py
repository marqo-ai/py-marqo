from setuptools import setup, find_packages
import os
# from src.marqo.version import __version__

with open("README.md", "r", encoding="utf8") as fh:
    long_description = fh.read()


def get_package_name() -> str:
    return os.getenv("MARQO_PACKAGE_NAME", "marqo1")


setup(
    install_requires=[
        # client:
        "requests",
        "urllib3<2.0.0, >=1.26.0",
        "pydantic<2.0.0",
        "typing-extensions>=4.5.0",
        "packaging"
    ],
    tests_require=[
        "pytest",
        "tox"
    ],
    author="marqo org",
    name=get_package_name(),
    author_email="org@marqo.io",
    description="Tensor search for humans",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(where="src", exclude=("tests*",)),
    keywords="search python marqo opensearch tensor neural semantic vector embedding",
    platform="any",
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    include_package_data=True,
    python_requires=">=3",
    package_dir={"": "src"},
)
