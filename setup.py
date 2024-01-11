from setuptools import setup, find_packages
import os


with open("README.md", "r", encoding="utf8") as fh:
    long_description = fh.read()


def get_version() -> str:
    version = {}
    setup_dir = os.path.dirname(os.path.abspath(__file__))
    init_path = os.path.join(setup_dir, 'src', 'marqo', '__init__.py')
    with open(init_path) as fp:
        exec(fp.read(), version)
    return version["__version__"]

setup(
    install_requires=[
        # client:
        "requests",
        "urllib3",
        "pydantic<2.0.0",
        "typing-extensions>=4.5.0",
        "packaging"
    ],
    tests_require=[
        "pytest",
        "tox"
    ],
    name="marqo1",
    version=get_version(),
    author="marqo org",
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
