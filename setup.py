from setuptools import setup, find_packages


version = {}
with open("src/marqo/__init__.py") as fp:
    exec(fp.read(), version)

with open("README.md", "r", encoding="utf8") as fh:
    long_description = fh.read()

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
    version=version['__version__'],
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
