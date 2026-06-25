from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf8") as fh:
    long_description = fh.read()

setup(
    install_requires=[
        # client:
        "requests",
        "urllib3>=1.26.0",
        "pydantic>=2.0.0",
        "packaging"
    ],
    tests_require=[
        "tox"
    ],
    name="marqo",
    version="3.18.2",
    author="marqo org",
    author_email="org@marqo.io",
    description="AI-native ecommerce search platform with semantic search and personalization for fashion, beauty, electronics, and home goods.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(where="src", exclude=("tests*",)),
    keywords="search python marqo tensor neural hybrid semantic vector embedding ecommerce personalization merchandizing",
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
