import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="gnuper",
    version="0.0.2",
    author="Fabian Bruckschen",
    author_email="fabian@knuper.com",
    description="Open Source Package for Mobile Phone Metadata Preprocessing",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab.com/knuper/gnuper",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
