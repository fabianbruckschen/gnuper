import setuptools

with open("README.rst", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="gnuper",
    version="0.0.3",
    author="Fabian Bruckschen",
    author_email="fabian@knuper.com",
    description="Open Source Package for Mobile Phone Metadata Preprocessing",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab.com/knuper/gnuper",
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=[
          'bandicoot==0.5.3',
          'glob2==0.5',
          'numpy',
          'numpydoc',
          'pandas==0.23.3',
          'py4j==0.10.7',
          'pyarrow',
          'pyspark==2.3.1',
          'tqdm'
      ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
