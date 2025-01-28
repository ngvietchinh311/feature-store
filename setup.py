from setuptools import setup, find_packages

setup(
    name="fs_kit",
    version="0.1.0",
    description="A package for managing features.",
    author="Chinh Nguyen",
    author_email="ngvietchinh311@gmail.com",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.2.2",  # Thêm các phụ thuộc cần thiết
    ],
    python_requires=">=3.7",
)
