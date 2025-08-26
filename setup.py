"""
Setup script for DataProxy.
"""

from setuptools import setup, find_packages
import os

# Read README file
def read_readme():
    with open("README.md", "r", encoding="utf-8") as fh:
        return fh.read()

# Read requirements
def read_requirements():
    with open("requirements.txt", "r", encoding="utf-8") as fh:
        return [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="dataproxy",
    version="0.1.0",
    author="DataProxy Team",
    author_email="team@dataproxy.com",
    description="Database proxy with local write caching",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/dataproxy",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.8",
    install_requires=read_requirements(),
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov>=2.0",
            "black>=21.0",
            "flake8>=3.8",
            "mypy>=0.800",
        ],
    },
    entry_points={
        "console_scripts": [
            "dataproxy=dataproxy.main:cli",
        ],
    },
    include_package_data=True,
    zip_safe=False,
    keywords="database, proxy, mysql, mariadb, caching, development",
    project_urls={
        "Bug Reports": "https://github.com/yourusername/dataproxy/issues",
        "Source": "https://github.com/yourusername/dataproxy",
        "Documentation": "https://github.com/yourusername/dataproxy#readme",
    },
)
