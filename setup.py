import setuptools, os

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

if os.environ.get("os_deployment", "False") == "True":
    requirements_path = "cicd/requirements_os.lock"
else:
    requirements_path = "cicd/requirements.lock"

with open(requirements_path, "r") as requirements_file:
    requirements = requirements_file.readlines()
    requirements = [x[:-1] for x in requirements if "==" in x]

setuptools.setup(
    name="lakehouse-engine",
    version="1.19.0",
    author="Adidas Lakehouse Foundations Team",
    author_email="software.engineering@adidas.com",
    description="A Spark framework serving as the engine for several lakehouse "
                "algorithms and data flows.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://adidas.github.io/lakehouse-engine-docs/lakehouse_engine.html",
    project_urls={
        "Source Code": "https://adidas.github.io/lakehouse-engine",
        "Documentation": "https://adidas.github.io/lakehouse-engine-docs/lakehouse_engine.html",
        "Issues": "https://github.com/adidas/lakehouse-engine/issues"
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python :: 3",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Other Audience",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development",
        "License :: OSI Approved :: Apache Software License",
    ],
    packages=setuptools.find_packages(exclude="tests"),
    python_requires=">=3.9",
    install_requires=requirements,
    package_data={"lakehouse_engine": ["configs/engine.yaml"]},
    # Passing requirements list is required for building sdist only
    # and could be removed once sdist building is disabled
    data_files=[("requirements", [requirements_path])],
)
