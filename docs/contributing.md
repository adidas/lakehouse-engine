# How to Contribute

📖 Search algorithms, transformations and check implementation details & examples in our [documentation](https://adidas.github.io/lakehouse-engine-docs/lakehouse_engine.html).

💭 In case you have doubts, ideas, want to ask for help or want to discuss different approach and usages, feel free to create a [discussion](https://github.com/adidas/lakehouse-engine/discussions).

⚠️ Are you facing any issues? Open an issue on [GitHub](https://github.com/adidas/lakehouse-engine/issues).

💡 Do you have ideas for new features? Open a feature request on [GitHub](https://github.com/adidas/lakehouse-engine/issues).

🚀 Want to find the available releases? Check our release notes on [GitHub](https://github.com/adidas/lakehouse-engine/releases) and [PyPi](https://pypi.org/project/lakehouse-engine/).

## Prerequisites

1. Git.
2. Your IDE of choice with a Python 3 environment (e.g., virtualenv created from the requirements_cicd.txt file).
3. Docker.
4. GNU make.

## Reporting possible bugs and issues

It is natural that you might find something that Esmerald should support or even experience some sorte of unexpected
behaviour that needs addressing.

The way we love doing things is very simple, contributions should start out with a
[discussion](https://github.com/adidas/lakehouse-engine/discussions).

We can then decide if the discussion needs to be escalated into an "Issue" or not.

When reporting something you should always try to:

* Be as more descriptive as possible
* Provide as much evidence as you can, something like:
    * OS platform
    * Python version
    * Installed dependencies
    * Code snippets
    * Tracebacks

Avoid putting examples extremely complex to understand and read. Simplify the examples as much as possible to make
it clear to understand and get the required help.

## Development

There are a few steps that allow a clean contribution to the project, for instance, to contribute to the documentation
you must have installed the necessary requirements such as `mkdocs-material` and for that reason, the project uses
the `make requirements-local` that will make sure every single requirement is installed in your local development.

!!! Tip
    It is strongly advised to have these installed in a isolated virtual environment.

To develop for Lakehouse Engine, create a fork of the [Lakehouse Engine](https://github.com/adidas/lakehouse-engine) repository on GitHub.

After, clone your fork with the follow command replacing `YOUR-USERNAME` with your GitHub username:

```shell
$ git clone https://github.com/YOUR-USERNAME/lakehouse-engine
```

1. Create your feature branch following the convention [feature|bugfix]/ISSUE_ID_short_name.
2. Apply your changes in the recently created branch. It is **mandatory** to add tests covering the feature of fix contributed.

### Install the project dependencies

```shell
cd lakehouse-engine
scripts/install
```

### Enable pre-commit

Beforehand it is better to install this seperately.

```shell
$ pip install pre-commit
```

The project comes with a pre-commit hook configuration. To enable it, just run inside the clone:

```shell
$ pre-commit install
$ pre-commit
```

This will install all the necessary hooks for the project.

### Run the style

```shell
scripts/style
```

### Run the linting

```shell
scripts/lint
```

### Run the tests

```shell
scripts/test
```

Also it is advised to run the `test-security` tests.

```shell
scripts/test-security
```

### Run the docs

To serve the documentation locally, simply run.

```
scripts/docs
```

## General steps for contributing

!!! Tip
    To use the make targets with another docker-compatible cli other than docker you can pass the parameter "container_cli".
    Example: `make test container_cli=nerdctl`

!!! Tip
    Most make target commands are running on docker. If you face any problem, you can also check the code of the respective
    make targets and directly execute the code in your python virtual environment.

1. (optional) You can build the wheel locally with `make build` or `make build os_deployment=True` (in case the wheel targets an environment, which does not have the dependencies listed in [extra_os_requirements.txt](https://github.com/adidas/lakehouse-engine/blob/master/cicd/extra_os_requirements.txt) pre-installed).
2. (optional) Install the wheel you have just generated and test it.
3. If you have changed or added new requirements, you should run `make build-lock-files`, to rebuild the lock files.
4. If the transitive dependencies have not been updated for a while, and you want to upgrade them, you can use `make upgrade-lock-files` to update them.
This will update the transitive dependencies even if you have not changed the requirements.
1.  When you're ready with your changes, open a Pull Request (PR) to develop.
2.  Ping the team through the preferred communication channel.
3.  The team will come together to review it and approve it (2 approvals required).
4.  Your changes will be tested internally, promoted to master and included in the next release.

## 🚀 Join us 🚀

**Pull Requests are welcome from anyone**. However, before opening one, please make sure to open an issue on [GitHub](https://github.com/adidas/lakehouse-engine/issues) and link it.

Moreover, if the Pull Request intends to cover big changes or features, it is recommended to first discuss it on a [GitHub issue](https://github.com/adidas/lakehouse-engine/issues) or [Discussion](https://github.com/adidas/lakehouse-engine/discussions).
