# How to Contribute

📖 Search algorithms, transformations and check implementation details & examples in our [documentation](https://adidas.github.io/lakehouse-engine-docs/lakehouse_engine.html).

💭 In case you have doubts, ideas, want to ask for help or want to discuss different approach and usages, feel free to create a [discussion](https://github.com/adidas/lakehouse-engine/discussions).

⚠️ Are you facing any issues? Open an issue on [GitHub](https://github.com/adidas/lakehouse-engine/issues).

💡 Do you have ideas for new features? Open a feature request on [GitHub](https://github.com/adidas/lakehouse-engine/issues).

🚀 Want to find the available releases? Check our release notes on [GitHub](https://github.com/adidas/lakehouse-engine/releases) and [PyPi](https://pypi.org/project/lakehouse-engine/).

## Prerequisites

1. Git.
2. Your IDE of choice with a Python 3 environment (e.g., virtualenv created from the requirements_cicd.txt file).
3. Docker. **Warning:** The default spark driver memory limit for the tests is set at 2g. This limit is configurable but your
   testing docker setup **MUST** always have **at least** 2 * spark driver memory limit + 1 gb configured.
4. GNU make.

## General steps for contributing
1. Fork the project.
2. Clone the forked project into your working environment.
3. Create your feature branch following the convention [feature|bugfix]/ISSUE_ID_short_name.
4. Apply your changes in the recently created branch. It is **mandatory** to add tests covering the feature of fix contributed.
5. Style, lint, test and test security:
    ```
    make style
    make lint
    make test
    make test-security
    ```
---
> ***Note:*** To use the make targets with another docker-compatible cli other than docker you can pass the parameter "container_cli". 
Example: `make test container_cli=nerdctl`

---

---
> ***Note:*** Most make target commands are running on docker. If you face any problem, you can also check the code of the respective make targets and directly execute the code in your python virtual environment.

---

6. (optional) You can build the wheel locally with `make build` or `make build os_deployment=True` (in case the wheel targets an environment, which does not have the dependencies listed in [extra_os_requirements.txt](cicd/extra_os_requirements.txt) pre-installed).
7. (optional) Install the wheel you have just generated and test it.
8. If you have changed or added new requirements, you should run `make build-lock-files`, to rebuild the lock files. 
9. If the transitive dependencies have not been updated for a while, and you want to upgrade them, you can use `make upgrade-lock-files` to update them. 
This will update the transitive dependencies even if you have not changed the requirements.
10. When you're ready with your changes, open a Pull Request (PR) to develop.
11. Ping the team through the preferred communication channel.
12. The team will come together to review it and approve it (2 approvals required).
13. Your changes will be tested internally, promoted to master and included in the next release.

> 🚀🚀🚀
>
> **Pull Requests are welcome from anyone**. However, before opening one, please make sure to open an issue on [GitHub](https://github.com/adidas/lakehouse-engine/issues)
> and link it.
> Moreover, if the Pull Request intends to cover big changes or features, it is recommended to first discuss it on a [GitHub issue](https://github.com/adidas/lakehouse-engine/issues) or [Discussion](https://github.com/adidas/lakehouse-engine/discussions).
>
> 🚀🚀🚀