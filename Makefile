SHELL := /bin/bash -euxo pipefail

container_cli := docker
image_name := lakehouse-engine
deploy_env := dev
project_version := $(shell cat cicd/.bumpversion.cfg | grep "current_version =" | cut -f 3 -d " ")
version := $(project_version)
# Gets system information in upper case
system_information := $(shell uname -mvp | tr a-z A-Z)
meta_conf_file := cicd/meta.yaml
meta_os_conf_file := cicd/meta_os.yaml
engine_conf_file := lakehouse_engine/configs/engine.yaml
engine_os_conf_file := lakehouse_engine/configs/engine_os.yaml
remove_files_from_os := $(engine_conf_file) $(meta_conf_file) CODEOWNERS sonar-project.properties CONTRIBUTING.md CHANGELOG.md assets/img/os_strategy.png
last_commit_msg := "$(shell git log -1 --pretty=%B)"
git_tag := $(shell git describe --tags --abbrev=0)
commits_url := $(shell cat $(meta_conf_file) | grep commits_url | cut -f 2 -d " ")

ifneq ($(project_version), $(version))
wheel_version := $(project_version)+$(subst _,.,$(version))
project_name := lakehouse_engine_experimental
else
wheel_version := $(version)
project_name := lakehouse_engine
endif

# Condition to define the Python image to be built based on the machine CPU architecture.
# The base Python image only changes if the identified CPU architecture is ARM.
ifneq (,$(findstring ARM,$(system_information)))
python_image := $(shell cat $(meta_conf_file) | grep arm_python_image | cut -f 2 -d " ")
cpu_architecture := arm64
else
python_image := $(shell cat $(meta_conf_file) | grep amd_python_image | cut -f 2 -d " ")
cpu_architecture := amd64
endif

# Condition to define the spark driver memory limit to be used in the tests
# In order to change this limit you can use the spark_driver_memory parameter
# Example: make test spark_driver_memory=3g
#
# WARNING: When the tests are being run 2 spark nodes are created, so despite
# the default value being 2g, your configured docker environment should have
# extra memory for communication and overhead.
ifndef $(spark_driver_memory)
	spark_driver_memory := "2g"
endif

# A requirements_full.lock file is created based on all the requirements of the project (core, dq, os, azure, sftp and cicd).
# The requirements_full.lock file is then used as a constraints file to build the other lock files so that we ensure dependencies are consistent and compatible
# with each other, otherwise, the the installations would likely fail.
# Moreover, the requirement_full.lock file is also used in the dockerfile to install all project dependencies.
full_requirements := -o requirements_full.lock requirements.txt requirements_os.txt requirements_dq.txt requirements_azure.txt requirements_sftp.txt requirements_cicd.txt
requirements := -o requirements.lock requirements.txt -c requirements_full.lock
os_requirements := -o requirements_os.lock requirements_os.txt -c requirements_full.lock
dq_requirements = -o requirements_dq.lock requirements_dq.txt -c requirements_full.lock
azure_requirements = -o requirements_azure.lock requirements_azure.txt -c requirements_full.lock
sftp_requirements = -o requirements_sftp.lock requirements_sftp.txt -c requirements_full.lock
os_deployment := False
container_user_dir := /home/appuser
trust_git_host := ssh -oStrictHostKeyChecking=no -i $(container_user_dir)/.ssh/id_rsa git@github.com
ifeq ($(os_deployment), True)
build_src_dir := tmp_os/lakehouse-engine
else
build_src_dir := .
endif

build-image:
	$(container_cli) build \
		--build-arg USER_ID=$(shell id -u ${USER}) \
		--build-arg GROUP_ID=$(shell id -g ${USER}) \
		--build-arg PYTHON_IMAGE=$(python_image) \
		--build-arg CPU_ARCHITECTURE=$(cpu_architecture) \
		-t $(image_name):$(version) . -f cicd/Dockerfile

build-image-windows:
	$(container_cli) build \
		--build-arg PYTHON_IMAGE=$(python_image) \
        --build-arg CPU_ARCHITECTURE=$(cpu_architecture) \
        -t $(image_name):$(version) . -f cicd/Dockerfile

# The build target is used to build the wheel package.
# It makes usage of some `perl` commands to change the project name and the wheel version in the pyproject.toml file,
# whenever the goal is to build a `project_name` for distribution and testing, instead of an official release.
# Ex: if you run 'make build-image version=feature-x-1276, and the current project version is 1.20.0, the generated wheel will be: lakehouse_engine_experimental-1.20.0+feature.x.1276-py3-none-any,
# while for the official 1.20.0 release, the wheel will be: lakehouse_engine-1.20.0-py3-none-any.
build:
	perl -pi -e 's/version = "$(project_version)"/version = "$(wheel_version)"/g' pyproject.toml && \
	perl -pi -e 's/name = "lakehouse-engine"/name = "$(project_name)"/g' pyproject.toml && \
	$(container_cli) run --rm \
		-w /app \
		-v "$$PWD":/app \
		$(image_name):$(version) \
		/bin/bash -c 'python -m build --wheel $(build_src_dir)' && \
	perl -pi -e 's/version = "$(wheel_version)"/version = "$(project_version)"/g' pyproject.toml && \
	perl -pi -e 's/name = "$(project_name)"/name = "lakehouse-engine"/g' pyproject.toml

deploy: build
	$(container_cli) run --rm \
		-w /app \
		-v "$$PWD":/app \
		-v $(artifactory_credentials_file):$(container_user_dir)/.pypirc \
		$(image_name):$(version) \
		/bin/bash -c 'twine upload -r artifactory dist/$(project_name)-$(wheel_version)-py3-none-any.whl --skip-existing'

docs:
	$(container_cli) run --rm \
		-w /app \
		-v "$$PWD":/app \
		$(image_name):$(version) \
		/bin/bash -c 'cd $(build_src_dir) && python ./cicd/code_doc/render_doc.py'

# mypy incremental mode is used by default, so in case there is any cache related issue,
# you can modify the command to include --no-incremental flag or you can delete mypy_cache folder.
lint:
	$(container_cli) run --rm \
		-w /app \
        -v "$$PWD":/app \
		$(image_name):$(version) \
		/bin/bash -c 'flake8 --docstring-convention google --config=cicd/flake8.conf lakehouse_engine tests cicd/code_doc/render_doc.py \
		&& mypy lakehouse_engine tests'

# useful to print and use make variables. Usage: make print-variable var=variable_to_print.
print-variable:
	@echo $($(var))

style:
	$(container_cli) run --rm \
		-w /app \
		-v "$$PWD":/app \
		$(image_name):$(version) \
		/bin/bash -c '''isort lakehouse_engine tests cicd/code_doc/render_doc.py && \
        black lakehouse_engine tests cicd/code_doc/render_doc.py'''

terminal:
	$(container_cli) run \
		-it \
		--rm \
	  	-w /app \
		-v "$$PWD":/app \
		$(image_name):$(version) \
		/bin/bash

# Can use test only: ```make test test_only="tests/feature/test_delta_load_record_mode_cdc.py"```.
# You can also hack it by doing ```make test test_only="-rx tests/feature/test_delta_load_record_mode_cdc.py"```
# to show complete output even of passed tests.
# We also fix the coverage filepaths, using perl, so that report has the correct paths
test:
	$(container_cli) run \
		--rm \
		-w /app \
        -v "$$PWD":/app \
		$(image_name):$(version) \
		/bin/bash -c "pytest \
            --junitxml=artefacts/tests.xml \
            --cov-report xml --cov-report xml:artefacts/coverage.xml \
            --cov-report term-missing --cov=lakehouse_engine \
            --log-cli-level=INFO --color=yes -x -v \
            --spark_driver_memory=$(spark_driver_memory) $(test_only)" && \
	perl -pi -e 's/filename=\"/filename=\"lakehouse_engine\//g' artefacts/coverage.xml

test-security:
	$(container_cli) run \
		--rm \
		-w /app \
        -v "$$PWD":/app \
		$(image_name):$(version) \
		/bin/bash -c 'bandit -c cicd/bandit.yaml -r lakehouse_engine tests'

#####################################
##### Dependency Management Targets #####
#####################################

audit-dep-safety:
	$(container_cli) run --rm \
		-w /app \
        -v "$$PWD":/app \
		$(image_name):$(version) \
		/bin/bash -c 'pip-audit -r cicd/requirements_full.lock --desc on -f json --fix --dry-run -o artefacts/safety_analysis.json'

# This target will build the lock files to be used for building the wheel and delivering it.
build-lock-files:
	$(container_cli) run --rm \
	    -w /app \
	    -v "$$PWD":/app \
	    $(image_name):$(version) \
	    /bin/bash -c 'cd cicd && pip-compile --resolver=backtracking $(full_requirements) && \
	    pip-compile --resolver=backtracking $(requirements) && \
	    pip-compile --resolver=backtracking $(os_requirements) && \
	    pip-compile --resolver=backtracking $(dq_requirements) && \
		pip-compile --resolver=backtracking $(azure_requirements) && \
		pip-compile --resolver=backtracking $(sftp_requirements)'

# We test the dependencies to check if they need to be updated because requirements.txt files have changed.
# On top of that, we also test if we will be able to install the base and the extra packages together, 
# as their lock files are built separately and therefore dependency constraints might be too restricted. 
# If that happens, pip install will fail because it cannot solve the dependency resolution process, and therefore
# we need to pin those conflict dependencies in the requirements.txt files to a version that fits both the base and 
# extra packages.
test-deps:
	@GIT_STATUS="$$(git status --porcelain --ignore-submodules cicd/)"; \
	if [ ! "x$$GIT_STATUS" = "x"  ]; then \
	    echo "!!! Requirements lists has been updated but lock file was not rebuilt !!!"; \
	    echo "!!! Run make build-lock-files !!!"; \
	    echo -e "$${GIT_STATUS}"; \
	    git diff cicd/; \
	    exit 1; \
	fi
	$(container_cli) run --rm \
		-w /app \
        -v "$$PWD":/app \
		$(image_name):$(version) \
		/bin/bash -c 'pip install -e .[azure,dq,sftp,os] --dry-run --ignore-installed'

# This will update the transitive dependencies even if there were no changes in the requirements files.
# This should be a recurrent activity to make sure transitive dependencies are kept up to date.
upgrade-lock-files:
	$(container_cli) run --rm \
	    -w /app \
	    -v "$$PWD":/app \
	    $(image_name):$(version) \
	    /bin/bash -c 'cd cicd && pip-compile --resolver=backtracking --upgrade $(cicd_requirements) && \
	    pip-compile --resolver=backtracking --upgrade $(requirements) && \
	    pip-compile --resolver=backtracking --upgrade $(os_requirements) && \
	    pip-compile --resolver=backtracking --upgrade $(dq_requirements) && \
		pip-compile --resolver=backtracking --upgrade $(azure_requirements) && \
		pip-compile --resolver=backtracking --upgrade $(sftp_requirements)'

#####################################
##### GitHub Deployment Targets #####
#####################################

prepare-github-repo:
	$(container_cli) run --rm \
		-w /app \
		-v "$$PWD":/app \
		-v $(git_credentials_file):$(container_user_dir)/.ssh/id_rsa \
		$(image_name):$(version) \
		/bin/bash -c """mkdir -p tmp_os/$(repository); \
		cd tmp_os/$(repository); \
		git init -b master; \
		git config pull.rebase false; \
		git config user.email 'lak-engine@adidas.com'; \
		git config user.name 'Lakehouse Engine'; \
		$(trust_git_host); \
		git remote add origin git@github.com:adidas/$(repository).git; \
		git pull origin master --tags"""

sync-to-github: prepare-github-repo
	$(container_cli) run --rm \
		-w /app \
		-v "$$PWD":/app \
		-v $(git_credentials_file):$(container_user_dir)/.ssh/id_rsa \
		$(image_name):$(version) \
		/bin/bash -c """cd tmp_os/lakehouse-engine; \
		rsync -r --exclude=.git --exclude=.*cache* --exclude=venv --exclude=dist --exclude=tmp_os /app/ . ; \
		rm $(remove_files_from_os); \
		mv $(engine_os_conf_file) $(engine_conf_file); \
		mv $(meta_os_conf_file) $(meta_conf_file); \
		mv CONTRIBUTING_OS.md CONTRIBUTING.md; \
		$(trust_git_host); \
		git add . ; \
		git commit -m "'${last_commit_msg}'"; \
		git tag -a $(git_tag) -m 'Release $(git_tag)' ; \
		git push origin master --follow-tags;"""

deploy-docs-to-github: docs prepare-github-repo
	$(container_cli) run --rm \
		-w /app \
		-v "$$PWD":/app \
		-v $(git_credentials_file):$(container_user_dir)/.ssh/id_rsa \
		$(image_name):$(version) \
		/bin/bash -c """cp -r tmp_os/lakehouse-engine/artefacts/docs/* tmp_os/lakehouse-engine-docs/ ; \
		cd tmp_os/lakehouse-engine-docs; \
		$(trust_git_host); \
		git add . ; \
		git commit -m 'Lakehouse Engine $(version) documentation'; \
		git push origin master ; \
		cd .. && rm -rf tmp_os/lakehouse-engine-docs"""

deploy-to-pypi: build
	$(container_cli) run --rm \
		-w /app \
		-v "$$PWD":/app \
		-v $(pypi_credentials_file):$(container_user_dir)/.pypirc \
		$(image_name):$(version) \
		/bin/bash -c 'twine upload tmp_os/lakehouse-engine/dist/lakehouse_engine-$(project_version)-py3-none-any.whl --skip-existing'

deploy-to-pypi-and-clean: deploy-to-pypi
	$(container_cli) run --rm \
		-w /app \
		-v "$$PWD":/app \
		$(image_name):$(version) \
		/bin/bash -c 'rm -rf tmp_os/lakehouse-engine'

###########################
##### Release Targets #####
###########################
create-changelog:
	echo "# Changelog - $(shell date +"%Y-%m-%d") v$(version)" > CHANGELOG.md && \
	echo "All notable changes to this project will be documented in this file automatically. This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)." >> CHANGELOG.md && \
	echo "" >> CHANGELOG.md && \
	git log --no-decorate --pretty=format:"#### [%cs] [%(describe)]%n [%h]($(commits_url)%H) %s" -n 1000 >> CHANGELOG.md

bump-up-version: create-changelog
	$(container_cli) run --rm \
		-w /app \
		-v "$$PWD":/app \
		$(image_name):$(version) \
		/bin/bash -c 'bump2version --config-file cicd/.bumpversion.cfg $(increment)'

prepare-release: bump-up-version
	echo "Prepared version and changelog to release!"

commit-release:
	git commit -a -m 'Create release $(version)' && \
    git tag -a 'v$(version)' -m 'Release $(version)'

push-release:
	git push --follow-tags

delete-tag:
	git push --delete origin $(tag)

.PHONY: $(MAKECMDGOALS)
