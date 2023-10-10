SHELL := /bin/bash -euxo pipefail

container_cli := docker
image_name := lakehouse-engine
deploy_env := dev
project_version := $(shell cat cicd/.bumpversion.cfg | grep current_version | cut -f 3 -d " ")
version := $(project_version)
latest_suffix := latest-feature
# Gets system information in upper case
system_information := $(shell uname -mvp | tr a-z A-Z)
meta_conf_file := cicd/meta.yaml
meta_os_conf_file := cicd/meta_os.yaml
engine_conf_file := lakehouse_engine/configs/engine.yaml
engine_os_conf_file := lakehouse_engine/configs/engine_os.yaml
remove_files_from_os := $(engine_conf_file) $(meta_conf_file) CODEOWNERS sonar-project.properties CONTRIBUTING.md CHANGELOG.md assets/img/os_strategy.png
last_commit_msg := "$(shell git log -1 --pretty=%B)"
git_tag := $(shell git describe --tags --abbrev=0)
commits_url := $(shell cat $(meta_conf_file) | grep commidddts_url | cut -f 2 -d " ")

ifeq ($(deploy_env), dev)
deploy_bucket := $(shell cat $(meta_conf_file) | grep dev_deploy_bucket | cut -f 2 -d " ")
else ifeq ($(deploy_env), prod)
deploy_bucket := $(shell cat $(meta_conf_file) | grep prod_deploy_bucket | cut -f 2 -d " ")
else
$(error Invalid deployment environment. It must be one of: dev or prod. Received: $(deploy_env))
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

version_deploy_path := $(deploy_bucket)/lakehouse-engine/lakehouse_engine-$(version)-py3-none-any.whl
latest_deploy_path := $(deploy_bucket)/lakehouse-engine/lakehouse_engine-$(latest_suffix)-py3-none-any.whl

requirements := cicd/requirements.txt
extra_requirements := cicd/extra_os_requirements.txt
cicd_requirements := cicd/requirements_cicd.txt cicd/requirements.lock $(extra_requirements)
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

build:
	$(container_cli) run --rm \
		-w /app \
		-v "$$PWD":/app \
		$(image_name):$(version) \
		/bin/bash -c 'export os_deployment=$(os_deployment); python -m build $(build_src_dir)'

deploy: build
	$(container_cli) run --rm \
		-w /app \
		-v "$$PWD":/app \
		-v $(aws_credentials_file):$(container_user_dir)/.aws/credentials:ro \
		$(image_name):$(version) \
		/bin/bash -c 'aws s3 --profile $(deploy_env) cp dist/lakehouse_engine-$(project_version)-py3-none-any.whl $(version_deploy_path) && \
        aws s3 --profile $(deploy_env) cp dist/lakehouse_engine-$(project_version)-py3-none-any.whl $(latest_deploy_path) && \
        rm dist/lakehouse_engine-$(project_version)-py3-none-any.whl'

test-deps: build-lock-files
	@GIT_STATUS="$$(git status --porcelain --ignore-submodules cicd/)"; \
	if [ ! "x$$GIT_STATUS" = "x"  ]; then \
	    echo "!!! Requirements lists has been updated but lock file was not rebuilt !!!"; \
	    echo "!!! Run `make build-lock-files` !!!"; \
	    echo -e "$${GIT_STATUS}"; \
	    git diff cicd/; \
	    exit 1; \
	fi

build-lock-files:
	$(container_cli) run --rm \
	    -w /app \
	    -v "$$PWD":/app \
	    $(image_name):$(version) \
	    /bin/bash -c 'pip-compile --resolver=backtracking --output-file=cicd/requirements.lock $(requirements) && \
	    pip-compile --resolver=backtracking --output-file=cicd/requirements_os.lock $(requirements) $(extra_requirements) && \
	    pip-compile --resolver=backtracking --output-file=cicd/requirements_cicd.lock $(cicd_requirements)'

upgrade-lock-files:
	$(container_cli) run --rm \
	    -w /app \
	    -v "$$PWD":/app \
	    $(image_name):$(version) \
	    /bin/bash -c 'pip-compile --resolver=backtracking --upgrade --output-file=cicd/requirements.lock $(requirements) && \
	    pip-compile --resolver=backtracking --upgrade --output-file=cicd/requirements_os.lock $(requirements) $(extra_requirements) && \
	    pip-compile --resolver=backtracking --upgrade --output-file=cicd/requirements_cicd.lock $(cicd_requirements)'

docs:
	$(container_cli) run --rm \
		-w /app \
		-v "$$PWD":/app \
		$(image_name):$(version) \
		/bin/bash -c 'cd $(build_src_dir) && python ./cicd/code_doc/render_doc.py'

lint:
	$(container_cli) run --rm \
		-w /app \
        -v "$$PWD":/app \
		$(image_name):$(version) \
		/bin/bash -c 'flake8 --docstring-convention google --config=cicd/flake8.conf lakehouse_engine tests cicd/code_doc/render_doc.py \
		&& mypy --config-file cicd/mypy.ini lakehouse_engine tests'

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
		-v $(git_credentials_file):$(container_user_dir)/.ssh/id_rsa \
		$(image_name):$(version) \
		/bin/bash

# Can use test only: ```make test test_only="tests/feature/test_delta_load_record_mode_cdc.py"```.
# You can also hack it by doing ```make test test_only="-rx tests/feature/test_delta_load_record_mode_cdc.py"```
# to show complete output even of passed tests.
# We also fix the coverage filepaths, using sed, so that report has the correct paths
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
            $(test_only)" && \
	sed -i'' -e 's/filename=\"/filename=\"lakehouse_engine\//g' artefacts/coverage.xml

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

test-security:
	$(container_cli) run \
		--rm \
		-w /app \
        -v "$$PWD":/app \
		$(image_name):$(version) \
		/bin/bash -c 'bandit -c cicd/bandit.yaml -r lakehouse_engine tests'

###########################
##### Release Targets #####
###########################
create-changelog:
	echo "# Changelog - $(shell date +"%Y-%m-%d") v$(shell python3 setup.py --version)" > CHANGELOG.md && \
	echo "All notable changes to this project will be documented in this file automatically. This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)." >> CHANGELOG.md && \
	echo "" >> CHANGELOG.md && \
	git log --no-decorate --pretty=format:"#### [%cs] [%(describe)]%n [%h]($(commits_url)%H) %s" -n 1000 >> CHANGELOG.md

bump-up-version:
	$(container_cli) run --rm \
		-w /app \
		-v "$$PWD":/app \
		$(image_name):$(version) \
		/bin/bash -c 'bump2version --config-file cicd/.bumpversion.cfg $(increment)'

prepare-release: bump-up-version create-changelog
	echo "Prepared version and changelog to release!"

commit-release:
	git commit -a -m 'Create release $(shell python3 setup.py --version)' && \
    git tag -a 'v$(shell python3 setup.py --version)' -m 'Release $(shell python3 setup.py --version)'

push-release:
	git push --follow-tags

delete-tag:
	git push --delete origin $(tag)

.PHONY: $(MAKECMDGOALS)
