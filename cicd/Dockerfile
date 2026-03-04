ARG PYTHON_IMAGE=python:3.12-slim-bullseye

FROM $PYTHON_IMAGE

ARG USER_ID=1000
ARG GROUP_ID=1000
ARG CPU_ARCHITECTURE

# Install Prerequisites
RUN mkdir -p /usr/share/man/man1 && \
    apt-get -y update && \
    apt-get install -y wget=1.21* gnupg2=2.2* git=1:2* g++=4:10.2.1* rsync=3.2* && \
    apt-get -y clean

# Install jdk
RUN mkdir -p /etc/apt/keyrings && \
    wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor | tee /etc/apt/trusted.gpg.d/adoptium.gpg > /dev/null && \
    echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list && \
    apt-get -y update && \
    apt-get -y install temurin-17-jdk && \
    apt-get -y clean
ENV JAVA_HOME=/usr/lib/jvm/temurin-17-jdk-${CPU_ARCHITECTURE}

# useradd -l is necessary to avoid docker build hanging in export image phase when using large uids
RUN groupadd -g ${GROUP_ID} appuser && \
    useradd -rm -l -u ${USER_ID} -d /home/appuser -s /bin/bash -g appuser appuser

COPY cicd/requirements_full.lock /tmp/requirements.txt

USER appuser

ENV PATH="/home/appuser/.local/bin:$PATH"
RUN python -m pip install --upgrade pip==25.2 setuptools==74.* --user
RUN python -m pip install --user -r /tmp/requirements.txt

RUN mkdir /home/appuser/.ssh/ && touch /home/appuser/.ssh/known_hosts

RUN echo Image built for $CPU_ARCHITECTURE with python image $PYTHON_IMAGE.
