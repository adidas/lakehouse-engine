ARG PYTHON_IMAGE

FROM $PYTHON_IMAGE

ARG USER_ID=1000
ARG GROUP_ID=1000
ARG PYTHON_IMAGE
ARG CPU_ARCHITECTURE

# Install Prerequisites
RUN mkdir -p /usr/share/man/man1 && \
    apt-get -y update && \
    apt-get install -y wget=1.21* gnupg2=2.2* unzip=6.0* git=1:2* g++=4:10.2.1*  rsync=3.2* && \
    apt-get -y clean

# Install jdk
RUN mkdir -p /etc/apt/keyrings && \
    wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | tee /etc/apt/keyrings/adoptium.asc && \
    echo "deb [signed-by=/etc/apt/keyrings/adoptium.asc] https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list && \
    apt-get -y update && \
    apt-get -y install temurin-8-jdk && \
    apt-get -y clean
ENV JAVA_HOME=/usr/lib/jvm/temurin-8-jdk-${CPU_ARCHITECTURE}

# useradd -l is necessary to avoid docker build hanging in export image phase when using large uids
RUN groupadd -g ${GROUP_ID} appuser && \
    useradd -rm -l -u ${USER_ID} -d /home/appuser -s /bin/bash -g appuser appuser

COPY cicd/requirements_full.lock /tmp/requirements.txt

USER appuser

ENV PATH="/home/appuser/.local/bin:$PATH"
RUN python -m pip install --upgrade pip==24.* setuptools==74.* --user
RUN python -m pip install --user -r /tmp/requirements.txt

RUN mkdir /home/appuser/.ssh/ && touch /home/appuser/.ssh/known_hosts

RUN echo Image built for $CPU_ARCHITECTURE with python image $PYTHON_IMAGE.
