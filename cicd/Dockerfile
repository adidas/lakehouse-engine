ARG PYTHON_IMAGE=python:3.12-slim-trixie

# The image is built in two stages to reduce the final image size.
# The first stage installs all build dependencies and compiles Python packages,
# while the second stage is a minimal runtime image that only contains the necessary
# runtime dependencies and compiled packages.

# Builder stage - install all build dependencies and compile Python packages
FROM $PYTHON_IMAGE AS builder

ARG CPU_ARCHITECTURE

# Install build prerequisites (will not be in final image)
RUN mkdir -p /usr/share/man/man1 && \
    apt-get -y update && \
    apt-get install -y --no-install-recommends wget=1.25.0-2 gnupg=2.4.7-21+deb13u1 g++=4:14.2.0-1 && \
    apt-get -y clean && rm -rf /var/lib/apt/lists/*

# Install JDK in builder
RUN mkdir -p /etc/apt/keyrings && \
    wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor | tee /etc/apt/trusted.gpg.d/adoptium.gpg > /dev/null && \
    echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list && \
    apt-get -y update && \
    apt-get -y install temurin-17-jdk && \
    apt-get -y clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/temurin-17-jdk-${CPU_ARCHITECTURE}

# Download and extract Spark
RUN wget -q https://downloads01-he-fi.apache.org/spark/spark-4.1.4/spark-4.1.4-bin-hadoop3.tgz && \
    tar -xf spark-4.1.4-bin-hadoop3.tgz && \
    rm spark-4.1.4-bin-hadoop3.tgz

# Install Python dependencies with uv
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN python -m pip install --no-cache-dir --upgrade pip==26.2.2 && \
    python -m pip install --no-cache-dir uv==0.13.6

COPY pyproject.toml uv.lock /tmp/project/
RUN cd /tmp/project && \
    uv export --frozen --no-emit-project \
    --all-extras \
    --format requirements.txt -o /tmp/requirements.txt && \
    uv pip install --python /opt/venv/bin/python -r /tmp/requirements.txt

# Runtime stage - minimal final image
FROM $PYTHON_IMAGE

ARG USER_ID=1000
ARG GROUP_ID=1000
ARG CPU_ARCHITECTURE

# Install runtime dependencies including temporary tools for JDK setup
RUN mkdir -p /usr/share/man/man1 && \
    apt-get -y update && \
    apt-get install -y --no-install-recommends rsync=3.4.1+ds1-5+deb13u4 git=1:2.47.3-0+deb13u1 wget=1.25.0-2 gnupg=2.4.7-21+deb13u1 g++=4:14.2.0-1 openssh-client=1:10.0p1-7+deb13u4 && \
    mkdir -p /etc/apt/keyrings && \
    wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor | tee /etc/apt/trusted.gpg.d/adoptium.gpg > /dev/null && \
    echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list && \
    apt-get -y update && \
    apt-get -y install temurin-17-jre && \
    apt-get -y remove wget gnupg --purge && \
    apt-get -y clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/temurin-17-jre-${CPU_ARCHITECTURE}

# useradd -l is necessary to avoid docker build hanging in export image phase when using large uids
RUN if ! getent group ${GROUP_ID} > /dev/null; then groupadd -g ${GROUP_ID} appuser; fi && \
    useradd -rm -l -u ${USER_ID} -d /home/appuser -s /bin/bash -g ${GROUP_ID} appuser

# Copy compiled Python dependencies from builder
COPY --from=builder --chown=appuser:appuser /opt/venv /opt/venv

# Copy Spark from builder
COPY --from=builder --chown=appuser:appuser /spark-4.1.4-bin-hadoop3 /home/appuser/spark

USER appuser

ENV PATH="/opt/venv/bin:$PATH" \
    SPARK_HOME=/home/appuser/spark

RUN mkdir /home/appuser/.ssh/ && touch /home/appuser/.ssh/known_hosts

RUN echo Image built for $CPU_ARCHITECTURE with python image $PYTHON_IMAGE.