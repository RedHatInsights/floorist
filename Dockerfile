ARG deps="python3.12 python3.12-pip libpq procps-ng diffutils"
ARG devDeps="python3.12 python3.12-devel python3.12-pip postgresql-devel gcc"

FROM registry.access.redhat.com/ubi9/ubi-minimal AS build

# Add application sources to a directory that the assemble script expects them
# and set permissions so that the container runs without root access
USER 0

ARG devDeps

RUN microdnf install -y $devDeps                            && \
    python3.12 -m pip  install virtualenv                   && \
    mkdir -p /opt/app-root                                  && \
    chown 1001:0 /opt/app-root

WORKDIR /opt/app-root

COPY app.py pyproject.toml setup.cfg requirements.txt ./
COPY src ./src

RUN virtualenv .                          && \
    bin/pip install --upgrade pip         && \
    bin/pip install -r requirements.txt . && \
    rm -rf \~

FROM registry.access.redhat.com/ubi9/ubi-minimal AS base

USER 0

ARG deps
ENV deps=${deps}

WORKDIR /opt/app-root

COPY --chown=1001:0 --from=build /opt/app-root /opt/app-root

RUN rpm -qa --qf '%{NAME}\n' | sort > /opt/basepackages && \
    microdnf install -y $deps && \
    rpm -qa --qf '%{NAME}\n' | sort | diff /opt/basepackages - | grep -Po '(?<=> )(.*)' > /opt/installedpackages && \
    rm /opt/basepackages && \
    chown 1001:0 /opt/app-root

USER 1001

ENV PATH="/opt/app-root/bin:$PATH"

# Set the default command for the resulting image
CMD python ./app.py

FROM base AS test

ADD tests/test_* tests/floorplan_* tests/requirements.txt ./tests/

RUN pip install --no-cache-dir -r tests/requirements.txt
