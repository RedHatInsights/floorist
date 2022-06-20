FROM registry.access.redhat.com/ubi8/ubi-minimal AS build

# Add application sources to a directory that the assemble script expects them
# and set permissions so that the container runs without root access
USER 0

RUN microdnf install -y python39-devel postgresql-devel gcc && \
    pip3 install virtualenv                                 && \
    mkdir -p /opt/app-root                                  && \
    chown 1001:0 /opt/app-root

WORKDIR /opt/app-root

COPY app.py pyproject.toml setup.cfg requirements.txt ./
COPY src ./src

RUN virtualenv .                          && \
    bin/pip install --upgrade pip         && \
    bin/pip install -r requirements.txt . && \
    rm -rf \~

FROM registry.access.redhat.com/ubi8/ubi-minimal as base

USER 0

WORKDIR /opt/app-root

COPY --chown=1001:0 --from=build /opt/app-root /opt/app-root

RUN microdnf install -y python39 libpq procps-ng && \
    chown 1001:0 /opt/app-root

USER 1001

ENV PATH="/opt/app-root/bin:$PATH"

# Set the default command for the resulting image
CMD python ./app.py

FROM base as test

ADD tests/test_* tests/floorplan_* tests/requirements.txt ./tests/

RUN pip install --no-cache-dir -r tests/requirements.txt
