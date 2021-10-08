FROM registry.access.redhat.com/ubi8/python-39 as base

# Add application sources to a directory that the assemble script expects them
# and set permissions so that the container runs without root access
USER 0
ADD setup.py requirements.txt /tmp/src/
ADD src /tmp/src/src

RUN /usr/bin/fix-permissions /tmp/src
USER 1001

# Install the dependencies
RUN /usr/libexec/s2i/assemble

# Set the default command for the resulting image
CMD /usr/libexec/s2i/run

FROM base as test

ADD tests/test_* tests/floorplan_* tests/requirements.txt ./tests/

RUN pip install -r tests/requirements.txt
