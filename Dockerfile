FROM apache/spark:4.0.1-scala2.13-java17-python3-ubuntu
USER root

# Image nay dung Ubuntu 22.04 (Python 3.10, pip 22) nen khong can --break-system-packages
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3-pip \
 && rm -rf /var/lib/apt/lists/* \
 && python3 -m pip install --no-cache-dir --upgrade pip \
 && python3 -m pip install --no-cache-dir \
      pyspark==4.0.1 confluent-kafka jupyterlab

ENV PYSPARK_PYTHON=python3
RUN ln -sf /usr/bin/python3 /usr/bin/python
WORKDIR /workspace
