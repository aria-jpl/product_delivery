FROM hysds/pge-base:latest

MAINTAINER hysds-dev "hysds-dev@jpl.nasa.gov"
LABEL description="Delivery of products"

ARG id
ARG gid

# copy PGE
COPY . /home/ops/verdi/ops/product_delivery
RUN set -ex \
 && sudo chown -R ops:ops /home/ops/verdi/ops/product_delivery
RUN sudo pip install elasticsearch

# set entrypoint
USER ops
WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
