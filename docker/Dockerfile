FROM hysds/isce2:latest-es1

MAINTAINER hysds-dev "hysds-dev@jpl.nasa.gov"
LABEL description="Delivery of products"

ARG id
ARG gid

# copy PGE
COPY . /home/ops/verdi/ops/product_delivery
RUN set -ex \
 && sudo chown -R ops:ops /home/ops/verdi/ops/product_delivery

# set entrypoint
USER ops
WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
