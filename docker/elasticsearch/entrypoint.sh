#!/bin/bash
# Fix data directory ownership for bind mounts, then drop back to elasticsearch user
if [ "$(id -u)" = '0' ]; then
  chown -R elasticsearch:elasticsearch /usr/share/elasticsearch/data
  exec runuser -u elasticsearch -- /usr/local/bin/docker-entrypoint.sh elasticsearch
else
  exec /usr/local/bin/docker-entrypoint.sh elasticsearch
fi
