FROM docker.elastic.co/elasticsearch/elasticsearch:8.15.0

# Rename original entrypoint
RUN mv /usr/local/bin/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh.orig

# Copy your custom entrypoint (không chmod/chown)
COPY docker-entrypoint.sh /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

CMD ["eswrapper"]
