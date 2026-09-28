FROM postgres:16.4

COPY replica-entrypoint.sh /usr/local/bin/eventflow-replica-entrypoint
RUN chmod 0755 /usr/local/bin/eventflow-replica-entrypoint

ENTRYPOINT ["/usr/local/bin/eventflow-replica-entrypoint"]
