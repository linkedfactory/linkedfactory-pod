FROM eclipse-temurin:17-jre-alpine
EXPOSE 8080

#RUN apk update && apk add --no-cache libstdc++ bash
RUN apk add gcompat

# Add linkedfactory-pod server
ADD ./server/target/assembly /opt/linkedfactory-pod

RUN set -x && \
  ln -s /opt/linkedfactory-pod /linkedfactory-pod

VOLUME /linkedfactory-pod/workspace

# copy config.ttl to root directory for initializing mounted workspace directories
ADD ./server/target/assembly/workspace/config.ttl /linkedfactory-pod/config.ttl
ADD ./docker/entrypoint.sh .
RUN chmod +x entrypoint.sh
ENTRYPOINT ["./entrypoint.sh"]

# Pass command to entrypoint for running the linkedfactory-pod server
CMD ["/linkedfactory-pod/bin/karaf", "run"]
