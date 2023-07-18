FROM adoptopenjdk:11-jre-hotspot
EXPOSE 8080

# Add linkedfactory-pod server
ADD ./server/target/assembly /opt/linkedfactory-pod

RUN set -x && \
  ln -s /opt/linkedfactory-pod /linkedfactory-pod

VOLUME /linkedfactory-pod/workspace

# Run the enilink platform server
CMD ["/linkedfactory-pod/bin/karaf", "run"]
