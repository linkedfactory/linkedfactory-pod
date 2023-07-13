FROM adoptopenjdk:11-jre-hotspot

# Add enilink server
ADD server/target/assembly /opt/enilink-dist

RUN set -x && \
  ln -s /opt/enilink-dist /enilink-server

# Run the enilink platform server
CMD ["/enilink-server/bin/karaf", "run"]
