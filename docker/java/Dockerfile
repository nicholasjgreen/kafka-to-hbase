FROM debian:bullseye-slim
RUN apt update && apt upgrade -y
RUN apt install -y \
      openjdk-11-jdk \
      wget \
      jq
ENV JAVA_HOME /etc/alternatives/jre
