FROM maven:3.9.9-eclipse-temurin-17 AS build

WORKDIR /workspace

COPY pom.xml ./
COPY jmqx-app/pom.xml jmqx-app/pom.xml
COPY jmqx-bench/pom.xml jmqx-bench/pom.xml
COPY jmqx-cluster/pom.xml jmqx-cluster/pom.xml
COPY jmqx-common/pom.xml jmqx-common/pom.xml
COPY jmqx-core/pom.xml jmqx-core/pom.xml
COPY jmqx-plugin/pom.xml jmqx-plugin/pom.xml
COPY jmqx-protocol/pom.xml jmqx-protocol/pom.xml
COPY jmqx-transport/pom.xml jmqx-transport/pom.xml

COPY . .

RUN mvn -pl jmqx-app -am -DskipTests package

FROM eclipse-temurin:17-jre

WORKDIR /opt/jmqx

COPY --from=build /workspace/jmqx-app/target/jmqx-app.jar /opt/jmqx/jmqx-app.jar

RUN mkdir -p /opt/jmqx/data

ENV JMQX_JAVA_OPTS=""

EXPOSE 1883 8083 7800 7900 17800 18081

VOLUME ["/opt/jmqx/data"]

ENTRYPOINT ["sh", "-c", "exec java $JMQX_JAVA_OPTS -jar /opt/jmqx/jmqx-app.jar"]
