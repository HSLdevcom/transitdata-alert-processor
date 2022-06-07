FROM openjdk:8-jre-slim

#Install curl for health check
RUN apt-get update && apt-get install -y --no-install-recommends curl

ADD target/transitdata-alert-processor-jar-with-dependencies.jar /usr/app/transitdata-alert-processor.jar
COPY start-application.sh /
RUN chmod +x /start-application.sh

CMD ["/start-application.sh"]
