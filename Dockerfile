FROM tomcat:7.0-jre8-alpine
RUN apk add gettext # for envsubst
WORKDIR $CATALINA_HOME
ARG WAR_FILE
COPY with-templates.sh /bin/with-templates.sh
RUN chmod a+x /bin/with-templates.sh
ENTRYPOINT ["/bin/with-templates.sh"]
# Setting ENTRYPOINT resets CMD
CMD ["catalina.sh", "run"]
RUN wget http://central.maven.org/maven2/mysql/mysql-connector-java/5.1.28/mysql-connector-java-5.1.28.jar -O/usr/local/tomcat/lib/mysql-connector-java-5.1.28.jar

RUN mkdir -p /opt/plos/data
COPY target/$WAR_FILE /usr/local/tomcat/webapps/v1.war
COPY src/deb/tomcat7/conf/context-local.template.xml /usr/local/tomcat/conf/context.template.xml
ENV TEMPLATES "/usr/local/tomcat/conf/context.template.xml"
