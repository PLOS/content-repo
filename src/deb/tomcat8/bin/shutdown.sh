#!/usr/bin/env bash

export CATALINA_BASE="$( dirname "${BASH_SOURCE[0]}" )"/..
/usr/share/tomcat8/bin/shutdown.sh
echo "Tomcat stopped"
