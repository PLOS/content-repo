Dockerized Content Repo
=======================

So you want to run your service in an isolated container?

The docker stack provided here contains all your should need to get a full production stack running including MySQL and MogileFS.

1. Make sure the service .war is built
2. Enter this directory and run "./repo-up.sh"
3. Visit the running service at http://localhost:8085

Requirements
------------
* docker >= 1.3 (www.docker.com)
* docker-compse (pip install docker-compose)
* fig >= 0.5.2 (www.fig.sh)
