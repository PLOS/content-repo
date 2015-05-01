#!/bin/bash

docker-compose rm --force && docker-compose build && docker-compose up
