#!/bin/bash

for k in `moglistkeys --trackers=127.0.0.1:7001`; do
	echo deleting $k
	mogdelete --key=$k
done

