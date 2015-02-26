#!/usr/bin/env bash

# Get Containers IP addresses
PUBIP=$(sudo docker inspect --format '{{ .NetworkSettings.IPAddress }}' ROQPUB);
GCMIP=$(sudo docker inspect --format '{{ .NetworkSettings.IPAddress }}' ROQGCM);

# Open an ssh connection to the publisher container 
# and run the publisher's daemon inside
ssh -o StrictHostKeyChecking=no root@$PUBIP \
		java -Djava.library.path=/usr/local/lib -cp \
		/lib/ROQ/roq-demonstration/target/roq-demonstration-1.0-SNAPSHOT-jar-with-dependencies.jar \
		org.roq.demonstration.RoQDemonstrationPublisherLauncher $GCMIP testQ