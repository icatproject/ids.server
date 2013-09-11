#!/bin/bash

mvn clean install -DskipTests
asadmin undeploy ids-1.0.0-SNAPSHOT
asadmin deploy --contextroot ids target/ids-1.0.0-SNAPSHOT.war | grep -v PER0
