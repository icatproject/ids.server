#!/bin/bash

mvn clean install -DskipTests
cp src/test/java/test.properties target/test-classes/
cp target/IDS-0.0.war install/
cd install/
./ids_setup.py --deploy
