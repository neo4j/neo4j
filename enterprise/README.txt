neo4j-server project
====================

This component builds the runnable server component. 

build-test-run
--------------

When building for the first time, do:

`mvn clean package -P initial-build`

Subsequent builds can simply:

`mvn clean package`

Finally, run the server using:

`mvn exec:java`

functional-tests
----------------

To run the functional tests:

`mvn clean package -Dtests=functional`


