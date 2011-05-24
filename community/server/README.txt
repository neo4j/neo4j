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



integration-tests
----------------

To run the webdriver-selenium-integration tests:

`mvn clean integration-test -Dtests=web`

You can also run them one-by-one via a web GUI:

`tools/cukerunner.py`

Webadmin development
--------------------

Webadmin development comes with a few helpers to make development faster. To 
work on webadmin, let the following commands run in separate terminals:

Run the server, let this get the server started before issuing other commands:

`mvn clean antrun:run -Pwebdev-exec,neodev`

Auto-recompile coffeescript and HAML files:

`mvn package -Pwebadmin-build -Dbrew.watch=true -DskipTests`
