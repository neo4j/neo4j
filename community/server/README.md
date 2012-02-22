# neo4j-server
 
This project contains the runnable neo4j server component. 

## build-test-run cycle

When building for the first time, do:

    mvn clean package -P initial-build

Subsequent builds can simply:

    mvn clean package

Finally, run the server using:

    mvn exec:java

## Webadmin development

Webadmin builds during the compile and process-classes phases. If you are doing webadmin development work, you can make your changes auto-deploy, so you don't have to restart the server. Run the two commands below in separate consoles.

Start the server (let this get the server started before issuing other commands):

    mvn clean compile exec:java -Pneodev

Auto-deploy changes to webadmin files:

    mvn compile -Dbrew.watch=true -Pneodev

Then go to [http://localhost:7474/webadmin/](http://localhost:7474/webadmin/)

## Webadmin unit tests

Unit tests for webadmin are written with the Jasmine BDD framework. They are run as 
part of the normal compile and test cycles.

If you want, you can set up the tests to re-run rapidly during development, without
having to run the whole server build. To do that, start up auto-deployment of webadmin
files as described in the section above, and then run:

    mvn jasmine:bdd

This will start up the standard Jasmine web server. Navigate to the url this command prints
out, and refresh the page to re-run your tests.

## Webadmin integration tests

To run the webdriver tests for webadmin:

    mvn clean test -Dtests=web

Or, to run all tests (both unit, functional and webdriver):

    mvn clean test -Dtests=all

You can run the tests under different browsers using maven profiles. By default, Firefox is used. 
Available profiles are:

* `-Pie` for internet explorer
* `-Pchrome-mac` for chrome on Mac.
  
Please note that the chrome driver requires an binary managed outside of maven.  If not already downloaded, the tests will attempt to download and extract the binary to `server/webdriver-binaries/` 
