# neo4j-server
 
This project contains the runnable neo4j server component. 

## build-test-run cycle

When building for the first time, do:

    mvn clean package -P initial-build

Subsequent builds can simply:

    mvn clean package

Run the server using:

    mvn exec:java

If this fails, ensure you are not missing neo4j-home/conf/neo4j-server.properties
with a line containing 'org.neo4j.server.database.location=<path-to-database-files>'.
This may happen if you delete neo4j-home during development to reset the database.

## Webadmin development

The web administration interface, webadmin, can be found in two places of the source tree:

    # This contains uncompiled coffeescript and haml files, including unit tests (under test/)
    src/main/coffeescript

    # This contains static web resources, such as javascript libraries, css and images
    src/main/resources/webadmin-html
    
Webadmin compiles as part of the normal server build. 
If you are doing development work, you can start a server instance that automatically picks up
changes in the two webadmin source folders, and deploys them into the running server. 

    # Run this bash command while standing in the 'server'-folder
    tools/webadmin-develop

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

    mvn clean verify -DfullBuild

You can run the tests under different browsers using maven profiles. By default, Firefox is used. 
Available profiles are:

* `-Pie` for internet explorer
* `-Pchrome-mac` for chrome on Mac.
  
Please note that the chrome driver requires an binary managed outside of maven.  If not already downloaded, the tests will attempt to download and extract the binary to `server/webdriver-binaries/` 

## Codenames and icons

Each version of the neo4j server has a codename and a corresponding icon. 
The build system is set up such that the build fails if a codename and an icon is not defined for the current version.

Codenames for each version is defined here:

    src/main/metadata/codenames.properties

Icons are located here:

    src/main/resources/webadmin-html/img/icons/branding
