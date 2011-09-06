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

To run the selenium-integration tests:

`mvn clean integration-test -Dtests=web`

You can also run them one-by-one via a web GUI:

`tools/cukerunner.py`

You can run the tests under different browsers using maven profiles. By default, Firefox is used. 
Available profiles are: -Phtmlunit for headless browser emulation, -Pie for internet explorer, and -Pchrome for chrome.
Please note that the chrome driver requires an installed binary on your system, which you can find here: http://code.google.com/p/chromium/downloads/list

Install it to a location of your choice, and tell the tests where to find it using the 'webdriver.chrome.driver' property:

`-Dwebdriver.chrome.driver=/path/to/chromedriver`


Webadmin development
--------------------

The webadmin build tool (brew) is disconnected from the normal build, and
the artifact it produces (webadmin.min.js) is actually checked into this 
source tree. If you make changes to webadmin code, run the following command
to re-build webadmin.min.js:

`mvn clean package -Pwebadmin-build`

Webadmin development comes with a few helpers to make development faster. To 
work on webadmin, let the following commands run in separate terminals:

Run the server (let this get the server started before issuing other commands):

`mvn clean compile antrun:run -Pwebdev-exec,neodev`

Auto-recompile coffeescript and HAML files:

`mvn package -Pwebadmin-build,neodev -Dbrew.watch=true -DskipTests`

Then go to http://localhost:7474/webadmin/dev.html 

The dev.html file loads each individual js file, unminified, which makes debugging a lot easier. Please note however, that for your changes to be seen in the normal http://localhost:7474/webadmin/ you need to run the normal webadmin build at the top of this section.
