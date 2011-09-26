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

Webadmin builds durin the compile and process-classes phases. If you are doing webadmin development work, you can make your changes auto-deploy, so you don't have to restart the server. Run the two commands below in separate consoles.

Start the server (let this get the server started before issuing other commands):

`mvn clean compile exec:java -Pneodev`

Auto-deploy changes to webadmin files:

`mvn compile -Dbrew.watch=true -Pneodev`

Then go to http://localhost:7474/webadmin/dev.html 

The dev.html file loads each individual js file, which makes debugging a lot easier. 
