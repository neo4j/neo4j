In order to integrate cucumber in Intellij please install the following plugins:
cucumber-java
cucumber-scala

In such a way you can run features or individual scenarios from Intellij.

Note though that the Intellij test run configuration must be modified manually in order to set the correct glue path,
i.e., the the package where the step implementations are defined. Search for 'CompatibilitySpecSuiteSteps.scala' to find the correct package.

Finally, notice that several plugins must be enable in order to execute correctly the cucumber runner.
Please  add a --plugin $PLUGIN_NAME to the Intellij test run configuration for each plugin.
The list of necessary plugins with relative configuration can be found in 'FeatureSuiteTest.java'

An example of a configuration is the following (as of 2016-03-18):

Main Class:	cucumber.api.cli.Main
Glue:	cypher
Feature or folder path:	/path/to/neo4j/checkout/community/cypher/compatibility-suite/src/test/resources/cypher/
VM options:
Program arguments:	 --plugin org.jetbrains.plugins.cucumber.java.run.CucumberJvmSMFormatter --plugin cypher.cucumber.db.DatabaseProvider:target/dbs --plugin cypher.cucumber.db.DatabaseConfigProvider:/cypher/db/config/${YOUR_CONFIG_HERE}.json  --plugin cypher.feature.reporting.CypherResultReporter:target/reporter-output --monochrome
Working directory:
Enviroment variables:
Use classpath of module:	neo4j-cypher-compatibility-suite
