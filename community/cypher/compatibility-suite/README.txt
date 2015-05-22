In order to integrate cucumber in Intellij please install the following plugins:
cucumber-java
cucumber-scala

In such a way you can run features or individual scenarios from Intellij.

Note though that the Intellij test run configuration must be modified manually in order to set the correct glue path,
i.e., the the package where the step implementations are defined. Search for 'GlueSteps.scala' to find the correct package.

Finally, notice that several plugins must be enable in order to execute correctly the cucumber runner.
Please  add a --plugin $PLUGIN_NAME to the Intellij test run configuration for each plugin.
The list of necessary plugins with relative configuration can be found in 'FeatureSuiteTest.java'
