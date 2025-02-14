# Cucumber based tests for Cypher (.feature files)

This module uses Cucumber libraries. See documentation at:

- [Cucmber](https://cucumber.io/docs/cucumber/)
- [Cucumber JUnit Platform Engine](https://github.com/cucumber/cucumber-jvm/tree/main/cucumber-junit-platform-engine)

## Running scenarios

### Single Scenarios/Features From Intellij IDEA

1. Install JetBrains "Cucumber for Java" Intellij Idea plugin.
2. Select the configuration in `cucumber.properties`.
3. Click to run/debug.

### Full Suites

The full suites runs in parallel and is much faster for this reason.
They can be run both from the IDE and are also run as part of maven builds.

See classes in:

- `CypherFeatureAcceptanceTests.scala`
- `CypherFeatureTckTests.scala`
- `PrettifierFeatureTests.scala`

## Tags

- `@fails` -
  expect failure in all configurations.
- `@fails:parallel-runtime`, `@fails:cypher-5`, `@fails:db-format-multiversion`, ... -
  expected failure in specific configuration.
- `@conf:key=value`, `@conf:internal.cypher.enable_extra_semantic_features=MatchModes`, ... -
  scenario needs extra configuration, like a feature flag.
- `@ignore` -
  skip scenario in all configurations. Avoid if possible!
- `@ignore:parallel-runtime`, `@ignore:cypher-5`, `@ignore:db-format-multiversion`... -
  skip scenario in specific configuration. Avoid if possible!
- External scenarios (the TCK ones from open cypher) don't have the required tags,
  so they are loaded from `tck-tags.txt`.

## Extending the API

Some starting points:

- `CypherCucumberSteps.scala` contains a trait with definitions of the steps we can use.
  Implementing classes contains the assertions.
- `TestConf.scala` contains definitions of the configurations we can run in.
- `TestFrameworkTests.feature` (and accompanying `CypherCucumberTest.scala`) contains tests of the test framework,
  it's good practice to add test cases here when extending the steps. 