package org.neo4j.server.configuration.validation;

import org.apache.commons.configuration.BaseConfiguration;
import org.junit.Test;


public class DatabaseLocationMustBeSpecifiedRuleTest {
    @Test(expected=RuleFailedException.class)
    public void shouldFailWhenDatabaseLocationIsAbsentFromConfig() {
        DatabaseLocationMustBeSpecifiedRule theRule = new DatabaseLocationMustBeSpecifiedRule();
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty("foo", "bar");
        theRule.validate(config);
    }
}
