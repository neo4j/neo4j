package org.neo4j.server.configuration.validation;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.junit.Test;
import org.neo4j.server.logging.InMemoryAppender;

public class DuplicateKeyRuleTest {

    private static final String EMPTY_STRING = "";

    @Test
    public void duplicateKeyAndValueShouldNotSignalRuleFailure() {
        DuplicateKeyRule rule = new DuplicateKeyRule();

        BaseConfiguration configA = new BaseConfiguration();
        configA.addProperty("jim", "webber");

        BaseConfiguration configB = new BaseConfiguration();
        configB.addProperty("andreas", "kollegger");

        CompositeConfiguration cc = new CompositeConfiguration();
        cc.addConfiguration(configA);

        rule.validate(cc, configB);
    }

    @Test
    public void duplicateKeyAndValueShouldLogWhereDuplicatesAreFound() {
        InMemoryAppender appender = new InMemoryAppender(DuplicateKeyRule.log);
        DuplicateKeyRule rule = new DuplicateKeyRule();

        BaseConfiguration configA = new BaseConfiguration();
        configA.addProperty("jim", "webber");

        BaseConfiguration configB = new BaseConfiguration();
        configB.addProperty("jim", "webber");

        CompositeConfiguration cc = new CompositeConfiguration();
        cc.addConfiguration(configA);

        rule.validate(cc, configB);

        assertThat(appender.toString(), not(EMPTY_STRING));
        assertThat(appender.toString(), containsString("Duplicate key [jim] found in configuration files"));

    }

    @Test(expected = RuleFailedException.class)
    public void duplicateKeyWithDifferentValueShouldCauseFailure() {
        DuplicateKeyRule rule = new DuplicateKeyRule();

        BaseConfiguration configA = new BaseConfiguration();
        configA.addProperty("jim", "webber");

        BaseConfiguration configB = new BaseConfiguration();
        configB.addProperty("jim", "kollegger");

        CompositeConfiguration cc = new CompositeConfiguration();
        cc.addConfiguration(configA);

        rule.validate(cc, configB);

    }
}
