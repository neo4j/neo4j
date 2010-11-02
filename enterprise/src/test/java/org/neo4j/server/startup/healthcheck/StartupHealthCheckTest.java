package org.neo4j.server.startup.healthcheck;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Test;
import org.neo4j.server.logging.InMemoryAppender;


public class StartupHealthCheckTest {

    @Test
    public void shouldPassWithNoRules() {
        StartupHealthCheck check = new StartupHealthCheck();
        assertTrue(check.run());
    }
    
    @Test
    public void shouldRunAllHealthChecksToCompletionIfNonFail() {
        StartupHealthCheck check = new StartupHealthCheck(getPassingRules());
        assertTrue(check.run());
    }
    
    @Test
    public void shouldFailIfOneOrMoreHealthChecksFail() {
        StartupHealthCheck check = new StartupHealthCheck(getWithOneFailingRule());
        assertFalse(check.run());
    }
    
    @Test
    public void shouldLogFailedRule() {
        StartupHealthCheck check = new StartupHealthCheck(getWithOneFailingRule());
        InMemoryAppender appender = new InMemoryAppender(StartupHealthCheck.log);
        check.run();
        
        assertThat(appender.toString(), containsString("ERROR - blah blah"));
    }
    
    private StartupHealthCheckRule[] getWithOneFailingRule() {
        StartupHealthCheckRule[] rules = new StartupHealthCheckRule[5];
        
        for(int i = 0; i < rules.length; i++) {
            rules[i] = new StartupHealthCheckRule() {
                public boolean execute(Properties properties) {
                    return true;
                }

                public String getMessage() {
                    return "blah blah";
                }};
        }
        
        rules[rules.length / 2] = new StartupHealthCheckRule() {
            public boolean execute(Properties properties) {
                return false;
            }

            public String getMessage() {
                return "blah blah";
            }};
        
        return rules;
    }

    private StartupHealthCheckRule[] getPassingRules() {
        StartupHealthCheckRule[] rules = new StartupHealthCheckRule[5];
        
        for(int i = 0; i < rules.length; i++) {
            rules[i] = new StartupHealthCheckRule() {
                public boolean execute(Properties properties) {
                    return true;
                }

                public String getMessage() {
                    return "blah blah";
                }};
        }
        
        return rules;
    }
}
