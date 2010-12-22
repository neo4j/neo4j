package org.neo4j.server.configuration.validation;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.commons.configuration.BaseConfiguration;
import org.junit.Test;
import org.neo4j.server.configuration.Configurator;


public class WebadminConfigurationRuleTest {
    
    private static final boolean theValidatorHasPassed = true;
    
    @Test(expected=RuleFailedException.class)
    public void shouldFailIfNoWebadminConfigSpecified() throws RuleFailedException {
        WebadminConfigurationRule rule = new WebadminConfigurationRule();
        BaseConfiguration emptyConfig = new BaseConfiguration();
        rule.validate(emptyConfig);
        assertFalse(theValidatorHasPassed);
    }
    
    @Test(expected=RuleFailedException.class)
    public void shouldFailIfOnlyRestApiKeySpecified() throws RuleFailedException {
        WebadminConfigurationRule rule = new WebadminConfigurationRule();
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(Configurator.WEB_ADMIN_REST_API_PATH_PROPERTY_KEY, "http://localhost:7474/db/data");
        rule.validate(config);
        assertFalse(theValidatorHasPassed);
    }
    
    @Test(expected=RuleFailedException.class)
    public void shouldFailIfOnlyAdminApiKeySpecified() throws RuleFailedException {
        WebadminConfigurationRule rule = new WebadminConfigurationRule();
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(Configurator.WEB_ADMIN_PATH_PROPERTY_KEY, "http://localhost:7474/db/manage");
        rule.validate(config);
        assertFalse(theValidatorHasPassed);
    }
    
    @Test
    public void shouldAllowAbsoluteUris() throws RuleFailedException {
        WebadminConfigurationRule rule = new WebadminConfigurationRule();
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(Configurator.WEB_ADMIN_REST_API_PATH_PROPERTY_KEY, "http://localhost:7474/db/data");
        config.addProperty(Configurator.WEB_ADMIN_PATH_PROPERTY_KEY, "http://localhost:7474/db/manage");
        rule.validate(config);
        assertTrue(theValidatorHasPassed);
    }
    
    @Test
    public void shouldAllowRelativeUris() throws RuleFailedException {
        WebadminConfigurationRule rule = new WebadminConfigurationRule();
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(Configurator.WEB_ADMIN_REST_API_PATH_PROPERTY_KEY, "/db/data");
        config.addProperty(Configurator.WEB_ADMIN_PATH_PROPERTY_KEY, "/db/manage");
        rule.validate(config);
        assertTrue(theValidatorHasPassed);
    }
    
    @Test
    public void shouldNormaliseUris() throws RuleFailedException {
        WebadminConfigurationRule rule = new WebadminConfigurationRule();
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(Configurator.WEB_ADMIN_REST_API_PATH_PROPERTY_KEY, "http://localhost:7474///db///data///");
        config.addProperty(Configurator.WEB_ADMIN_PATH_PROPERTY_KEY, "http://localhost:7474////db///manage");
        rule.validate(config);
        
        assertThat((String)config.getProperty(Configurator.WEB_ADMIN_REST_API_PATH_PROPERTY_KEY), not(containsString("///")));
        assertThat((String)config.getProperty(Configurator.WEB_ADMIN_PATH_PROPERTY_KEY), not(containsString("///")));
        assertFalse(((String)config.getProperty(Configurator.WEB_ADMIN_REST_API_PATH_PROPERTY_KEY)).endsWith("//"));
        assertTrue(((String)config.getProperty(Configurator.WEB_ADMIN_REST_API_PATH_PROPERTY_KEY)).endsWith("/"));
    }
}
