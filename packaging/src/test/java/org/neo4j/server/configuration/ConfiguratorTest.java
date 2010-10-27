package org.neo4j.server.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.commons.configuration.Configuration;
import org.junit.Test;

public class ConfiguratorTest {

    private static final String TEST_COMPONENT = "test.component";

    @Test
    public void shouldProvideAConfiguration() {
       Configuration config = Configurator.getConfigurationFor(TEST_COMPONENT); 
       assertNotNull(config);
    }    
    
    @Test
    public void shouldRegisterConfigurationSources() {
        Configuration mockConfig = mock(Configuration.class);
        String configValue = "AndreasAndJim";
        when(mockConfig.getString(anyString())).thenReturn(configValue);

        Configurator.set(TEST_COMPONENT, mockConfig);
        
        Configuration config = Configurator.getConfigurationFor(TEST_COMPONENT);
        
        assertEquals(configValue, config.getString("any old string will do"));
    }
}