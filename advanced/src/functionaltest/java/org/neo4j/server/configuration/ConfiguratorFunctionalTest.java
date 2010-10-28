package org.neo4j.server.configuration;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.commons.configuration.Configuration;
import org.junit.Test;


public class ConfiguratorFunctionalTest {
    @Test
    public void shouldUseSpecifiedConfigDir() {
        Configuration testConf = new Configurator(new File("src/test/resources/etc/neo-server")).configuration();

        final String EXPECTED_VALUE = "bar";
        assertEquals(EXPECTED_VALUE, testConf.getString("org.neo4j.foo"));
    }
}
