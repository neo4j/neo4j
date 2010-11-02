package org.neo4j.server.startup.healthcheck;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerTestUtils;

public class ConfigFileMustBePresentRuleTest {
    @Test
    public void shouldFailIfThereIsNoSystemPropertyForConfigFile() {
        ConfigFileMustBePresentRule rule = new ConfigFileMustBePresentRule();
        assertFalse(rule.execute(propertiesWithoutConfigFileLocation()));
    }
    
    @Test
    public void shouldFailIfThereIsNoConfigFileWhereTheSystemPropertyPoints() throws IOException {
        ConfigFileMustBePresentRule rule = new ConfigFileMustBePresentRule();
        File tempFile = ServerTestUtils.createTempPropertyFile();
        tempFile.delete();
        
        assertFalse(rule.execute(propertiesWithConfigFileLocation(tempFile)));
    }

    @Test
    public void shouldPassIfThereIsAConfigFileWhereTheSystemPropertyPoints() throws IOException {
        File propertyFile = ServerTestUtils.createTempPropertyFile();
        ServerTestUtils.writePropertyToFile("org.neo4j.database.location", "/tmp/foo.db", propertyFile);
        
        ConfigFileMustBePresentRule rule = new ConfigFileMustBePresentRule();
        assertTrue(rule.execute(propertiesWithConfigFileLocation(propertyFile)));
    }
    
    private Properties propertiesWithoutConfigFileLocation() {
        System.clearProperty(NeoServer.NEO_CONFIGDIR_PROPERTY);
        return System.getProperties();
    }
    
    private Properties propertiesWithConfigFileLocation(File propertyFile) {
        System.setProperty(NeoServer.NEO_CONFIGDIR_PROPERTY, propertyFile.getAbsolutePath());
        return System.getProperties();
    }
}
