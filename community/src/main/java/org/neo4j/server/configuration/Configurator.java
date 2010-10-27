package org.neo4j.server.configuration;

import java.util.HashMap;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.SystemConfiguration;

public class Configurator {
   
    private static HashMap<String, Configuration> config = new HashMap<String, Configuration>();
    
    public static Configuration getConfigurationFor(String componentName) {
        Configuration configuration = config.get(componentName);
        return configuration == null ? new SystemConfiguration() : configuration;
    }

    public static void set(String componentName, Configuration c) {
        config.put(componentName, c);
    }
}
