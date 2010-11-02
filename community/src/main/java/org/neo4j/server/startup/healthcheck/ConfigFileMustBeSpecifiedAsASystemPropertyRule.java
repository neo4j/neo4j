package org.neo4j.server.startup.healthcheck;

import java.util.Properties;

import org.neo4j.server.NeoServer;

public class ConfigFileMustBeSpecifiedAsASystemPropertyRule implements StartupHealthCheckRule {

    private boolean ran = false;
    private boolean passed = false;

    public boolean execute(Properties properties) {
        String key = properties.getProperty(NeoServer.NEO_CONFIGDIR_PROPERTY);
        this.passed   = key != null;
        ran = true;
        return passed;
    }

    public String getMessage() {
        if(!ran) {
            return String.format("[%s] Healthcheck has not been run", this.getClass().getName());
        }
        
        if(passed) {
            return String.format("[%s] Passed healthcheck", this.getClass().getName());
        } else {
            return String.format("[%s] Failed healthcheck", this.getClass().getName());
        }
    }

}
