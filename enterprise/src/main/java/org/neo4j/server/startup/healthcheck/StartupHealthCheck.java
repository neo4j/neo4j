package org.neo4j.server.startup.healthcheck;

import java.util.Properties;

import org.neo4j.server.logging.Logger;


public class StartupHealthCheck {
    public static final Logger log = Logger.getLogger(StartupHealthCheck.class);
    
    private final StartupHealthCheckRule[] rules;

    public StartupHealthCheck(StartupHealthCheckRule ... rules) {
        this.rules = rules;
    }
    
    public boolean run() {
        if(rules == null || rules.length < 1) {
            return true;
        }
        
        Properties properties = System.getProperties();
        for(StartupHealthCheckRule r : rules) {
            if(!r.execute(properties)) {
                log.error(r.getMessage());
                return false;
            }
        }
        
        return true;
    }
}
