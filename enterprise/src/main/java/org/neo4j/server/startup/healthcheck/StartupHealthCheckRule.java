package org.neo4j.server.startup.healthcheck;

import java.util.Properties;

public interface StartupHealthCheckRule {
    public boolean execute(Properties properties);
    public String getMessage();
}
