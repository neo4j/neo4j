package org.neo4j.server.startup.healthcheck;

@SuppressWarnings("serial")
public class StartupHealthCheckFailedException extends RuntimeException {
    public StartupHealthCheckFailedException(String message) {
        super(message);
    }
    
    public StartupHealthCheckFailedException(String message, Object ... args) {
        super(String.format(message, args));
    }
}
