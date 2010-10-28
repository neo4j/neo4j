package org.neo4j.server.configuration;

public class InvalidServerConfigurationException extends RuntimeException {
    public InvalidServerConfigurationException(String message) {
        super(message);
    }
}
