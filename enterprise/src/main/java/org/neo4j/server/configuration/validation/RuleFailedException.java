package org.neo4j.server.configuration.validation;

@SuppressWarnings("serial")
public class RuleFailedException extends RuntimeException {
    public RuleFailedException(String message) {
        super(message);
    }
}
