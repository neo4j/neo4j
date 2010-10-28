package org.neo4j.server.configuration.validation;

import org.apache.commons.configuration.Configuration;

public interface ValidationRule {
    public void validate(Configuration existingConfiguration, Configuration additionalConfiguration) throws RuleFailedException;
}
