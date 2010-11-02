package org.neo4j.server.configuration.validation;

import org.apache.commons.configuration.Configuration;

public class DatabaseLocationMustBeSpecifiedRule implements ValidationRule {
    private static final String ORG_NEO4J_DATABASE_LOCATION = "org.neo4j.database.location";

    public void validate(Configuration configuration) throws RuleFailedException {
        String dbLocation = configuration.getString(ORG_NEO4J_DATABASE_LOCATION);
        if(dbLocation == null || dbLocation.length() < 1) {
            throw new RuleFailedException("The key [%s] is missing from the Neo Server configuration.", ORG_NEO4J_DATABASE_LOCATION);
        }
    }

}
