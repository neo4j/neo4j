package org.neo4j.server.configuration.validation;

import java.util.Iterator;

import org.apache.commons.configuration.Configuration;
import org.neo4j.server.logging.Logger;

public class DuplicateKeyRule implements ValidationRule {

    public static Logger log = Logger.getLogger(DuplicateKeyRule.class);

    @SuppressWarnings("unchecked")
    public void validate(Configuration existingConfiguration, Configuration additionalConfiguration) {
        Iterator<String> existingKeys = existingConfiguration.getKeys();
        while (existingKeys.hasNext()) {
            Iterator<String> additionalKeys = additionalConfiguration.getKeys();
            while (additionalKeys.hasNext()) {
                String existingKey = existingKeys.next();
                String additionalKey = additionalKeys.next();
                if (existingKey.equals(additionalKey)) {
                    log.info("Duplicate key [%s] found in configuration files", existingKey);
                    if (!existingConfiguration.getString(existingKey).equals(additionalConfiguration.getString(additionalKey))) {
                        throw new RuleFailedException(String.format("Key [%s] has existing value [%s], cannot overwrite it with new value [%s]", existingKey,
                                existingConfiguration.getString(existingKey), additionalConfiguration.getString(additionalKey)));

                    }
                }
            }
        }
    }
}
