package org.neo4j.server.configuration.validation;

import java.util.ArrayList;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

public class Validator {

    public static Logger log = Logger.getLogger(Validator.class);
    private final ArrayList<ValidationRule> validationRules = new ArrayList<ValidationRule>();
    
    public Validator(ValidationRule ... rules) {
        if(rules == null) {
            return;
        }
        for(ValidationRule r : rules) {
            this.validationRules.add(r);
        }
    }
    
    public boolean validate(Configuration existingConfiguration, Configuration additionalConfiguration) {
        for(ValidationRule vr : validationRules) {
            try {
                vr.validate(existingConfiguration, additionalConfiguration);
            } catch(RuntimeException re) {
                log.debug(re.getMessage());
                return false;
            }
        }
        return true;
    }
}
