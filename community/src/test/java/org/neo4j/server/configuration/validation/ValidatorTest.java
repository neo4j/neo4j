package org.neo4j.server.configuration.validation;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.junit.Test;


public class ValidatorTest {
    @Test
    public void successfulValidationReturnsPositively() {
        BaseConfiguration configA = new BaseConfiguration();
        configA.addProperty("jim", "webber");
        
        BaseConfiguration configB = new BaseConfiguration();
        configB.addProperty("andreas", "kollegger");
        
        CompositeConfiguration cc = new CompositeConfiguration();
        cc.addConfiguration(configA);
        
        Validator v = new Validator();
        v.validate(cc, configB);

    }
    
    @Test
    public void unsuccessfulValidationReturnsNegativelyAndLogs() {
        BaseConfiguration configA = new BaseConfiguration();
        configA.addProperty("jim", "webber");
        
        BaseConfiguration configB = new BaseConfiguration();
        configB.addProperty("jim", "kollegger");
        
        CompositeConfiguration cc = new CompositeConfiguration();
        cc.addConfiguration(configA);
        
        Validator v = new Validator();
        v.validate(cc, configB);
    }
}
