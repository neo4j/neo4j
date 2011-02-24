/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.configuration.validation;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.commons.configuration.BaseConfiguration;
import org.junit.Test;
import org.neo4j.server.configuration.Configurator;


public class WebadminConfigurationRuleTest {
    
    private static final boolean theValidatorHasPassed = true;
    
    @Test(expected=RuleFailedException.class)
    public void shouldFailIfNoWebadminConfigSpecified() throws RuleFailedException {
        WebadminConfigurationRule rule = new WebadminConfigurationRule();
        BaseConfiguration emptyConfig = new BaseConfiguration();
        rule.validate(emptyConfig);
        assertFalse(theValidatorHasPassed);
    }
    
    @Test(expected=RuleFailedException.class)
    public void shouldFailIfOnlyRestApiKeySpecified() throws RuleFailedException {
        WebadminConfigurationRule rule = new WebadminConfigurationRule();
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(Configurator.DATA_API_PATH_PROPERTY_KEY, "http://localhost:7474/db/data");
        rule.validate(config);
        assertFalse(theValidatorHasPassed);
    }
    
    @Test(expected=RuleFailedException.class)
    public void shouldFailIfOnlyAdminApiKeySpecified() throws RuleFailedException {
        WebadminConfigurationRule rule = new WebadminConfigurationRule();
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(Configurator.MANAGEMENT_PATH_PROPERTY_KEY, "http://localhost:7474/db/manage");
        rule.validate(config);
        assertFalse(theValidatorHasPassed);
    }
    
    @Test
    public void shouldAllowAbsoluteUris() throws RuleFailedException {
        WebadminConfigurationRule rule = new WebadminConfigurationRule();
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(Configurator.DATA_API_PATH_PROPERTY_KEY, "http://localhost:7474/db/data");
        config.addProperty(Configurator.MANAGEMENT_PATH_PROPERTY_KEY, "http://localhost:7474/db/manage");
        rule.validate(config);
        assertTrue(theValidatorHasPassed);
    }
    
    @Test
    public void shouldAllowRelativeUris() throws RuleFailedException {
        WebadminConfigurationRule rule = new WebadminConfigurationRule();
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(Configurator.DATA_API_PATH_PROPERTY_KEY, "/db/data");
        config.addProperty(Configurator.MANAGEMENT_PATH_PROPERTY_KEY, "/db/manage");
        rule.validate(config);
        assertTrue(theValidatorHasPassed);
    }
    
    @Test
    public void shouldNormaliseUris() throws RuleFailedException {
        WebadminConfigurationRule rule = new WebadminConfigurationRule();
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(Configurator.DATA_API_PATH_PROPERTY_KEY, "http://localhost:7474///db///data///");
        config.addProperty(Configurator.MANAGEMENT_PATH_PROPERTY_KEY, "http://localhost:7474////db///manage");
        rule.validate(config);
        
        
        assertThat((String)config.getProperty(Configurator.MANAGEMENT_PATH_PROPERTY_KEY), not(containsString("///")));
        assertFalse(((String)config.getProperty(Configurator.MANAGEMENT_PATH_PROPERTY_KEY)).endsWith("//"));
        assertFalse(((String)config.getProperty(Configurator.MANAGEMENT_PATH_PROPERTY_KEY)).endsWith("/"));
        
        System.out.println(config.getProperty(Configurator.DATA_API_PATH_PROPERTY_KEY));
        
        assertThat((String)config.getProperty(Configurator.DATA_API_PATH_PROPERTY_KEY), not(containsString("///")));
        assertFalse(((String)config.getProperty(Configurator.DATA_API_PATH_PROPERTY_KEY)).endsWith("//"));
        assertFalse(((String)config.getProperty(Configurator.DATA_API_PATH_PROPERTY_KEY)).endsWith("/"));
    }
}
