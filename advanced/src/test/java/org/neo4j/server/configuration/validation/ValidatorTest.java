/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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
