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

package org.neo4j.server.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.commons.configuration.Configuration;

import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class ConfiguratorTest {

    private static final String TEST_COMPONENT = "test-component";

    @Test
    public void shouldProvideAConfiguration() {
       Configuration config = new Configurator().getConfigurationFor(TEST_COMPONENT);
       assertNotNull(config);
    }    
   
    @Test
    public void shouldUseSpecifiedConfigDir() {
        Configuration testConf = new Configurator().getConfigurationFor( TEST_COMPONENT );

        final String EXPECTED_VALUE_OF_FOO = "a useful value";
        assertEquals(EXPECTED_VALUE_OF_FOO, testConf.getString( "foo" ));
        
    }
}