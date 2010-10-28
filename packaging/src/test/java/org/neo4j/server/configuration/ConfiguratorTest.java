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

import java.io.File;

import org.apache.commons.configuration.Configuration;
import org.junit.Test;

public class ConfiguratorTest {
    
    @Test
    public void shouldProvideAConfiguration() {
        Configuration config = new Configurator().configuration();
        assertNotNull(config);
    }

    @Test
    public void shouldUseSpecifiedConfigDir() {
        Configuration testConf = new Configurator(new File("src/test/resources/etc/neo-server")).configuration();

        final String EXPECTED_VALUE = "bar";
        assertEquals(EXPECTED_VALUE, testConf.getString("org.neo4j.foo"));

    }
}