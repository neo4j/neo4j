/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.configuration;

import org.junit.Test;

import java.net.URI;
import java.util.HashMap;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.web.ServerInternalSettings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ConfigWrappingConfigurationTest
{

    @Test
    public void shouldGetDefaultPropertyByKey() throws Exception
    {
        // GIVEN
        Config config = new Config( new HashMap<String,String>(), BaseServerConfigLoader.getDefaultSettingsClasses() );
        ConfigWrappingConfiguration wrappingConfiguration = new ConfigWrappingConfiguration( config );

        // WHEN
        final Object propertyValue = wrappingConfiguration.getProperty( ServerInternalSettings.rest_api_path.name() );

        // THEN
        assertEquals( new URI( ServerInternalSettings.rest_api_path.getDefaultValue() ), propertyValue );
    }

    @Test
    public void shouldGetPropertyInRightFormat() throws Exception
    {
        // GIVEN
        Config config = new Config( new HashMap<String,String>(), BaseServerConfigLoader.getDefaultSettingsClasses() );
        ConfigWrappingConfiguration wrappingConfiguration = new ConfigWrappingConfiguration( config );

        // WHEN
        wrappingConfiguration
                .setProperty( ServerInternalSettings.rest_api_path.name(), "http://localhost:7474///db///data///" );
        final Object dataPath = wrappingConfiguration.getProperty( ServerInternalSettings.rest_api_path.name() );

        // THEN
        assertEquals( new URI( ServerInternalSettings.rest_api_path.getDefaultValue() ), dataPath );
    }

    @Test
    public void shouldContainAllKeysOfPropertiesWithDefaultOrUserDefinedValues() throws Exception
    {
        // GIVEN

        Config config = new Config( new HashMap<String,String>(), BaseServerConfigLoader.getDefaultSettingsClasses() );
        ConfigWrappingConfiguration wrappingConfiguration = new ConfigWrappingConfiguration( config );

        // THEN
        assertTrue( wrappingConfiguration.getKeys().hasNext() );
    }

    @Test
    public void shouldAbleToAccessRegisteredPropertyByName()
    {
        Config config = new Config( new HashMap<String,String>(), BaseServerConfigLoader.getDefaultSettingsClasses() );
        ConfigWrappingConfiguration wrappingConfiguration = new ConfigWrappingConfiguration( config );

        assertEquals( 60000L, wrappingConfiguration.getProperty( ServerSettings.transaction_timeout.name() ) );
    }

    @Test
    public void shouldAbleToAccessNonRegisteredPropertyByName()
    {
        Config config = new Config( stringMap( ServerSettings.transaction_timeout.name(), "600" ) );
        ConfigWrappingConfiguration wrappingConfiguration = new ConfigWrappingConfiguration( config );

        assertEquals( "600", wrappingConfiguration.getProperty( ServerSettings.transaction_timeout.name() ) );
    }
}
