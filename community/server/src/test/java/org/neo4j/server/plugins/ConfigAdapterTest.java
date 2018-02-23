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
package org.neo4j.server.plugins;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.configuration.ServerSettings;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigAdapterTest
{
    @Test
    void shouldGetDefaultPropertyByKey() throws Exception
    {
        // GIVEN
        Config config = Config.defaults();
        ConfigAdapter wrappingConfiguration = new ConfigAdapter( config );

        // WHEN
        final Object propertyValue = wrappingConfiguration.getProperty( ServerSettings.rest_api_path.name() );

        // THEN
        assertEquals( new URI( ServerSettings.rest_api_path.getDefaultValue() ), propertyValue );
    }

    @Test
    void shouldGetPropertyInRightFormat() throws Exception
    {
        // GIVEN
        Config config = Config.defaults();
        ConfigAdapter wrappingConfiguration = new ConfigAdapter( config );

        // WHEN
        wrappingConfiguration
                .setProperty( ServerSettings.rest_api_path.name(), "http://localhost:7474///db///data///" );
        final Object dataPath = wrappingConfiguration.getProperty( ServerSettings.rest_api_path.name() );

        // THEN
        assertEquals( new URI( ServerSettings.rest_api_path.getDefaultValue() ), dataPath );
    }

    @Test
    void shouldContainAllKeysOfPropertiesWithDefaultOrUserDefinedValues()
    {
        // GIVEN

        Config config = Config.defaults();
        ConfigAdapter wrappingConfiguration = new ConfigAdapter( config );

        // THEN
        assertTrue( wrappingConfiguration.getKeys().hasNext() );
    }

    @Test
    void shouldAbleToAccessRegisteredPropertyByName()
    {
        Config config = Config.defaults();
        ConfigAdapter wrappingConfiguration = new ConfigAdapter( config );

        assertEquals( Duration.ofSeconds( 60 ),
                wrappingConfiguration.getProperty( ServerSettings.transaction_idle_timeout.name() ) );
    }

    @Test
    void shouldAbleToAccessNonRegisteredPropertyByName()
    {
        Config config = Config.defaults( ServerSettings.transaction_idle_timeout, "600ms" );
        ConfigAdapter wrappingConfiguration = new ConfigAdapter( config );

        assertEquals( Duration.ofMillis( 600 ),
                wrappingConfiguration.getProperty( ServerSettings.transaction_idle_timeout.name() ) );
    }
}
