/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

import org.neo4j.configuration.ConfigValue;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.HttpConnector;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ServerSettingsTest
{
    @Test
    public void webServerThreadCountDefaultShouldBeDocumented()
    {
        Config config = Config.builder().withServerDefaults().build();

        String documentedDefaultValue =
                config.getConfigValues().entrySet().stream()
                        .filter( c -> c.getKey().equals( ServerSettings.webserver_max_threads.name() ) )
                        .map( Entry::getValue )
                        .findAny()
                        .orElseThrow( () -> new RuntimeException( "Setting not present!" ) )
                        .documentedDefaultValue()
                        .orElseThrow( () -> new RuntimeException( "Default value not present!" ) );

        assertEquals( "Number of available processors (max 500).", documentedDefaultValue );
    }

    @Test
    public void configValuesContainsConnectors()
    {
        Config config = Config.builder().withServerDefaults().build();

        List<String> connectorSettings = config.getConfigValues().entrySet().stream()
                .map( Entry::getKey )
                .filter( c -> c.startsWith( "dbms.connector" ) )
                .filter( c -> c.endsWith( ".enabled" ) )
                .collect( toList() );

        assertThat( connectorSettings, containsInAnyOrder( "dbms.connector.http.enabled",
                "dbms.connector.https.enabled",
                "dbms.connector.bolt.enabled" ) );
    }

    @Test
    public void connectorSettingHasItsOwnValues()
    {
        Config config = Config.builder().withServerDefaults()
                .withSetting( new HttpConnector( "http" ).address, "localhost:123" ).build();

        ConfigValue address = config.getConfigValues().entrySet().stream()
                .filter( c -> c.getKey().equals( "dbms.connector.http.address" ) )
                .map( Entry::getValue )
                .findAny()
                .orElseThrow( () -> new RuntimeException( "Setting not present!" ) );

        assertTrue( address.deprecated() );
        assertEquals( Optional.of( "dbms.connector.http.listen_address" ), address.replacement() );
    }
}
