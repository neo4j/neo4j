/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.configuration;

import org.junit.Test;

import java.util.List;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.collection.MapUtil;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.configuration.GroupSettingSupport.enumerate;
import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.setting;

public class GroupConfigTest
{
    @Test
    public void shouldProvideNiceSetMechanism() throws Throwable
    {
        assertThat( connector(0).enabled.name(), equalTo( "dbms.connector.0.enabled" ) );
    }

    @Test
    public void shouldProvideConvenientWayToEnumerateGroups() throws Throwable
    {
        // Given
        Config config = new Config( MapUtil.stringMap(
            connector( 0 ).enabled.name(), "true",

            connector( 1 ).enabled.name(), "false",
            connector( 1 ).name.name(), "Cat Stevens",

            connector( 3 ).enabled.name(), "false"
        ) );

        // When
        List<ConnectorExample> groups = config.view( enumerate( ConnectorExample.class ) )
                .map( ConnectorExample::new )
                .collect( toList() );

        // Then
        assertThat( groups.size(), equalTo( 3 ) );
        assertThat( config.get( groups.get( 0 ).enabled ), equalTo( true ));
        assertThat( config.get( groups.get( 0 ).name ), equalTo( "Bob Dylan" ));

        assertThat( config.get( groups.get( 1 ).enabled ), equalTo( false ));
        assertThat( config.get( groups.get( 1 ).name ), equalTo( "Cat Stevens" ));

        assertThat( config.get( groups.get( 2 ).enabled ), equalTo( false ));
        assertThat( config.get( groups.get( 2 ).name ), equalTo( "Bob Dylan" ));

    }

    static ConnectorExample connector( int key )
    {
        return new ConnectorExample( Integer.toString(key) );
    }

    @Group("dbms.connector")
    static class ConnectorExample
    {
        public final Setting<Boolean> enabled;
        public final Setting<String> name;

        private final GroupSettingSupport group;

        public ConnectorExample( String key )
        {
            group = new GroupSettingSupport( ConnectorExample.class, key );
            this.enabled = group.scope( setting( "enabled", BOOLEAN, "false" ) );
            this.name = group.scope( setting( "name", STRING, "Bob Dylan" ) );
        }
    }
}
