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
package org.neo4j.kernel.configuration;

import org.junit.Test;

import org.neo4j.graphdb.config.Setting;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.setting;

public class GroupConfigTest
{
    @Test
    public void shouldProvideNiceSetMechanism()
    {
        assertThat( connector(0).enabled.name(), equalTo( "dbms.connector.0.enabled" ) );
    }

    static ConnectorExample connector( int key )
    {
        return new ConnectorExample( Integer.toString(key) );
    }

    @Group( "dbms.connector" )
    static class ConnectorExample
    {
        public final Setting<Boolean> enabled;
        public final Setting<String> name;

        private final GroupSettingSupport group;

        ConnectorExample( String key )
        {
            group = new GroupSettingSupport( ConnectorExample.class, key );
            this.enabled = group.scope( setting( "enabled", BOOLEAN, FALSE ) );
            this.name = group.scope( setting( "name", STRING, "Bob Dylan" ) );
        }
    }
}
