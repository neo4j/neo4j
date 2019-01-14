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

import org.neo4j.configuration.Description;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.options;
import static org.neo4j.kernel.configuration.Settings.setting;

@Group( "dbms.connector" )
public class Connector
{
    @Description( "Enable this connector" )
    public final Setting<Boolean> enabled;

    @Description( "Connector type. You should always set this to the connector type you want" )
    public final Setting<ConnectorType> type;

    // Note: Be careful about adding things here that does not apply to all connectors,
    //       consider future options like non-tcp transports, making `address` a bad choice
    //       as a setting that applies to every connector, for instance.

    public final GroupSettingSupport group;

    /**
     * Deprecated, please use other constructor. This constructor will be removed in 4.0.
     *
     * @param key of connector
     * @param typeDefault unused parameter
     */
    @Deprecated
    public Connector( String key, @SuppressWarnings( "UnusedParameters" ) String typeDefault )
    {
        this( key );
    }

    public Connector( String key )
    {
        group = new GroupSettingSupport( Connector.class, key );
        enabled = group.scope( setting( "enabled", BOOLEAN, "false" ) );
        type = group.scope( setting( "type", options( ConnectorType.class ), NO_DEFAULT ) );
    }

    public enum ConnectorType
    {
        BOLT, HTTP
    }

    public String key()
    {
        return group.groupKey;
    }
}
