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
package org.neo4j.configuration.connectors;

import org.neo4j.configuration.GroupSetting;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.graphdb.config.Setting;

//Not public API, will Connectors as a GroupSetting will be replaced by single connectors
public abstract class Connector extends GroupSetting
{
    public final Setting<Boolean> enabled = getBuilder( "enabled", SettingValueParsers.BOOL, false ).build();
    private static final String PREFIX = "dbms.connector";

    protected Connector( String name )
    {
        super( name );
    }

    @Override
    public String getPrefix()
    {
        return PREFIX;
    }
}
