/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.perftest.enterprise.util;

import java.util.HashMap;
import java.util.Map;

public class DirectlyCorrelatedParameter
{
    public static Map<String, String> passOn( Configuration configuration, DirectlyCorrelatedParameter... parameters )
    {
        HashMap<String, String> map = new HashMap<String, String>();
        for ( DirectlyCorrelatedParameter parameter : parameters )
        {
            Setting<?> localSetting = parameter.localSetting;
            map.put( parameter.databaseSetting.name(), asString( configuration, localSetting ) );
        }
        return map;
    }

    public static DirectlyCorrelatedParameter param( org.neo4j.graphdb.config.Setting<?> databaseSetting,
                                                     Setting<?> localSetting )
    {
        return new DirectlyCorrelatedParameter( localSetting, databaseSetting );
    }

    private static <T> String asString( Configuration configuration, Setting<T> localSetting )
    {
        return localSetting.asString( configuration.get( localSetting ) );
    }

    private final Setting<?> localSetting;
    private final org.neo4j.graphdb.config.Setting<?> databaseSetting;

    DirectlyCorrelatedParameter( Setting<?> localSetting, org.neo4j.graphdb.config.Setting<?> databaseSetting )
    {
        this.localSetting = localSetting;
        this.databaseSetting = databaseSetting;
    }
}
