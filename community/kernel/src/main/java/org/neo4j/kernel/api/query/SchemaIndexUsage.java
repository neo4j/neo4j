/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.query;

import java.util.HashMap;
import java.util.Map;

class SchemaIndexUsage extends IndexUsage
{
    private final String label;
    private final String[] propertyKeys;

    SchemaIndexUsage( String identifier, String label, String[] propertyKeys )
    {
        super( identifier );
        this.label = label;
        this.propertyKeys = propertyKeys;
    }

    public Map<String,String> asMap()
    {
        Map<String,String> map = new HashMap<>();
        map.put( "indexType", "SCHEMA INDEX" );
        map.put( "entityType", "NODE" );
        map.put( "identifier", identifier );
        map.put( "label", label );
        for ( int i = 0; i < propertyKeys.length; i++ )
        {
            String key = (propertyKeys.length > 1) ? "propertyKey_" + i : "propertyKey";
            map.put( key, propertyKeys[i] );
        }
        return map;
    }
}
