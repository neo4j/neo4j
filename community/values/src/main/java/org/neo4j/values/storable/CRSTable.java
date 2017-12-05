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
package org.neo4j.values.storable;

import java.util.HashMap;
import java.util.Map;

public enum CRSTable
{
    CUSTOM( "custom", 0 ),
    EPSG( "epsg", 1 ),
    SR_ORG( "sr-org", 2 );

    private static final CRSTable[] TYPES = CRSTable.values();
    private static final Map<String, CRSTable> all = new HashMap<>( TYPES.length );

    static
    {
        for ( CRSTable table : TYPES )
        {
            all.put( table.name, table );
        }
    }

    public static CRSTable find( int tableId )
    {
        if ( tableId < TYPES.length )
        {
            return TYPES[tableId];
        }
        else
        {
            throw new IllegalArgumentException( "No known Coordinate Reference System table: " + tableId );
        }
    }

    public static CRSTable find( String name )
    {
        CRSTable table = all.get( name );
        if ( table != null )
        {
            return table;
        }
        else
        {
            throw new IllegalArgumentException( "No known Coordinate Reference System table: " + name );
        }
    }

    private final String name;
    private final int tableId;

    CRSTable( String name, int tableId )
    {
        this.name = name;
        this.tableId = tableId;
    }

    public String href( int code )
    {
        if ( tableId == CUSTOM.tableId )
        {
            return "crs://" + name + "/" + code + "/";
        }
        else
        {
            return "http://spatialreference.org/ref/" + name + "/" + code + "/";
        }
    }

    public String getName()
    {
        return name;
    }

    public int getTableId()
    {
        return tableId;
    }
}
