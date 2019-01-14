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
package org.neo4j.values.storable;

public enum CRSTable
{
    CUSTOM( "custom", 0 ),
    EPSG( "epsg", 1 ),
    SR_ORG( "sr-org", 2 );

    private static final CRSTable[] TYPES = CRSTable.values();

    private final String prefix;

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

    private final String name;
    private final int tableId;

    CRSTable( String name, int tableId )
    {
        assert lowerCase( name );
        this.name = name;
        this.tableId = tableId;
        this.prefix = tableId == 0 ? "crs://" + name + "/" : "http://spatialreference.org/ref/" + name + "/";
    }

    public String href( int code )
    {
        return prefix + code + "/";
    }

    private boolean lowerCase( String string )
    {
        return string.toLowerCase().equals( string );
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
