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

import java.util.Objects;

import org.neo4j.graphdb.spatial.CRS;

public class CoordinateReferenceSystem implements CRS
{
    public static final CoordinateReferenceSystem Cartesian = new CoordinateReferenceSystem( "cartesian", CRSTable.SR_ORG, 7203 );
    public static final CoordinateReferenceSystem WGS84 = new CoordinateReferenceSystem( "WGS-84", CRSTable.EPSG, 4326 );

    public static CoordinateReferenceSystem get( int tableId, int code )
    {
        CRSTable table = CRSTable.find( tableId );
        if ( tableId == CRSTable.SR_ORG.getTableId() && code == Cartesian.code )
        {
            return CoordinateReferenceSystem.Cartesian;
        }
        else if ( tableId == CRSTable.EPSG.getTableId() && code == WGS84.code )
        {
            return CoordinateReferenceSystem.WGS84;
        }
        else
        {
            // We expect this path only to get executed once we allow writing custom CRS's to the store
            return new CoordinateReferenceSystem( table.getName() + "-" + code, table, code );
        }
    }

    public static CoordinateReferenceSystem get( CRS crs )
    {
        Objects.requireNonNull( crs );
        return get(crs.getHref());
    }

    public static CoordinateReferenceSystem get( String href )
    {
        if ( CoordinateReferenceSystem.WGS84.href.equals( href ) )
        {
            return CoordinateReferenceSystem.WGS84;
        }
        else if ( CoordinateReferenceSystem.Cartesian.href.equals( href ) )
        {
            return CoordinateReferenceSystem.Cartesian;
        }
        else
        {
            throw new IllegalArgumentException( "Unknown CRS: " + href );
        }
    }

    private final String name;
    private final CRSTable table;
    private final int code;
    private final String href;

    CoordinateReferenceSystem( String name, CRSTable table, int code )
    {
        this.name = name;
        this.table = table;
        this.code = code;
        this.href = table.href( code );
    }

    @Override
    public int getCode()
    {
        return code;
    }

    @Override
    public String getType()
    {
        return name;
    }

    @Override
    public String getHref()
    {
        return href;
    }

    public String getName()
    {
        return name;
    }

    public CRSTable getTable()
    {
        return table;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        CoordinateReferenceSystem that = (CoordinateReferenceSystem) o;

        return href.equals( that.href );
    }

    @Override
    public int hashCode()
    {
        return href.hashCode();
    }
}
