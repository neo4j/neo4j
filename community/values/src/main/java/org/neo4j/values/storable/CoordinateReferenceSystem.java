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

import java.util.Objects;

import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.values.utils.InvalidValuesArgumentException;
import org.neo4j.helpers.collection.Iterables;

public class CoordinateReferenceSystem implements CRS
{
    public static final CoordinateReferenceSystem Cartesian = new CoordinateReferenceSystem( "cartesian", CRSTable.SR_ORG, 7203, 2, false );
    public static final CoordinateReferenceSystem Cartesian_3D = new CoordinateReferenceSystem( "cartesian-3d", CRSTable.SR_ORG, 9157, 3, false );
    public static final CoordinateReferenceSystem WGS84 = new CoordinateReferenceSystem( "wgs-84", CRSTable.EPSG, 4326, 2, true );
    public static final CoordinateReferenceSystem WGS84_3D = new CoordinateReferenceSystem( "wgs-84-3d", CRSTable.EPSG, 4979, 3, true );

    private static final CoordinateReferenceSystem[] TYPES = new CoordinateReferenceSystem[]{Cartesian, Cartesian_3D, WGS84, WGS84_3D};

    public static Iterable<CoordinateReferenceSystem> all()
    {
        return Iterables.asIterable( TYPES );
    }

    public static CoordinateReferenceSystem get( int tableId, int code )
    {
        CRSTable table = CRSTable.find( tableId );
        for ( CoordinateReferenceSystem type : TYPES )
        {
            if ( type.table == table && type.code == code )
            {
                return type;
            }
        }
        throw new InvalidValuesArgumentException( "Unknown coordinate reference system: " + tableId + "-" + code );
    }

    public static CoordinateReferenceSystem get( CRS crs )
    {
        Objects.requireNonNull( crs );
        return get( crs.getHref() );
    }

    public static CoordinateReferenceSystem byName( String name )
    {
        for ( CoordinateReferenceSystem type : TYPES )
        {
            if ( type.name.equals( name.toLowerCase() ) )
            {
                return type;
            }
        }

        throw new InvalidValuesArgumentException( "Unknown coordinate reference system: " + name );
    }

    public static CoordinateReferenceSystem get( String href )
    {
        for ( CoordinateReferenceSystem type : TYPES )
        {
            if ( type.href.equals( href ) )
            {
                return type;
            }
        }
        throw new InvalidValuesArgumentException( "Unknown coordinate reference system: " + href );
    }

    public static CoordinateReferenceSystem get( int code )
    {
        for ( CRSTable table : CRSTable.values() )
        {
            String href = table.href( code );
            for ( CoordinateReferenceSystem type : TYPES )
            {
                if ( type.href.equals( href ) )
                {
                    return type;
                }
            }
        }
        throw new InvalidValuesArgumentException( "Unknown coordinate reference system code: " + code );
    }

    private final String name;
    private final CRSTable table;
    private final int code;
    private final String href;
    private final int dimension;
    private final boolean geographic;
    private final CRSCalculator calculator;

    private CoordinateReferenceSystem( String name, CRSTable table, int code, int dimension, boolean geographic )
    {
        assert name.toLowerCase().equals( name );
        this.name = name;
        this.table = table;
        this.code = code;
        this.href = table.href( code );
        this.dimension = dimension;
        this.geographic = geographic;
        if ( geographic )
        {
            this.calculator = new CRSCalculator.GeographicCalculator( dimension );
        }
        else
        {
            this.calculator = new CRSCalculator.CartesianCalculator( dimension );
        }
    }

    @Override
    public String toString()
    {
        return name;
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

    public int getDimension()
    {
        return dimension;
    }

    public boolean isGeographic()
    {
        return geographic;
    }

    public CRSCalculator getCalculator()
    {
        return calculator;
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
