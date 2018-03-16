/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.Pair;

public class CoordinateReferenceSystem implements CRS
{
    public static final CoordinateReferenceSystem Cartesian = new CoordinateReferenceSystem( "cartesian", CRSTable.SR_ORG, 7203, 2, false );
    public static final CoordinateReferenceSystem Cartesian_3D = new CoordinateReferenceSystem( "cartesian-3D", CRSTable.SR_ORG, 9157, 3, false );
    public static final CoordinateReferenceSystem WGS84 = new CoordinateReferenceSystem( "WGS-84", CRSTable.EPSG, 4326, 2, true );
    public static final CoordinateReferenceSystem WGS84_3D = new CoordinateReferenceSystem( "WGS-84-3D", CRSTable.EPSG, 4979, 3, true );

    private static final CoordinateReferenceSystem[] TYPES = new CoordinateReferenceSystem[]{Cartesian, Cartesian_3D, WGS84, WGS84_3D};
    private static final Map<String,CoordinateReferenceSystem> all_by_name = new HashMap<>( TYPES.length );
    private static final Map<String,CoordinateReferenceSystem> all_by_href = new HashMap<>( TYPES.length );

    static
    {
        for ( CoordinateReferenceSystem crs : TYPES )
        {
            all_by_name.put( crs.name.toLowerCase(), crs );
            all_by_href.put( crs.href.toLowerCase(), crs );
        }
    }

    public static Iterator<CoordinateReferenceSystem> all()
    {
        return Iterators.iterator( TYPES );
    }

    public static CoordinateReferenceSystem get( int tableId, int code )
    {
        CRSTable table = CRSTable.find( tableId );
        String href = table.href( code );
        if ( all_by_href.containsKey( href.toLowerCase() ) )
        {
            return all_by_href.get( href.toLowerCase() );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown coordinate reference system: " + tableId + "-" + code );
        }
    }

    public static CoordinateReferenceSystem get( CRS crs )
    {
        Objects.requireNonNull( crs );
        return get( crs.getHref() );
    }

    public static CoordinateReferenceSystem byName( String name )
    {
        if ( all_by_name.containsKey( name.toLowerCase() ) )
        {
            return all_by_name.get( name.toLowerCase() );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown coordinate reference system: " + name );
        }
    }

    public static CoordinateReferenceSystem get( String href )
    {
        if ( all_by_href.containsKey( href.toLowerCase() ) )
        {
            return all_by_href.get( href.toLowerCase() );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown coordinate reference system: " + href );
        }
    }

    public static CoordinateReferenceSystem get( int code )
    {
        for ( CRSTable table : CRSTable.values() )
        {
            String href = table.href( code );
            if ( all_by_href.containsKey( href.toLowerCase() ) )
            {
                return all_by_href.get( href.toLowerCase() );
            }
        }
        throw new IllegalArgumentException( "Unknown coordinate reference system code: " + code );
    }

    private final String name;
    private final CRSTable table;
    private final int code;
    private final String href;
    private final int dimension;
    private final boolean geographic;
    private final Pair<double[],double[]> indexEnvelope;
    private final CRSCalculator calculator;

    private CoordinateReferenceSystem( String name, CRSTable table, int code, int dimension, boolean geographic )
    {
        this.name = name;
        this.table = table;
        this.code = code;
        this.href = table.href( code );
        this.dimension = dimension;
        this.geographic = geographic;
        this.indexEnvelope = envelopeFromCRS( dimension, geographic, -1000000, 1000000 );
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

    public Pair<double[],double[]> getIndexEnvelope()
    {
        return indexEnvelope;
    }

    private static Pair<double[],double[]> envelopeFromCRS( int dimension, boolean geographic, double minCartesian, double maxCartesian )
    {
        assert dimension >= 2;
        double[] min = new double[dimension];
        double[] max = new double[dimension];
        int cartesianStartIndex = 0;
        if ( geographic )
        {
            // Geographic CRS default to extent of the earth in degrees
            min[0] = -180.0;
            max[0] = 180.0;
            min[1] = -90.0;
            max[1] = 90.0;
            cartesianStartIndex = 2;    // if geographic index has higher than 2D, then other dimensions are cartesian
        }
        for ( int i = cartesianStartIndex; i < dimension; i++ )
        {
            min[i] = minCartesian;
            max[i] = maxCartesian;
        }
        return Pair.of( min, max );
    }
}
