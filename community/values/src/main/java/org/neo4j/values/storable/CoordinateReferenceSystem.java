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
import java.util.Map;
import java.util.Objects;

import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.helpers.collection.Pair;

import static java.lang.Math.asin;
import static java.lang.Math.atan2;
import static java.lang.Math.cos;
import static java.lang.Math.pow;
import static java.lang.Math.sin;
import static java.lang.Math.sqrt;
import static java.lang.Math.toDegrees;
import static java.lang.Math.toRadians;

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
            all_by_name.put( crs.name, crs );
            all_by_href.put( crs.href, crs );
        }
    }

    public static CoordinateReferenceSystem get( int tableId, int code )
    {
        CRSTable table = CRSTable.find( tableId );
        String href = table.href( code );
        if ( all_by_href.containsKey( href ) )
        {
            return all_by_href.get( href );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown Coordinate Reference System: " + tableId + "-" + code );
        }
    }

    public static CoordinateReferenceSystem get( CRS crs )
    {
        Objects.requireNonNull( crs );
        return get( crs.getHref() );
    }

    public static CoordinateReferenceSystem byName( String name )
    {
        if ( all_by_name.containsKey( name ) )
        {
            return all_by_name.get( name );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown Coordinate Reference System: " + name );
        }
    }

    public static CoordinateReferenceSystem get( String href )
    {
        if ( all_by_href.containsKey( href ) )
        {
            return all_by_href.get( href );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown Coordinate Reference System: " + href );
        }
    }

    public static CoordinateReferenceSystem get( int code )
    {
        if ( WGS84.code == code )
        {
            return WGS84;
        }
        else if ( Cartesian.code == code )
        {
            return Cartesian;
        }
        else
        {
            throw new IllegalArgumentException( "Unknown CRS code: " + code );
        }
    }

    private final String name;
    private final CRSTable table;
    private final int code;
    private final String href;
    private final int dimension;
    private final boolean geographic;
    private final Pair<double[],double[]> indexEnvelope;
    private final Calculator calculator;

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
            this.calculator = new GeographicCalculator( dimension );
        }
        else
        {
            this.calculator = new CartesianCalculator( dimension );
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

    public CoordinateReferenceSystem.Calculator getCalculator()
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

    public interface Calculator
    {
        double distance( PointValue p1, PointValue p2 );

        Pair<PointValue,PointValue> boundingBox( PointValue center, double distance );
    }

    private static double pythagoras( double[] a, double[] b )
    {
        double sqrSum = 0.0;
        for ( int i = 0; i < a.length; i++ )
        {
            double diff = a[i] - b[i];
            sqrSum += diff * diff;
        }
        return sqrt( sqrSum );
    }

    private static class CartesianCalculator implements Calculator
    {
        int dimension;

        CartesianCalculator( int dimension )
        {
            this.dimension = dimension;
        }

        @Override
        public double distance( PointValue p1, PointValue p2 )
        {
            assert p1.getCoordinateReferenceSystem().dimension == dimension;
            assert p2.getCoordinateReferenceSystem().dimension == dimension;
            return pythagoras( p1.coordinate(), p2.coordinate() );
        }

        @Override
        public Pair<PointValue,PointValue> boundingBox( PointValue center, double distance )
        {
            assert center.getCoordinateReferenceSystem().dimension == dimension;
            double[] coordinates = center.coordinate();
            double[] min = new double[dimension];
            double[] max = new double[dimension];
            for ( int i = 0; i < dimension; i++ )
            {
                min[i] = coordinates[i] - distance;
                max[i] = coordinates[i] + distance;
            }
            CoordinateReferenceSystem crs = center.getCoordinateReferenceSystem();
            return Pair.of( Values.pointValue( crs, min ), Values.pointValue( crs, max ) );
        }
    }

    private static class GeographicCalculator implements Calculator
    {
        private static final double EARTH_RADIUS_METERS = 6378140.0;
        private static final double EXTENSION_FACTOR = 1.0001;
        int dimension;

        GeographicCalculator( int dimension )
        {
            this.dimension = dimension;
        }

        @Override
        public double distance( PointValue p1, PointValue p2 )
        {
            assert p1.getCoordinateReferenceSystem().dimension == dimension;
            assert p2.getCoordinateReferenceSystem().dimension == dimension;
            double[] c1Coord = p1.coordinate();
            double[] c2Coord = p2.coordinate();
            double[] c1 = new double[]{toRadians( c1Coord[0] ), toRadians( c1Coord[1] )};
            double[] c2 = new double[]{toRadians( c2Coord[0] ), toRadians( c2Coord[1] )};
            double dx = c2[0] - c1[0];
            double dy = c2[1] - c1[1];
            double alpha = pow( sin( dy / 2 ), 2.0 ) + cos( c1[1] ) * cos( c2[1] ) * pow( sin( dx / 2.0 ), 2.0 );
            double greatCircleDistance = 2.0 * atan2( sqrt( alpha ), sqrt( 1 - alpha ) );
            double distance2D = EARTH_RADIUS_METERS * greatCircleDistance;
            if ( dimension > 2 )
            {
                double[] a = new double[dimension - 1];
                double[] b = new double[dimension - 1];
                a[0] = distance2D;
                b[0] = 0.0;
                for ( int i = 1; i < dimension - 1; i++ )
                {
                    a[i] = 0.0;
                    b[i] = c1Coord[i + 1] - c2Coord[i + 1];
                }
                return pythagoras( a, b );
            }
            else
            {
                return distance2D;
            }
        }

        @Override
        // http://janmatuschek.de/LatitudeLongitudeBoundingCoordinates
        // But calculating in degrees instead of radians to avoid rounding errors
        public Pair<PointValue,PointValue> boundingBox( PointValue center, double distance )
        {
            if ( distance == 0.0 )
            {
                return Pair.of( center, center );
            }

            // Extend the distance slightly to assure that all relevant points lies inside the bounding box,
            // with rounding errors taken into account
            double extended_distance = distance * EXTENSION_FACTOR;

            CoordinateReferenceSystem crs = center.getCoordinateReferenceSystem();
            double lat = center.coordinate()[1];
            double lon = center.coordinate()[0];

            double r = extended_distance / EARTH_RADIUS_METERS;

            double lat_min = lat - toDegrees( r );
            double lat_max = lat + toDegrees( r );

            // If your query circle includes one of the poles
            if ( lat_max >= 90 )
            {
                return boundingBoxOf( -180, 180, lat_min, 90, center, distance );
            }
            else if ( lat_min <= -90 )
            {
                return boundingBoxOf( -180, 180, -90, lat_max, center, distance );
            }
            else
            {
                double delta_lon = toDegrees( asin( sin( r ) / cos( toRadians( lat ) ) ) );
                double lon_min = lon - delta_lon;
                double lon_max = lon + delta_lon;

                // If you query circle wraps around the dateline
                // Large rectangle covering all longitudes
                // TODO implement two rectangle solution instead
                if ( lon_min < -180 || lon_max > 180 )
                {
                    return boundingBoxOf( -180, 180, lat_min, lat_max, center, distance );
                }
                else
                {
                    return boundingBoxOf( lon_min, lon_max, lat_min, lat_max, center, distance );
                }
            }
        }

        private Pair<PointValue,PointValue> boundingBoxOf( double minLon, double maxLon, double minLat, double maxLat, PointValue center, double distance )
        {
            CoordinateReferenceSystem crs = center.getCoordinateReferenceSystem();
            int dimension = center.getCoordinateReferenceSystem().getDimension();
            double[] min = new double[dimension];
            double[] max = new double[dimension];
            min[0] = minLon;
            min[1] = minLat;
            max[0] = maxLon;
            max[1] = maxLat;
            if ( dimension > 2 )
            {
                double[] coordinates = center.coordinate();
                for ( int i = 2; i < dimension; i++ )
                {
                    min[i] = coordinates[i] - distance;
                    max[i] = coordinates[i] + distance;
                }
            }
            return Pair.of( Values.pointValue( crs, min ), Values.pointValue( crs, max ) );
        }
    }
}
