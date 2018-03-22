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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.graphdb.spatial.Coordinate;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.utils.PrettyPrinter;
import org.neo4j.values.virtual.MapValue;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

public class PointValue extends ScalarValue implements Point, Comparable<PointValue>
{
    private CoordinateReferenceSystem crs;
    private double[] coordinate;

    PointValue( CoordinateReferenceSystem crs, double... coordinate )
    {
        this.crs = crs;
        this.coordinate = coordinate;
        for ( double c : coordinate )
        {
            if ( !Double.isFinite( c ) )
            {
                throw new IllegalArgumentException( "Cannot create a point with non-finite coordinate values: " + Arrays.toString(coordinate) );
            }
        }
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        writer.writePoint( getCoordinateReferenceSystem(), coordinate );
    }

    @Override
    public String prettyPrint()
    {
        PrettyPrinter prettyPrinter = new PrettyPrinter();
        this.writeTo( prettyPrinter );
        return prettyPrinter.value();
    }

    @Override
    public ValueGroup valueGroup()
    {
        return ValueGroup.GEOMETRY;
    }

    @Override
    public NumberType numberType()
    {
        return NumberType.NO_NUMBER;
    }

    @Override
    public boolean equals( Value other )
    {
        if ( other instanceof PointValue )
        {
            PointValue pv = (PointValue) other;
            return Arrays.equals( this.coordinate, pv.coordinate ) && this.getCoordinateReferenceSystem().equals( pv.getCoordinateReferenceSystem() );
        }
        return false;
    }

    public boolean equals( Point other )
    {
        if ( !other.getCRS().getHref().equals( this.getCRS().getHref() ) )
        {
            return false;
        }
        List<Double> otherCoordinate = other.getCoordinate().getCoordinate();
        if ( otherCoordinate.size() != this.coordinate.length )
        {
            return false;
        }
        for ( int i = 0; i < this.coordinate.length; i++ )
        {
            if ( otherCoordinate.get( i ) != this.coordinate[i] )
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean eq( Object other )
    {
        return other != null && ((other instanceof Value && equals( (Value) other )) || (other instanceof Point && equals( (Point) other )));
    }

    @Override
    public int compareTo( PointValue other )
    {
        int cmpCRS = this.crs.getCode() - other.crs.getCode();
        if ( cmpCRS != 0 )
        {
            return cmpCRS;
        }

        if ( this.coordinate.length > other.coordinate.length )
        {
            return 1;
        }
        else if ( this.coordinate.length < other.coordinate.length )
        {
            return -1;
        }

        for ( int i = 0; i < coordinate.length; i++ )
        {
            int cmpVal = Double.compare( this.coordinate[i], other.coordinate[i] );
            if ( cmpVal != 0 )
            {
                return cmpVal;
            }
        }
        return 0;
    }

    @Override
    int unsafeCompareTo( Value otherValue )
    {
        return compareTo( (PointValue) otherValue );
    }

    @Override
    Integer unsafeTernaryCompareTo( Value otherValue )
    {
        PointValue other = (PointValue) otherValue;

        if ( this.crs.getCode() != other.crs.getCode() || this.coordinate.length != other.coordinate.length )
        {
            return null;
        }

        int result = 0;
        for ( int i = 0; i < coordinate.length; i++ )
        {
            int cmpVal = Double.compare( this.coordinate[i], other.coordinate[i] );
            if ( cmpVal != 0 && cmpVal != result )
            {
                if ( (cmpVal < 0 && result > 0) || (cmpVal > 0 && result < 0) )
                {
                    return null;
                }
                result = cmpVal;
            }
        }

        return result;
    }

    @Override
    public Point asObjectCopy()
    {
        return this;
    }

    public CoordinateReferenceSystem getCoordinateReferenceSystem()
    {
        return crs;
    }

    /*
     * Consumers must not modify the returned array.
     */
    public double[] coordinate()
    {
        return this.coordinate;
    }

    @Override
    public int computeHash()
    {
        int result = 1;
        result = 31 * result + NumberValues.hash( crs.getCode() );
        result = 31 * result + NumberValues.hash( coordinate );
        return result;
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return mapper.mapPoint( this );
    }

    @Override
    public String toString()
    {
        return format( "Point{ %s, %s}", getCoordinateReferenceSystem().getName(), Arrays.toString( coordinate ) );
    }

    @Override
    public List<Coordinate> getCoordinates()
    {
        return singletonList( new Coordinate( coordinate ) );
    }

    @Override
    public CRS getCRS()
    {
        return crs;
    }

    /**
     * Checks if this point is greater than (or equal) to lower and smaller than (or equal) to upper.
     *
     * @param lower point this value should be greater than
     * @param includeLower governs if the lower comparison should be inclusive
     * @param upper point this value should be smaller than
     * @param includeUpper governs if the upper comparison should be inclusive
     * @return true if this value is within the described range
     */
    public boolean withinRange( PointValue lower, boolean includeLower, PointValue upper, boolean includeUpper )
    {
        if ( lower != null )
        {
            Integer compareLower = this.unsafeTernaryCompareTo( lower );
            if ( compareLower == null || compareLower < 0 || compareLower == 0 && !includeLower )
            {
                return false;
            }
        }
        if ( upper != null )
        {
            Integer compareUpper = this.unsafeTernaryCompareTo( upper );
            if ( compareUpper == null || compareUpper > 0 || compareUpper == 0 && !includeUpper )
            {
                return false;
            }
        }
        return true;
    }

    public static PointValue fromMap( MapValue map )
    {
        PointCSVHeaderInformation fields = new PointCSVHeaderInformation();
        for ( Map.Entry<String,AnyValue> entry : map.entrySet() )
        {
            fields.assign( entry.getKey().toLowerCase(), entry.getValue() );
        }
        return fromInputFields( fields );
    }

    public static PointValue parse( CharSequence text )
    {
        return PointValue.parse( text, null );
    }

    /**
     * Parses the given text into a PointValue. The information stated in the header is saved into the PointValue
     * unless it is overridden by the information in the text
     *
     * @param text the input text to be parsed into a PointValue
     * @param fieldsFromHeader must be a value obtained from {@link #parseHeaderInformation(CharSequence)} or null
     * @return a PointValue instance with information from the {@param fieldsFromHeader} and {@param text}
     */
    public static PointValue parse( CharSequence text, CSVHeaderInformation fieldsFromHeader )
    {
        PointCSVHeaderInformation fieldsFromData = parseHeaderInformation( text );
        if ( fieldsFromHeader != null )
        {
            // Merge InputFields: Data fields override header fields
            if ( !(fieldsFromHeader instanceof PointCSVHeaderInformation) )
            {
                throw new IllegalStateException( "Wrong header information type: " + fieldsFromHeader );
            }
            fieldsFromData.mergeWithHeader( (PointCSVHeaderInformation) fieldsFromHeader );
        }
        return fromInputFields( fieldsFromData );
    }

    public static PointCSVHeaderInformation parseHeaderInformation( CharSequence text )
    {
        PointCSVHeaderInformation fields = new PointCSVHeaderInformation();
        Value.parseHeaderInformation( text, "point", fields );
        return fields;
    }

    private static CoordinateReferenceSystem findSpecifiedCRS( PointCSVHeaderInformation fields )
    {
        String crsValue = fields.crs;
        int sridValue = fields.srid;
        if ( crsValue != null && sridValue != -1 )
        {
            throw new IllegalArgumentException( "Cannot specify both CRS and SRID" );
        }
        else if ( crsValue != null )
        {
            return CoordinateReferenceSystem.byName( crsValue );
        }
        else if ( sridValue != -1 )
        {
            return CoordinateReferenceSystem.get( sridValue );
        }
        else
        {
            return null;
        }
    }

    /**
     * This contains the logic to decide the default coordinate reference system based on the input fields
     */
    private static PointValue fromInputFields( PointCSVHeaderInformation fields )
    {
        CoordinateReferenceSystem crs = findSpecifiedCRS( fields );
        double[] coordinates;

        if ( fields.x != null && fields.y != null )
        {
            coordinates = fields.z != null ? new double[]{fields.x, fields.y, fields.z} : new double[]{fields.x, fields.y};
            if ( crs == null )
            {
                crs = coordinates.length == 3 ? CoordinateReferenceSystem.Cartesian_3D : CoordinateReferenceSystem.Cartesian;
            }
        }
        else if ( fields.latitude != null && fields.longitude != null )
        {
            if ( fields.z != null )
            {
                coordinates = new double[]{fields.longitude, fields.latitude, fields.z};
            }
            else if ( fields.height != null )
            {
                coordinates = new double[]{fields.longitude, fields.latitude, fields.height};
            }
            else
            {
                coordinates = new double[]{fields.longitude, fields.latitude};
            }
            if ( crs == null )
            {
                crs = coordinates.length == 3 ? CoordinateReferenceSystem.WGS84_3D : CoordinateReferenceSystem.WGS84;
            }
            if ( !crs.isGeographic() )
            {
                throw new IllegalArgumentException( "Geographic points does not support coordinate reference system: " + crs +
                        ". This is set either in the csv header or the actual data column" );
            }
        }
        else
        {
            if ( crs == CoordinateReferenceSystem.Cartesian )
            {
                throw new IllegalArgumentException( "A " + CoordinateReferenceSystem.Cartesian.getName() + " point must contain 'x' and 'y'" );
            }
            else if ( crs == CoordinateReferenceSystem.Cartesian_3D )
            {
                throw new IllegalArgumentException( "A " + CoordinateReferenceSystem.Cartesian_3D.getName() + " point must contain 'x', 'y' and 'z'" );
            }
            else if ( crs == CoordinateReferenceSystem.WGS84 )
            {
                throw new IllegalArgumentException( "A " + CoordinateReferenceSystem.WGS84.getName() + " point must contain 'latitude' and 'longitude'" );
            }
            else if ( crs == CoordinateReferenceSystem.WGS84_3D )
            {
                throw new IllegalArgumentException(
                        "A " + CoordinateReferenceSystem.WGS84_3D.getName() + " point must contain 'latitude', 'longitude' and 'height'" );
            }
            throw new IllegalArgumentException( "A point must contain either 'x' and 'y' or 'latitude' and 'longitude'" );
        }

        if ( crs.getDimension() != coordinates.length )
        {
            throw new IllegalArgumentException( "Cannot create point with " + crs.getDimension() + "D coordinate reference system and " + coordinates.length +
                    " coordinates. Please consider using equivalent " + coordinates.length + "D coordinate reference system" );
        }
        return Values.pointValue( crs, coordinates );
    }

    /**
     * For accessors from cypher.
     */
    public AnyValue get( String fieldName )
    {
        switch ( fieldName.toLowerCase() )
        {
        case "x":
            return getNthCoordinate( 0, fieldName, false );
        case "y":
            return getNthCoordinate( 1, fieldName, false );
        case "z":
            return getNthCoordinate( 2, fieldName, false );
        case "longitude":
            return getNthCoordinate( 0, fieldName, true );
        case "latitude":
            return getNthCoordinate( 1, fieldName, true );
        case "height":
            return getNthCoordinate( 2, fieldName, true );
        case "crs":
            return Values.stringValue( crs.toString() );
        case "srid":
            return Values.intValue( crs.getCode() );
        default:
            throw new IllegalArgumentException( "No such field: " + fieldName );
        }
    }

    private DoubleValue getNthCoordinate( int n, String fieldName, boolean onlyGeographic )
    {
        if ( onlyGeographic && !this.getCoordinateReferenceSystem().isGeographic() )
        {
            throw new IllegalArgumentException( "Field: " + fieldName + " is not available on cartesian point: " + this );
        }
        else if ( n >= this.coordinate().length )
        {
            throw new IllegalArgumentException( "Field: " + fieldName + " is not available on point: " + this );
        }
        else
        {
            return Values.doubleValue( coordinate[n] );
        }
    }

    private static class PointCSVHeaderInformation implements CSVHeaderInformation
    {
        private String crs;
        private Double x;
        private Double y;
        private Double z;
        private Double longitude;
        private Double latitude;
        private Double height;
        private int srid = -1;

        private void checkUnassigned( Object key, String fieldName )
        {
            if ( key != null )
            {
                throw new IllegalArgumentException( String.format( "Duplicate field '%s' is not allowed." , fieldName ) );
            }
        }

        public void assign( String key, AnyValue value )
        {
            if ( value instanceof IntegralValue )
            {
                assignIntegral( key, ((IntegralValue) value).longValue() );
            }
            else if ( value instanceof FloatingPointValue )
            {
                assignFloatingPoint( key, ((FloatingPointValue) value).doubleValue() );
            }
            else if ( value instanceof TextValue )
            {
                assignTextValue( key, ((TextValue) value).stringValue() );
            }
            else
            {
                throw new IllegalArgumentException( String.format( "Cannot assign %s to field %s", value, key ) );
            }
        }

        private void assignTextValue( String key, String value )
        {
            String lowercaseKey = key.toLowerCase();
            switch ( lowercaseKey )
            {
            case "crs":
                checkUnassigned( crs, lowercaseKey );
                crs = quotesPattern.matcher( value ).replaceAll( "" );
                break;
            default:
                throwOnUnrecognizedKey( key );
            }
        }

        private void assignFloatingPoint( String key, double value )
        {
            String lowercaseKey = key.toLowerCase();
            switch ( lowercaseKey )
            {
            case "x":
                checkUnassigned( x, lowercaseKey );
                x = value;
                break;
            case "y":
                checkUnassigned( y, lowercaseKey );
                y = value;
                break;
            case "z":
                checkUnassigned( z, lowercaseKey );
                z = value;
                break;
            case "longitude":
                checkUnassigned( longitude, lowercaseKey );
                longitude = value;
                break;
            case "latitude":
                checkUnassigned( latitude, lowercaseKey );
                latitude = value;
                break;
            case "height":
                checkUnassigned( height, lowercaseKey );
                height = value;
                break;
            default:
                throwOnUnrecognizedKey( key );
            }
        }

        private void assignIntegral( String key, long value )
        {
            switch ( key.toLowerCase() )
            {
            case "x":
            case "y":
            case "z":
            case "longitude":
            case "latitude":
            case "height":
                assignFloatingPoint( key, (double) value );
                break;
            case "srid":
                if ( srid != -1 )
                {
                    throw new IllegalArgumentException( "Duplicate field 'srid' is not allowed." );
                }
                srid = (int) value;
                break;
            default:
                throwOnUnrecognizedKey( key );
            }
        }

        public void assign( String key, String value )
        {
            switch ( key.toLowerCase() )
            {
            case "crs":
                assignTextValue( key, value );
                break;
            case "x":
            case "y":
            case "z":
            case "longitude":
            case "latitude":
            case "height":
                assignFloatingPoint( key, Double.parseDouble( value ) );
                break;
            case "srid":
                assignIntegral( key, Integer.parseInt( value ) );
                break;
            default:
                throwOnUnrecognizedKey( key );
            }
        }

        private void throwOnUnrecognizedKey( String key )
        {
            throw new IllegalArgumentException( String.format( "Unknown key '%s' for creating new point", key ) );
        }

        void mergeWithHeader( PointCSVHeaderInformation header )
        {
            this.crs = this.crs == null ? header.crs : this.crs;
            this.x = this.x == null ? header.x : this.x;
            this.y = this.y == null ? header.y : this.y;
            this.z = this.z == null ? header.z : this.z;
            this.longitude = this.longitude == null ? header.longitude : this.longitude;
            this.latitude = this.latitude == null ? header.latitude : this.latitude;
            this.height = this.height == null ? header.height : this.height;
            this.srid = this.srid == -1 ? header.srid : this.srid;
        }
    }
}
