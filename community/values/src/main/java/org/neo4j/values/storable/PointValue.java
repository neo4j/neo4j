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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.graphdb.spatial.Coordinate;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.hashing.HashFunction;
import org.neo4j.values.AnyValue;
import org.neo4j.values.Comparison;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.utils.InvalidValuesArgumentException;
import org.neo4j.values.utils.PrettyPrinter;
import org.neo4j.values.virtual.MapValue;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

public class PointValue extends ScalarValue implements Point, Comparable<PointValue>
{
    public static String[] ALLOWED_KEYS = new String[]{"crs", "x", "y", "z", "longitude", "latitude", "height", "srid"};

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
                throw new InvalidValuesArgumentException( "Cannot create a point with non-finite coordinate values: " + Arrays.toString(coordinate) );
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
        // TODO: This can be an assert
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

        // TODO: This is unnecessary and can be an assert. Is it even correct? This implies e.g. that all 2D points are before all 3D regardless of x and y
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
    Comparison unsafeTernaryCompareTo( Value otherValue )
    {
        PointValue other = (PointValue) otherValue;

        if ( this.crs.getCode() != other.crs.getCode() || this.coordinate.length != other.coordinate.length )
        {
            return Comparison.UNDEFINED;
        }

        int eq = 0;
        int gt = 0;
        int lt = 0;
        for ( int i = 0; i < coordinate.length; i++ )
        {
            int cmpVal = Double.compare( this.coordinate[i], other.coordinate[i] );
            if ( cmpVal > 0 )
            {
                gt++;
            }
            else if ( cmpVal < 0 )
            {
                lt++;
            }
            else
            {
                eq++;
            }
        }
        if ( eq == coordinate.length )
        {
            return Comparison.EQUAL;
        }
        else if ( gt == coordinate.length )
        {
            return Comparison.GREATER_THAN;
        }
        else if ( lt == coordinate.length )
        {
            return Comparison.SMALLER_THAN;
        }
        else if ( lt == 0 )
        {
            return Comparison.GREATER_THAN_AND_EQUAL;
        }
        else if ( gt == 0 )
        {
            return Comparison.SMALLER_THAN_AND_EQUAL;
        }
        else
        {
            return Comparison.UNDEFINED;
        }
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
    public long updateHash( HashFunction hashFunction, long hash )
    {
        hash = hashFunction.update( hash, crs.getCode() );
        for ( double v : coordinate )
        {
            hash = hashFunction.update( hash, Double.doubleToLongBits( v ) );
        }
        return hash;
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return mapper.mapPoint( this );
    }

    @Override
    public String toString()
    {
        String coordString = coordinate.length == 2 ? format( "x: %s, y: %s", coordinate[0], coordinate[1] )
                                                    : format( "x: %s, y: %s, z: %s", coordinate[0], coordinate[1], coordinate[2] );
        return format( "point({%s, crs: '%s'})", coordString, getCoordinateReferenceSystem().getName() ); //TODO: Use getTypeName -> Breaking change
    }

    @Override
    public String getTypeName()
    {
        return "Point";
    }

    /**
     * The string representation of this object when indexed in string-only indexes, like lucene, for equality search only. This should normally only
     * happen when points are part of composite indexes, because otherwise they are indexed in the spatial index.
     */
    public String toIndexableString()
    {
        CoordinateReferenceSystem crs = getCoordinateReferenceSystem();
        return format( "P:%d-%d%s", crs.getTable().getTableId(), crs.getCode(), Arrays.toString( coordinate ) );
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
    public Boolean withinRange( PointValue lower, boolean includeLower, PointValue upper, boolean includeUpper )
    {
        // Unbounded
        if ( lower == null && upper == null )
        {
            return true;
        }

        // Invalid bounds (lower greater than upper)
        if ( lower != null && upper != null )
        {
            Comparison comparison = lower.unsafeTernaryCompareTo( upper );
            if ( comparison == Comparison.UNDEFINED || comparison == Comparison.GREATER_THAN || comparison == Comparison.GREATER_THAN_AND_EQUAL )
            {
                return null;
            }
        }

        // Lower bound defined
        if ( lower != null )
        {
            Comparison comparison = this.unsafeTernaryCompareTo( lower );
            if ( comparison == Comparison.UNDEFINED )
            {
                return null;
            }
            else if ( comparison == Comparison.SMALLER_THAN || comparison == Comparison.SMALLER_THAN_AND_EQUAL ||
                    (comparison == Comparison.EQUAL || comparison == Comparison.GREATER_THAN_AND_EQUAL) && !includeLower )
            {
                if ( upper != null && this.unsafeTernaryCompareTo( upper ) == Comparison.UNDEFINED )
                {
                    return null;
                }
                else
                {
                    return false;
                }
            }
        }

        // Upper bound defined
        if ( upper != null )
        {
            Comparison comparison = this.unsafeTernaryCompareTo( upper );
            if ( comparison == Comparison.UNDEFINED )
            {
                return null;
            }
            else if ( comparison == Comparison.GREATER_THAN || comparison == Comparison.GREATER_THAN_AND_EQUAL ||
                    (comparison == Comparison.EQUAL || comparison == Comparison.SMALLER_THAN_AND_EQUAL) && !includeUpper )
            {
                return false;
            }
        }

        return true;
    }

    public static PointValue fromMap( MapValue map )
    {
        PointBuilder fields = new PointBuilder();
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
        PointBuilder fieldsFromData = parseHeaderInformation( text );
        if ( fieldsFromHeader != null )
        {
            // Merge InputFields: Data fields override header fields
            if ( !(fieldsFromHeader instanceof PointBuilder) )
            {
                throw new IllegalStateException( "Wrong header information type: " + fieldsFromHeader );
            }
            fieldsFromData.mergeWithHeader( (PointBuilder) fieldsFromHeader );
        }
        return fromInputFields( fieldsFromData );
    }

    public static PointBuilder parseHeaderInformation( CharSequence text )
    {
        PointBuilder fields = new PointBuilder();
        Value.parseHeaderInformation( text, "point", fields );
        return fields;
    }

    private static CoordinateReferenceSystem findSpecifiedCRS( PointBuilder fields )
    {
        String crsValue = fields.crs;
        int sridValue = fields.srid;
        if ( crsValue != null && sridValue != -1 )
        {
            throw new InvalidValuesArgumentException( "Cannot specify both CRS and SRID" );
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
    private static PointValue fromInputFields( PointBuilder fields )
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
                throw new InvalidValuesArgumentException( "Geographic points does not support coordinate reference system: " + crs +
                        ". This is set either in the csv header or the actual data column" );
            }
        }
        else
        {
            if ( crs == CoordinateReferenceSystem.Cartesian )
            {
                throw new InvalidValuesArgumentException( "A " + CoordinateReferenceSystem.Cartesian.getName() + " point must contain 'x' and 'y'" );
            }
            else if ( crs == CoordinateReferenceSystem.Cartesian_3D )
            {
                throw new InvalidValuesArgumentException( "A " + CoordinateReferenceSystem.Cartesian_3D.getName() + " point must contain 'x', 'y' and 'z'" );
            }
            else if ( crs == CoordinateReferenceSystem.WGS84 )
            {
                throw new InvalidValuesArgumentException( "A " + CoordinateReferenceSystem.WGS84.getName() + " point must contain 'latitude' and 'longitude'" );
            }
            else if ( crs == CoordinateReferenceSystem.WGS84_3D )
            {
                throw new InvalidValuesArgumentException(
                        "A " + CoordinateReferenceSystem.WGS84_3D.getName() + " point must contain 'latitude', 'longitude' and 'height'" );
            }
            throw new InvalidValuesArgumentException( "A point must contain either 'x' and 'y' or 'latitude' and 'longitude'" );
        }

        if ( crs.getDimension() != coordinates.length )
        {
            throw new InvalidValuesArgumentException( "Cannot create point with " + crs.getDimension() + "D coordinate reference system and "
                    + coordinates.length + " coordinates. Please consider using equivalent " + coordinates.length + "D coordinate reference system" );
        }
        return Values.pointValue( crs, coordinates );
    }

    /**
     * For accessors from cypher.
     */
    public Value get( String fieldName )
    {
       return PointFields.fromName( fieldName ).get( this );
    }

    DoubleValue getNthCoordinate( int n, String fieldName, boolean onlyGeographic )
    {
        if ( onlyGeographic && !this.getCoordinateReferenceSystem().isGeographic() )
        {
            throw new InvalidValuesArgumentException( "Field: " + fieldName + " is not available on cartesian point: " + this );
        }
        else if ( n >= this.coordinate().length )
        {
            throw new InvalidValuesArgumentException( "Field: " + fieldName + " is not available on point: " + this );
        }
        else
        {
            return Values.doubleValue( coordinate[n] );
        }
    }

    private static class PointBuilder implements CSVHeaderInformation
    {
        private String crs;
        private Double x;
        private Double y;
        private Double z;
        private Double longitude;
        private Double latitude;
        private Double height;
        private int srid = -1;
        private boolean allowOpenMaps = true;

        @Override
        public void assign( String key, Object value )
        {
            switch ( key.toLowerCase() )
            {
            case "crs":
                checkUnassigned( crs, key );
                assignTextValue( key, value, str -> crs = quotesPattern.matcher( str ).replaceAll( "" ) );
                break;
            case "x":
                checkUnassigned( x, key );
                assignFloatingPoint( key, value, i -> x = i );
                break;
            case "y":
                checkUnassigned( y, key );
                assignFloatingPoint( key, value, i -> y = i );
                break;
            case "z":
                checkUnassigned( z, key );
                assignFloatingPoint( key, value, i -> z = i );
                break;
            case "longitude":
                checkUnassigned( longitude, key );
                assignFloatingPoint( key, value, i -> longitude = i );
                break;
            case "latitude":
                checkUnassigned( latitude, key );
                assignFloatingPoint( key, value, i -> latitude = i );
                break;
            case "height":
                checkUnassigned( height, key );
                assignFloatingPoint( key, value, i -> height = i );
                break;
            case "srid":
                if ( srid != -1 )
                {
                    throw new InvalidValuesArgumentException( String.format( "Duplicate field '%s' is not allowed.", key ) );
                }
                assignIntegral( key, value, i -> srid = i );
                break;
            default:
                if ( !allowOpenMaps )
                {
                    throwOnUnrecognizedKey( key );
                }
            }
        }

        void mergeWithHeader( PointBuilder header )
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

        private void assignTextValue( String key, Object value, Consumer<String> assigner )
        {
            if ( value instanceof String )
            {
                assigner.accept( (String) value );
            }
            else if ( value instanceof TextValue )
            {
                assigner.accept( ((TextValue) value).stringValue() );
            }
            else
            {
                throw new InvalidValuesArgumentException( String.format( "Cannot assign %s to field %s", value, key ) );
            }
        }

        private void assignFloatingPoint( String key, Object value, Consumer<Double> assigner )
        {
            if ( value instanceof String )
            {
                assigner.accept( assertConvertible( () -> Double.parseDouble( (String) value ) ) );
            }
            else if ( value instanceof IntegralValue )
            {
                assigner.accept( ((IntegralValue) value).doubleValue() );
            }
            else if ( value instanceof FloatingPointValue )
            {
                assigner.accept( ((FloatingPointValue) value).doubleValue() );
            }
            else
            {
                throw new InvalidValuesArgumentException( String.format( "Cannot assign %s to field %s", value, key ) );
            }
        }

        private void assignIntegral( String key, Object value, Consumer<Integer> assigner )
        {
            if ( value instanceof String )
            {
                assigner.accept( assertConvertible( () -> Integer.parseInt( (String) value ) ) );
            }
            else if ( value instanceof IntegralValue )
            {
                assigner.accept( (int) ((IntegralValue) value).longValue() );
            }
            else
            {
                throw new InvalidValuesArgumentException( String.format( "Cannot assign %s to field %s", value, key ) );
            }
        }

        private void throwOnUnrecognizedKey( String key )
        {
            throw new InvalidValuesArgumentException( String.format( "Unknown key '%s' for creating new point", key ) );
        }

        private <T extends Number> T assertConvertible( Supplier<T> func )
        {
            try
            {
                return func.get();
            }
            catch ( NumberFormatException e )
            {
                throw new InvalidValuesArgumentException( e.getMessage(), e );
            }
        }

        private void checkUnassigned( Object key, String fieldName )
        {
            if ( key != null )
            {
                throw new InvalidValuesArgumentException( String.format( "Duplicate field '%s' is not allowed.", fieldName ) );
            }
        }
    }
}
