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

import java.lang.reflect.Array;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.TernaryComparator;

import static java.lang.String.format;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateValue.date;
import static org.neo4j.values.storable.DurationValue.duration;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTime;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.TimeValue.time;

/**
 * Entry point to the values library.
 * <p>
 * The values library centers around the Value class, which represents a value in Neo4j. Values can be correctly
 * checked for equality over different primitive representations, including consistent hashCodes and sorting.
 * <p>
 * To create Values use the factory methods in the Values class.
 * <p>
 * Values come in two major categories: Storable and Virtual. Storable values are valid values for
 * node, relationship and graph properties. Virtual values are not supported as property values, but might be created
 * and returned as part of cypher execution. These include Node, Relationship and Path.
 */
@SuppressWarnings( "WeakerAccess" )
public final class Values
{
    public static final Value MIN_NUMBER = Values.doubleValue( Double.NEGATIVE_INFINITY );
    public static final Value MAX_NUMBER = Values.doubleValue( Double.NaN );
    public static final Value ZERO_FLOAT = Values.doubleValue( 0.0 );
    public static final IntegralValue ZERO_INT = Values.longValue( 0 );
    public static final Value MIN_STRING = StringValue.EMTPY;
    public static final Value MAX_STRING = Values.booleanValue( false );
    public static final BooleanValue TRUE = Values.booleanValue( true );
    public static final BooleanValue FALSE = Values.booleanValue( false );
    public static final TextValue EMPTY_STRING = StringValue.EMTPY;
    public static final DoubleValue E = Values.doubleValue( Math.E );
    public static final DoubleValue PI = Values.doubleValue( Math.PI );
    public static final ArrayValue EMPTY_SHORT_ARRAY = Values.shortArray( new short[0] );
    public static final ArrayValue EMPTY_BOOLEAN_ARRAY = Values.booleanArray( new boolean[0] );
    public static final ArrayValue EMPTY_BYTE_ARRAY = Values.byteArray( new byte[0] );
    public static final ArrayValue EMPTY_CHAR_ARRAY = Values.charArray( new char[0] );
    public static final ArrayValue EMPTY_INT_ARRAY = Values.intArray( new int[0] );
    public static final ArrayValue EMPTY_LONG_ARRAY = Values.longArray( new long[0] );
    public static final ArrayValue EMPTY_FLOAT_ARRAY = Values.floatArray( new float[0] );
    public static final ArrayValue EMPTY_DOUBLE_ARRAY = Values.doubleArray( new double[0] );
    public static final TextArray EMPTY_TEXT_ARRAY = Values.stringArray();

    private Values()
    {
    }

    /**
     * Default value comparator. Will correctly compare all storable values and order the value groups according the
     * to orderability group.
     *
     * To get Comparability semantics, use .ternaryCompare
     */
    public static final ValueComparator COMPARATOR = new ValueComparator( ValueGroup::compareTo );

    public static boolean isNumberValue( Object value )
    {
        return value instanceof NumberValue;
    }

    public static boolean isBooleanValue( Object value )
    {
        return value instanceof BooleanValue;
    }

    public static boolean isTextValue( Object value )
    {
        return value instanceof TextValue;
    }

    public static boolean isArrayValue( Value value )
    {
        return value instanceof ArrayValue;
    }

    public static boolean isGeometryValue( Value value )
    {
        return value instanceof PointValue;
    }

    public static boolean isTemporalValue( Value value )
    {
        return value instanceof TemporalValue || value instanceof DurationValue;
    }

    public static double coerceToDouble( Value value )
    {
        if ( value instanceof IntegralValue )
        {
            return ((IntegralValue) value).longValue();
        }
        if ( value instanceof FloatingPointValue )
        {
            return ((FloatingPointValue) value).doubleValue();
        }
        throw new UnsupportedOperationException( format( "Cannot coerce %s to double", value ) );
    }

    // DIRECT FACTORY METHODS

    public static final Value NO_VALUE = NoValue.NO_VALUE;

    public static TextValue utf8Value( byte[] bytes )
    {
        if ( bytes.length == 0 )
        {
            return EMPTY_STRING;
        }

        return utf8Value( bytes, 0, bytes.length );
    }

    public static TextValue utf8Value( byte[] bytes, int offset, int length )
    {
        if ( length == 0 )
        {
            return EMPTY_STRING;
        }

        return new UTF8StringValue( bytes, offset, length );
    }

    public static TextValue stringValue( String value )
    {
        if ( value.isEmpty() )
        {
            return EMPTY_STRING;
        }
        return new StringWrappingStringValue( value );
    }

    public static Value stringOrNoValue( String value )
    {
        if ( value == null )
        {
            return NO_VALUE;
        }
        else
        {
            return stringValue( value );
        }
    }

    public static NumberValue numberValue( Number number )
    {
        if ( number instanceof Long )
        {
            return longValue( number.longValue() );
        }
        if ( number instanceof Integer )
        {
            return intValue( number.intValue() );
        }
        if ( number instanceof Double )
        {
            return doubleValue( number.doubleValue() );
        }
        if ( number instanceof Byte )
        {
            return byteValue( number.byteValue() );
        }
        if ( number instanceof Float )
        {
            return floatValue( number.floatValue() );
        }
        if ( number instanceof Short )
        {
            return shortValue( number.shortValue() );
        }

        throw new UnsupportedOperationException( "Unsupported type of Number " + number.toString() );
    }

    public static LongValue longValue( long value )
    {
        return new LongValue( value );
    }

    public static IntValue intValue( int value )
    {
        return new IntValue( value );
    }

    public static ShortValue shortValue( short value )
    {
        return new ShortValue( value );
    }

    public static ByteValue byteValue( byte value )
    {
        return new ByteValue( value );
    }

    public static BooleanValue booleanValue( boolean value )
    {
        return value ? BooleanValue.TRUE : BooleanValue.FALSE;
    }

    public static CharValue charValue( char value )
    {
        return new CharValue( value );
    }

    public static DoubleValue doubleValue( double value )
    {
        return new DoubleValue( value );
    }

    public static FloatValue floatValue( float value )
    {
        return new FloatValue( value );
    }

    public static TextArray stringArray( String... value )
    {
        return new StringArray( value );
    }

    public static ByteArray byteArray( byte[] value )
    {
        return new ByteArray( value );
    }

    public static LongArray longArray( long[] value )
    {
        return new LongArray( value );
    }

    public static IntArray intArray( int[] value )
    {
        return new IntArray( value );
    }

    public static DoubleArray doubleArray( double[] value )
    {
        return new DoubleArray( value );
    }

    public static FloatArray floatArray( float[] value )
    {
        return new FloatArray( value );
    }

    public static BooleanArray booleanArray( boolean[] value )
    {
        return new BooleanArray( value );
    }

    public static CharArray charArray( char[] value )
    {
        return new CharArray( value );
    }

    public static ShortArray shortArray( short[] value )
    {
        return new ShortArray( value );
    }

    /**
     * Unlike pointValue(), this method does not enforce consistency between the CRS and coordinate dimensions.
     * This can be useful for testing.
     */
    public static PointValue unsafePointValue( CoordinateReferenceSystem crs, double... coordinate )
    {
        return new PointValue( crs, coordinate );
    }

    /**
     * Creates a PointValue, and enforces consistency between the CRS and coordinate dimensions.
     */
    public static PointValue pointValue( CoordinateReferenceSystem crs, double... coordinate )
    {
        if ( crs.getDimension() != coordinate.length )
        {
            throw new IllegalArgumentException(
                    format( "Cannot create point, CRS %s expects %d dimensions, but got coordinates %s",
                            crs, crs.getDimension(), Arrays.toString( coordinate ) ) );
        }
        return new PointValue( crs, coordinate );
    }

    public static PointValue point( Point point )
    {
        // An optimization could be to do an instanceof PointValue check here
        // and in that case just return the casted argument.
        List<Double> coordinate = point.getCoordinate().getCoordinate();
        double[] coords = new double[coordinate.size()];
        for ( int i = 0; i < coords.length; i++ )
        {
            coords[i] = coordinate.get( i );
        }
        return new PointValue( crs( point.getCRS() ), coords );
    }

    public static PointValue minPointValue( PointValue reference )
    {
        double[] coordinates = new double[reference.coordinate().length];
        Arrays.fill( coordinates, -Double.MAX_VALUE );
        return pointValue( reference.getCoordinateReferenceSystem(), coordinates );
    }

    public static PointValue maxPointValue( PointValue reference )
    {
        double[] coordinates = new double[reference.coordinate().length];
        Arrays.fill( coordinates, Double.MAX_VALUE );
        return pointValue( reference.getCoordinateReferenceSystem(), coordinates );
    }

    public static PointArray pointArray( Point[] points )
    {
        PointValue[] values = new PointValue[points.length];
        for ( int i = 0; i < points.length; i++ )
        {
            values[i] = Values.point( points[i] );
        }
        return new PointArray( values );
    }

    public static PointArray pointArray( Value[] maybePoints )
    {
        PointValue[] values = new PointValue[maybePoints.length];
        for ( int i = 0; i < maybePoints.length; i++ )
        {
            Value maybePoint = maybePoints[i];
            if ( !(maybePoint instanceof PointValue) )
            {
                throw new IllegalArgumentException( format( "[%s:%s] is not a supported point value", maybePoint, maybePoint.getClass().getName() ) );
            }
            values[i] = Values.point( (PointValue) maybePoint );
        }
        return pointArray( values );
    }

    public static PointArray pointArray( PointValue[] points )
    {
        return new PointArray( points );
    }

    public static CoordinateReferenceSystem crs( CRS crs )
    {
        return CoordinateReferenceSystem.get( crs );
    }

    public static Value temporalValue( Temporal value )
    {
        if ( value instanceof ZonedDateTime )
        {
            return datetime( (ZonedDateTime) value );
        }
        if ( value instanceof OffsetDateTime )
        {
            return datetime( (OffsetDateTime) value );
        }
        if ( value instanceof LocalDateTime )
        {
            return localDateTime( (LocalDateTime) value );
        }
        if ( value instanceof OffsetTime )
        {
            return time( (OffsetTime) value );
        }
        if ( value instanceof LocalDate )
        {
            return date( (LocalDate) value );
        }
        if ( value instanceof LocalTime )
        {
            return localTime( (LocalTime) value );
        }
        if ( value instanceof TemporalValue )
        {
            return (Value) value;
        }
        if ( value == null )
        {
            return NO_VALUE;
        }

        throw new UnsupportedOperationException( "Unsupported type of Temporal " + value.toString() );
    }

    public static DurationValue durationValue( TemporalAmount value )
    {
        if ( value instanceof Duration )
        {
            return duration( (Duration) value );
        }
        if ( value instanceof Period )
        {
            return duration( (Period) value );
        }
        if ( value instanceof DurationValue )
        {
            return (DurationValue) value;
        }
        DurationValue duration = duration( 0, 0, 0, 0 );
        for ( TemporalUnit unit : value.getUnits() )
        {
            duration = duration.plus( value.get( unit ), unit );
        }
        return duration;
    }

    public static ArrayValue dateTimeArray( ZonedDateTime[] values )
    {
        return new DateTimeArray( values );
    }

    public static ArrayValue localDateTimeArray( LocalDateTime[] values )
    {
        return new LocalDateTimeArray( values );
    }

    public static ArrayValue localTimeArray( LocalTime[] values )
    {
        return new LocalTimeArray( values );
    }

    public static ArrayValue timeArray( OffsetTime[] values )
    {
        return new TimeArray( values );
    }

    public static ArrayValue dateArray( LocalDate[] values )
    {
        return new DateArray( values );
    }

    public static ArrayValue durationArray( DurationValue[] values )
    {
        return new DurationArray( values );
    }

    public static ArrayValue durationArray( TemporalAmount[] values )
    {
        DurationValue[] durations = new DurationValue[values.length];
        for ( int i = 0; i < values.length; i++ )
        {
            durations[i] = durationValue( values[i] );
        }
        return new DurationArray( durations );
    }

    // BOXED FACTORY METHODS

    /**
     * Generic value factory method.
     * <p>
     * Beware, this method is intended for converting externally supplied values to the internal Value type, and to
     * make testing convenient. Passing a Value as in parameter should never be needed, and will throw an
     * UnsupportedOperationException.
     * <p>
     * This method does defensive copying of arrays, while the explicit *Array() factory methods do not.
     *
     * @param value Object to convert to Value
     * @return the created Value
     */
    public static Value of( Object value )
    {
        return of( value, true );
    }

    public static Value of( Object value, boolean allowNull )
    {
        Value of = unsafeOf( value, allowNull );
        if ( of != null )
        {
            return of;
        }
        Objects.requireNonNull( value );
        throw new IllegalArgumentException(
                format( "[%s:%s] is not a supported property value", value, value.getClass().getName() ) );
    }

    public static Value unsafeOf( Object value, boolean allowNull )
    {
        if ( value instanceof String )
        {
            return stringValue( (String) value );
        }
        if ( value instanceof Object[] )
        {
            return arrayValue( (Object[]) value );
        }
        if ( value instanceof Boolean )
        {
            return booleanValue( (Boolean) value );
        }
        if ( value instanceof Number )
        {
            return numberValue( (Number) value );
        }
        if ( value instanceof Character )
        {
            return charValue( (Character) value );
        }
        if ( value instanceof Temporal )
        {
            return temporalValue( (Temporal) value );
        }
        if ( value instanceof TemporalAmount )
        {
            return durationValue( (TemporalAmount) value );
        }
        if ( value instanceof byte[] )
        {
            return byteArray( ((byte[]) value).clone() );
        }
        if ( value instanceof long[] )
        {
            return longArray( ((long[]) value).clone() );
        }
        if ( value instanceof int[] )
        {
            return intArray( ((int[]) value).clone() );
        }
        if ( value instanceof double[] )
        {
            return doubleArray( ((double[]) value).clone() );
        }
        if ( value instanceof float[] )
        {
            return floatArray( ((float[]) value).clone() );
        }
        if ( value instanceof boolean[] )
        {
            return booleanArray( ((boolean[]) value).clone() );
        }
        if ( value instanceof char[] )
        {
            return charArray( ((char[]) value).clone() );
        }
        if ( value instanceof short[] )
        {
            return shortArray( ((short[]) value).clone() );
        }
        if ( value == null )
        {
            if ( allowNull )
            {
                return NoValue.NO_VALUE;
            }
            throw new IllegalArgumentException( "[null] is not a supported property value" );
        }
        if ( value instanceof Point )
        {
            return Values.point( (Point) value );
        }
        if ( value instanceof Value )
        {
            throw new UnsupportedOperationException(
                    "Converting a Value to a Value using Values.of() is not supported." );
        }

        // otherwise fail
       return null;
    }

    /**
     * Generic value factory method.
     * <p>
     * Converts an array of object values to the internal Value type. See {@link Values#of}.
     */
    public static Value[] values( Object... objects )
    {
        return Arrays.stream( objects )
                .map( Values::of )
                .toArray( Value[]::new );
    }

    @Deprecated
    public static Object asObject( Value value )
    {
        return value == null ? null : value.asObject();
    }

    public static Object[] asObjects( Value[] propertyValues )
    {
        Object[] legacy = new Object[propertyValues.length];

        for ( int i = 0; i < propertyValues.length; i++ )
        {
            legacy[i] = propertyValues[i].asObjectCopy();
        }

        return legacy;
    }

    private static Value arrayValue( Object[] value )
    {
        if ( value instanceof String[] )
        {
            return stringArray( copy( value, new String[value.length] ) );
        }
        if ( value instanceof Byte[] )
        {
            return byteArray( copy( value, new byte[value.length] ) );
        }
        if ( value instanceof Long[] )
        {
            return longArray( copy( value, new long[value.length] ) );
        }
        if ( value instanceof Integer[] )
        {
            return intArray( copy( value, new int[value.length] ) );
        }
        if ( value instanceof Double[] )
        {
            return doubleArray( copy( value, new double[value.length] ) );
        }
        if ( value instanceof Float[] )
        {
            return floatArray( copy( value, new float[value.length] ) );
        }
        if ( value instanceof Boolean[] )
        {
            return booleanArray( copy( value, new boolean[value.length] ) );
        }
        if ( value instanceof Character[] )
        {
            return charArray( copy( value, new char[value.length] ) );
        }
        if ( value instanceof Short[] )
        {
            return shortArray( copy( value, new short[value.length] ) );
        }
        if ( value instanceof PointValue[] )
        {
            return pointArray( copy( value, new PointValue[value.length] ) );
        }
        if ( value instanceof Point[] )
        {
            // no need to copy here, since the pointArray(...) method will copy into a PointValue[]
            return pointArray( (Point[])value );
        }
        if ( value instanceof ZonedDateTime[] )
        {
            return dateTimeArray( copy( value, new ZonedDateTime[value.length] ) );
        }
        if ( value instanceof LocalDateTime[] )
        {
            return localDateTimeArray( copy( value, new LocalDateTime[value.length] ) );
        }
        if ( value instanceof LocalTime[] )
        {
            return localTimeArray( copy( value, new LocalTime[value.length] ) );
        }
        if ( value instanceof OffsetTime[] )
        {
            return timeArray( copy( value, new OffsetTime[value.length] ) );
        }
        if ( value instanceof LocalDate[] )
        {
            return dateArray( copy( value, new LocalDate[value.length] ) );
        }
        if ( value instanceof TemporalAmount[] )
        {
            // no need to copy here, since the durationArray(...) method will perform copying as appropriate
            return durationArray( (TemporalAmount[]) value );
        }
        return null;
    }

    private static <T> T copy( Object[] value, T target )
    {
        for ( int i = 0; i < value.length; i++ )
        {
            if ( value[i] == null )
            {
                throw new IllegalArgumentException( "Property array value elements may not be null." );
            }
            Array.set( target, i, value[i] );
        }
        return target;
    }

    public static Value minValue( ValueGroup valueGroup, Value value )
    {
        switch ( valueGroup )
        {
        case TEXT: return MIN_STRING;
        case NUMBER: return MIN_NUMBER;
        case GEOMETRY: return minPointValue( (PointValue)value );
        case DATE: return DateValue.MIN_VALUE;
        case LOCAL_DATE_TIME: return LocalDateTimeValue.MIN_VALUE;
        case ZONED_DATE_TIME: return DateTimeValue.MIN_VALUE;
        case LOCAL_TIME: return LocalTimeValue.MIN_VALUE;
        case ZONED_TIME: return TimeValue.MIN_VALUE;
        default: throw new IllegalStateException(
                format( "The minValue for valueGroup %s is not defined yet", valueGroup ) );
        }
    }

    public static Value maxValue( ValueGroup valueGroup, Value value )
    {
        switch ( valueGroup )
        {
        case TEXT: return MAX_STRING;
        case NUMBER: return MAX_NUMBER;
        case GEOMETRY: return maxPointValue( (PointValue)value );
        case DATE: return DateValue.MAX_VALUE;
        case LOCAL_DATE_TIME: return LocalDateTimeValue.MAX_VALUE;
        case ZONED_DATE_TIME: return DateTimeValue.MAX_VALUE;
        case LOCAL_TIME: return LocalTimeValue.MAX_VALUE;
        case ZONED_TIME: return TimeValue.MAX_VALUE;
        default: throw new IllegalStateException(
                format( "The maxValue for valueGroup %s is not defined yet", valueGroup ) );
        }
    }
}
