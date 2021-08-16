/*
 * Copyright (c) "Neo4j"
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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;

import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;

/**
 * Enumerates the different Value types and facilitates creating arrays without resorting to reflection or
 * instance of checking at runtime.
 */
public enum ValueRepresentation
{
    UNKNOWN( ValueGroup.UNKNOWN, false ),
    GEOMETRY_ARRAY( ValueGroup.GEOMETRY_ARRAY, false ),
    ZONED_DATE_TIME_ARRAY( ValueGroup.ZONED_DATE_TIME_ARRAY, false ),
    LOCAL_DATE_TIME_ARRAY( ValueGroup.LOCAL_DATE_TIME_ARRAY, false ),
    DATE_ARRAY( ValueGroup.DATE_ARRAY, false ),
    ZONED_TIME_ARRAY( ValueGroup.ZONED_TIME_ARRAY, false ),
    LOCAL_TIME_ARRAY( ValueGroup.LOCAL_TIME_ARRAY, false ),
    DURATION_ARRAY( ValueGroup.DURATION_ARRAY, false ),
    TEXT_ARRAY( ValueGroup.TEXT_ARRAY, false ),
    BOOLEAN_ARRAY( ValueGroup.BOOLEAN_ARRAY, false ),
    INT64_ARRAY( ValueGroup.NUMBER_ARRAY, false ),
    INT32_ARRAY( ValueGroup.NUMBER_ARRAY, false ),
    INT16_ARRAY( ValueGroup.NUMBER_ARRAY, false ),
    INT8_ARRAY( ValueGroup.NUMBER_ARRAY, false ),
    FLOAT64_ARRAY( ValueGroup.NUMBER_ARRAY, false ),
    FLOAT32_ARRAY( ValueGroup.NUMBER_ARRAY, false ),
    GEOMETRY( ValueGroup.GEOMETRY, true )
            {
                @Override
                public ArrayValue arrayOf( SequenceValue values )
                {
                    PointValue[] points = new PointValue[values.length()];
                    int i = 0;
                    for ( AnyValue value : values )
                    {
                        points[i++] = ((PointValue) value);
                    }
                    return Values.pointArray( points );
                }
            },
    ZONED_DATE_TIME( ValueGroup.ZONED_DATE_TIME, true )
            {
                @Override
                public ArrayValue arrayOf( SequenceValue values )
                {
                    ZonedDateTime[] temporals = new ZonedDateTime[values.length()];
                    int i = 0;
                    for ( AnyValue value : values )
                    {
                        temporals[i++] = ((DateTimeValue) value).temporal();
                    }
                    return Values.dateTimeArray( temporals );
                }
            },
    LOCAL_DATE_TIME( ValueGroup.LOCAL_DATE_TIME, true )
            {
                @Override
                public ArrayValue arrayOf( SequenceValue values )
                {
                    LocalDateTime[] temporals = new LocalDateTime[values.length()];
                    int i = 0;
                    for ( AnyValue value : values )
                    {
                        temporals[i++] = ((LocalDateTimeValue) value).temporal();
                    }
                    return Values.localDateTimeArray( temporals );
                }
            },
    DATE( ValueGroup.DATE, true )
            {
                @Override
                public ArrayValue arrayOf( SequenceValue values )
                {
                    LocalDate[] temporals = new LocalDate[values.length()];
                     int i = 0;
                    for ( AnyValue value : values )
                    {
                        temporals[i++] = ((DateValue) value).temporal();
                    }
                    return Values.dateArray( temporals );
                }
            },
    ZONED_TIME( ValueGroup.ZONED_TIME, true )
            {
                @Override
                public ArrayValue arrayOf( SequenceValue values )
                {
                    OffsetTime[] temporals = new OffsetTime[values.length()];
                     int i = 0;
                    for ( AnyValue value : values )
                    {
                        temporals[i++] = ((TimeValue) value).temporal();
                    }
                    return Values.timeArray( temporals );
                }
            },
    LOCAL_TIME( ValueGroup.LOCAL_TIME, true )
            {
                @Override
                public ArrayValue arrayOf( SequenceValue values )
                {
                    LocalTime[] temporals = new LocalTime[values.length()];
                     int i = 0;
                    for ( AnyValue value : values )
                    {
                        temporals[i++] = ((LocalTimeValue) value).temporal();
                    }
                    return Values.localTimeArray( temporals );
                }
            },
    DURATION( ValueGroup.DURATION, true )
            {
                @Override
                public ArrayValue arrayOf( SequenceValue values )
                {
                    DurationValue[] temporals = new DurationValue[values.length()];
                     int i = 0;
                    for ( AnyValue value : values )
                    {
                        temporals[i++] = ((DurationValue) value);
                    }
                    return Values.durationArray( temporals );
                }
            },
    UTF16_TEXT( ValueGroup.TEXT, true )
            {
                @Override
                public ArrayValue arrayOf( SequenceValue values )
                {
                    String[] strings = new String[values.length()];
                      int i = 0;
                    for ( AnyValue value : values )
                    {
                        strings[i++] = ((TextValue) value).stringValue();
                    }
                    return Values.stringArray( strings );
                }

                @Override
                public ValueRepresentation coerce( ValueRepresentation other )
                {
                    switch ( other )
                    {
                    case UTF8_TEXT:
                    case UTF16_TEXT:
                        return UTF16_TEXT;
                    default:
                        return UNKNOWN;
                    }
                }
            },
    UTF8_TEXT( ValueGroup.TEXT, true )
            {
                @Override
                public ArrayValue arrayOf( SequenceValue values )
                {
                    String[] strings = new String[values.length()];
                      int i = 0;
                    for ( AnyValue value : values )
                    {
                        strings[i++] = ((TextValue) value).stringValue();
                    }
                    return Values.stringArray( strings );
                }

                @Override
                public ValueRepresentation coerce( ValueRepresentation other )
                {
                    switch ( other )
                    {
                    case UTF8_TEXT:
                        return UTF8_TEXT;
                    case UTF16_TEXT:
                        return UTF16_TEXT;
                    default:
                        return UNKNOWN;
                    }
                }
            },
    BOOLEAN( ValueGroup.BOOLEAN, true )
            {
                @Override
                public ArrayValue arrayOf( SequenceValue values )
                {
                    boolean[] bools = new boolean[values.length()];
                    int i = 0;
                    for ( AnyValue value : values )
                    {
                        bools[i++] = ((BooleanValue) value).booleanValue();
                    }
                    return Values.booleanArray( bools );
                }
            },
    INT64( ValueGroup.NUMBER, true )
            {
                @Override
                public ArrayValue arrayOf( SequenceValue values )
                {
                    long[] longs = new long[values.length()];
                    int i = 0;
                    for ( AnyValue value : values )
                    {
                        longs[i++] = ((NumberValue) value).longValue();
                    }
                    return Values.longArray( longs );
                }

                @Override
                public ValueRepresentation coerce( ValueRepresentation other )
                {
                    switch ( other )
                    {
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64:
                        return this;
                    case FLOAT32:
                        return FLOAT32;
                    case FLOAT64:
                        return FLOAT64;
                    default:
                        return ValueRepresentation.UNKNOWN;
                    }
                }
            },
    INT32( ValueGroup.NUMBER, true )
            {
                @Override
                public ArrayValue arrayOf( SequenceValue values )
                {
                    int[] ints = new int[values.length()];
                    int i = 0;
                    for ( AnyValue value : values )
                    {
                        ints[i++] = ((IntValue) value).value();
                    }
                    return Values.intArray( ints );
                }

                @Override
                public ValueRepresentation coerce( ValueRepresentation other )
                {
                    switch ( other )
                    {
                    case INT8:
                    case INT16:
                    case INT32:
                        return this;
                    case INT64:
                        return INT64;
                    case FLOAT32:
                        return FLOAT32;
                    case FLOAT64:
                        return FLOAT64;
                    default:
                        return ValueRepresentation.UNKNOWN;
                    }
                }
            },
    INT16( ValueGroup.NUMBER, true )
            {
                @Override
                public ArrayValue arrayOf( SequenceValue values )
                {
                    short[] shorts = new short[values.length()];
                    int i = 0;
                    for ( AnyValue value : values )
                    {
                        shorts[i++] = ((ShortValue) value).value();
                    }
                    return Values.shortArray( shorts );
                }

                @Override
                public ValueRepresentation coerce( ValueRepresentation other )
                {
                    switch ( other )
                    {
                    case INT8:
                    case INT16:
                        return this;
                    case INT32:
                        return INT32;
                    case INT64:
                        return INT64;
                    case FLOAT32:
                        return FLOAT32;
                    case FLOAT64:
                        return FLOAT64;
                    default:
                        return ValueRepresentation.UNKNOWN;
                    }
                }
            },

    INT8( ValueGroup.NUMBER, true )
            {
                @Override
                public ArrayValue arrayOf( SequenceValue values )
                {
                    byte[] bytes = new byte[values.length()];
                    int i = 0;
                    for ( AnyValue value : values )
                    {
                        bytes[i++] = ((ByteValue) value).value();
                    }
                    return Values.byteArray( bytes );
                }

                @Override
                public ValueRepresentation coerce( ValueRepresentation other )
                {
                    switch ( other )
                    {
                    case INT8:
                        return this;
                    case INT16:
                        return INT16;
                    case INT32:
                        return INT32;
                    case INT64:
                        return INT64;
                    case FLOAT32:
                        return FLOAT32;
                    case FLOAT64:
                        return FLOAT64;
                    default:
                        return ValueRepresentation.UNKNOWN;
                    }
                }
            },
    FLOAT64( ValueGroup.NUMBER, true )
            {
                @Override
                public ArrayValue arrayOf( SequenceValue values )
                {
                    double[] doubles = new double[values.length()];
                    int i = 0;
                    for ( AnyValue value : values )
                    {
                        doubles[i++] = ((NumberValue) value).doubleValue();
                    }
                    return Values.doubleArray( doubles );
                }

                @Override
                public ValueRepresentation coerce( ValueRepresentation other )
                {
                    switch ( other )
                    {
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64:
                    case FLOAT32:
                    case FLOAT64:
                        return this;
                    default:
                        return ValueRepresentation.UNKNOWN;
                    }
                }
            },
    FLOAT32( ValueGroup.NUMBER, true )
            {
                @Override
                public ArrayValue arrayOf( SequenceValue values )
                {
                    float[] floats = new float[values.length()];
                    int i = 0;
                    for ( AnyValue value : values )
                    {
                        floats[i++] = ((FloatValue) value).value();
                    }
                    return Values.floatArray( floats );
                }

                @Override
                public ValueRepresentation coerce( ValueRepresentation other )
                {
                    switch ( other )
                    {
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64:
                    case FLOAT32:
                        return this;
                    case FLOAT64:
                        return FLOAT64;
                    default:
                        return ValueRepresentation.UNKNOWN;
                    }
                }
            },
    NO_VALUE( ValueGroup.NO_VALUE, false );

    private final ValueGroup group;
    private final boolean canCreateArrayOf;

    ValueRepresentation( ValueGroup group, boolean canCreateArrayOf )
    {
        this.group = group;
        this.canCreateArrayOf = canCreateArrayOf;
    }

    public boolean canCreateArrayOfValueGroup()
    {
        return canCreateArrayOf;
    }

    public ValueGroup valueGroup()
    {
        return group;
    }

    /**
     * Creates an array of the corresponding type.
     *
     * NOTE: must call {@link #canCreateArrayOf} before calling this method.
     * NOTE: it is responsibility of the caller to make sure the provided values all are of the correct type
     *       if not a ClassCastException will be thrown.
     * @param values The values (of the correct type) to create the array of.
     * @return An array of the provided values.
     */
    public ArrayValue arrayOf( SequenceValue values )
    {
        throw new IllegalStateException( "Cannot create arrays of " + this );
    }

    /**
     * Finds a representation which fits this and provided representation.
     * @param other the representation to coerce.
     * @return a representation that can handle both representations.
     */
    public ValueRepresentation coerce( ValueRepresentation other )
    {
        return valueGroup() == other.valueGroup() ? this : ValueRepresentation.UNKNOWN;
    }
}
