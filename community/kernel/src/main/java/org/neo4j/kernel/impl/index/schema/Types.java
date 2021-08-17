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
package org.neo4j.kernel.impl.index.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueWriter;

import static org.neo4j.io.pagecache.PageCache.PAGE_SIZE;

/**
 * A collection of all instances of {@link Type} and mappings to and from them.
 */
class Types
{
    // A list of all supported types

    // Geometry type cannot be replaced directly, because we need to support
    // sorting of points both in BTREE and RANGE index types.
    // Since only one geometry type will be used in each index type,
    // we keep the same typeId to more easily fit it into how the type arrays are handled in this file.
    // GeometryType will be removed in 5.0.
    static final GeometryType GEOMETRY = new GeometryType( (byte) 0 );
    static final GeometryType2 GEOMETRY_2 = new GeometryType2( (byte) 0 );
    static final ZonedDateTimeType ZONED_DATE_TIME = new ZonedDateTimeType( (byte) 1 );
    static final LocalDateTimeType LOCAL_DATE_TIME = new LocalDateTimeType( (byte) 2 );
    static final DateType DATE = new DateType( (byte) 3 );
    static final ZonedTimeType ZONED_TIME = new ZonedTimeType( (byte) 4 );
    static final LocalTimeType LOCAL_TIME = new LocalTimeType( (byte) 5 );
    static final DurationType DURATION = new DurationType( (byte) 6 );
    static final TextType TEXT = new TextType( (byte) 7 );
    static final BooleanType BOOLEAN = new BooleanType( (byte) 8 );
    static final NumberType NUMBER = new NumberType( (byte) 9 );
    // See the comment about geometry type for explanation about the two geometry array types
    static final GeometryArrayType GEOMETRY_ARRAY = new GeometryArrayType( (byte) 10 );
    static final GeometryArrayType2 GEOMETRY_ARRAY_2 = new GeometryArrayType2( (byte) 10 );
    static final ZonedDateTimeArrayType ZONED_DATE_TIME_ARRAY = new ZonedDateTimeArrayType( (byte) 11 );
    static final LocalDateTimeArrayType LOCAL_DATE_TIME_ARRAY = new LocalDateTimeArrayType( (byte) 12 );
    static final DateArrayType DATE_ARRAY = new DateArrayType( (byte) 13 );
    static final ZonedTimeArrayType ZONED_TIME_ARRAY = new ZonedTimeArrayType( (byte) 14 );
    static final LocalTimeArrayType LOCAL_TIME_ARRAY = new LocalTimeArrayType( (byte) 15 );
    static final DurationArrayType DURATION_ARRAY = new DurationArrayType( (byte) 16 );
    static final TextArrayType TEXT_ARRAY = new TextArrayType( (byte) 17 );
    static final BooleanArrayType BOOLEAN_ARRAY = new BooleanArrayType( (byte) 18 );
    static final NumberArrayType NUMBER_ARRAY = new NumberArrayType( (byte) 19 );

    /* Geometry constants are located in PointKeyUtil */
    public static final int SIZE_ZONED_DATE_TIME = Long.BYTES +    /* epochSecond */
                                                   Integer.BYTES + /* nanoOfSecond */
                                                   Integer.BYTES;  /* timeZone */
    public static final int SIZE_LOCAL_DATE_TIME = Long.BYTES +    /* epochSecond */
                                                   Integer.BYTES;  /* nanoOfSecond */
    public static final int SIZE_DATE =            Long.BYTES;     /* epochDay */
    public static final int SIZE_ZONED_TIME = Long.BYTES +    /* nanosOfDayUTC */
                                              Integer.BYTES;  /* zoneOffsetSeconds */
    public static final int SIZE_LOCAL_TIME =      Long.BYTES;     /* nanoOfDay */
    public static final int SIZE_DURATION = Long.BYTES +    /* totalAvgSeconds */
                                            Integer.BYTES + /* nanosOfSecond */
                                            Long.BYTES +    /* months */
                                            Long.BYTES;     /* days */
    public static final int SIZE_STRING_LENGTH =   Short.BYTES;    /* length of string byte array */
    public static final int SIZE_BOOLEAN =         Byte.BYTES;     /* byte for this boolean value */
    public static final int SIZE_NUMBER_TYPE =     Byte.BYTES;     /* type of value */
    public static final int SIZE_NUMBER_BYTE =     Byte.BYTES;     /* raw value bits */
    static final int BIGGEST_REASONABLE_ARRAY_LENGTH = PAGE_SIZE / SIZE_NUMBER_BYTE;
    public static final int SIZE_NUMBER_SHORT =    Short.BYTES;    /* raw value bits */
    public static final int SIZE_NUMBER_INT =      Integer.BYTES;  /* raw value bits */
    public static final int SIZE_NUMBER_LONG =     Long.BYTES;     /* raw value bits */
    public static final int SIZE_NUMBER_FLOAT =    Integer.BYTES;  /* raw value bits */
    public static final int SIZE_NUMBER_DOUBLE =   Long.BYTES;     /* raw value bits */
    public static final int SIZE_ARRAY_LENGTH =    Short.BYTES;

    private static AbstractArrayType<?> typeOf( ValueWriter.ArrayType arrayType, AbstractArrayType<?> geometryArrayType )
    {
        switch ( arrayType )
        {
        case BOOLEAN:
            return BOOLEAN_ARRAY;
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
            return NUMBER_ARRAY;
        case STRING:
        case CHAR:
            return TEXT_ARRAY;
        case LOCAL_DATE_TIME:
            return LOCAL_DATE_TIME_ARRAY;
        case DATE:
            return DATE_ARRAY;
        case DURATION:
            return DURATION_ARRAY;
        case POINT:
            return geometryArrayType;
        case LOCAL_TIME:
            return LOCAL_TIME_ARRAY;
        case ZONED_DATE_TIME:
            return ZONED_DATE_TIME_ARRAY;
        case ZONED_TIME:
            return ZONED_TIME_ARRAY;
        default:
            throw new UnsupportedOperationException( arrayType.name() );
        }
    }

    private static Type[] instantiateTypes( Type geometryType, Type geometryArrayType )
    {
        List<Type> types = new ArrayList<>();

        types.add( geometryType );
        types.add( ZONED_DATE_TIME );
        types.add( LOCAL_DATE_TIME );
        types.add( DATE );
        types.add( ZONED_TIME );
        types.add( LOCAL_TIME );
        types.add( DURATION );
        types.add( TEXT );
        types.add( BOOLEAN );
        types.add( NUMBER );

        types.add( geometryArrayType );
        types.add( ZONED_DATE_TIME_ARRAY );
        types.add( LOCAL_DATE_TIME_ARRAY );
        types.add( DATE_ARRAY );
        types.add( ZONED_TIME_ARRAY );
        types.add( LOCAL_TIME_ARRAY );
        types.add( DURATION_ARRAY );
        types.add( TEXT_ARRAY );
        types.add( BOOLEAN_ARRAY );
        types.add( NUMBER_ARRAY );

        // Assert order of typeId
        byte expectedTypeId = 0;
        for ( Type type : types )
        {
            if ( type.typeId != expectedTypeId )
            {
                throw new IllegalStateException( "The order in this list is not the intended one" );
            }
            expectedTypeId++;
        }
        return types.toArray( new Type[0] );
    }

    static class Btree
    {
        /**
         * Holds typeId --> {@link Type} mapping.
         */
        static final Type[] BY_ID = instantiateTypes( GEOMETRY, GEOMETRY_ARRAY );

        /**
         * Holds {@link ValueGroup#ordinal()} --> {@link Type} mapping.
         */
        static final Type[] BY_GROUP = new Type[ValueGroup.values().length];

        /**
         * Holds {@link ValueWriter.ArrayType} --> {@link Type} mapping.
         */
        static final AbstractArrayType[] BY_ARRAY_TYPE = new AbstractArrayType[ValueWriter.ArrayType.values().length];

        /**
         * Lowest {@link Type} according to {@link Type#COMPARATOR}.
         */
        static final Type LOWEST_BY_VALUE_GROUP = Collections.min( Arrays.asList( BY_ID ), Type.COMPARATOR );

        /**
         * Highest {@link Type} according to {@link Type#COMPARATOR}.
         */
        static final Type HIGHEST_BY_VALUE_GROUP = Collections.max( Arrays.asList( BY_ID ), Type.COMPARATOR );

        static
        {
            // Build BY_GROUP mapping.
            for ( Type type : BY_ID )
            {
                BY_GROUP[type.valueGroup.ordinal()] = type;
            }

            // Build BY_ARRAY_TYPE mapping.
            for ( ValueWriter.ArrayType arrayType : ValueWriter.ArrayType.values() )
            {
                BY_ARRAY_TYPE[arrayType.ordinal()] = typeOf( arrayType, GEOMETRY_ARRAY );
            }
        }
    }

    static class Range
    {
        /**
         * Holds typeId --> {@link Type} mapping.
         */
        static final Type[] BY_ID = instantiateTypes( GEOMETRY_2, GEOMETRY_ARRAY_2);

        /**
         * Holds {@link ValueGroup#ordinal()} --> {@link Type} mapping.
         */
        static final Type[] BY_GROUP = new Type[ValueGroup.values().length];

        /**
         * Holds {@link ValueWriter.ArrayType} --> {@link Type} mapping.
         */
        static final AbstractArrayType[] BY_ARRAY_TYPE = new AbstractArrayType[ValueWriter.ArrayType.values().length];

        /**
         * Lowest {@link Type} according to {@link Type#COMPARATOR}.
         */
        static final Type LOWEST_BY_VALUE_GROUP = Collections.min( Arrays.asList( BY_ID ), Type.COMPARATOR );

        /**
         * Highest {@link Type} according to {@link Type#COMPARATOR}.
         */
        static final Type HIGHEST_BY_VALUE_GROUP = Collections.max( Arrays.asList( BY_ID ), Type.COMPARATOR );

        static
        {
            // Build BY_GROUP mapping.
            for ( Type type : BY_ID )
            {
                BY_GROUP[type.valueGroup.ordinal()] = type;
            }

            // Build BY_ARRAY_TYPE mapping.
            for ( ValueWriter.ArrayType arrayType : ValueWriter.ArrayType.values() )
            {
                BY_ARRAY_TYPE[arrayType.ordinal()] = typeOf( arrayType, GEOMETRY_ARRAY_2 );
            }
        }
    }

}
