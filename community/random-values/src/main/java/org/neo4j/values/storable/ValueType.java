/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

public enum ValueType
{
    BOOLEAN( ValueGroup.BOOLEAN, BooleanValue.class ),
    BYTE( ValueGroup.NUMBER, ByteValue.class ),
    SHORT( ValueGroup.NUMBER, ShortValue.class ),
    INT( ValueGroup.NUMBER, IntValue.class ),
    LONG( ValueGroup.NUMBER, LongValue.class ),
    FLOAT( ValueGroup.NUMBER, FloatValue.class ),
    DOUBLE( ValueGroup.NUMBER, DoubleValue.class ),
    CHAR( ValueGroup.TEXT, CharValue.class ),
    STRING( ValueGroup.TEXT, TextValue.class ),
    STRING_ALPHANUMERIC( ValueGroup.TEXT, TextValue.class ),
    STRING_ASCII( ValueGroup.TEXT, TextValue.class ),
    STRING_BMP( ValueGroup.TEXT, TextValue.class ),
    LOCAL_DATE_TIME( ValueGroup.LOCAL_DATE_TIME, LocalDateTimeValue.class ),
    DATE( ValueGroup.DATE, DateValue.class ),
    LOCAL_TIME( ValueGroup.LOCAL_TIME, LocalTimeValue.class ),
    PERIOD( ValueGroup.DURATION, DurationValue.class ),
    DURATION( ValueGroup.DURATION, DurationValue.class ),
    TIME( ValueGroup.ZONED_TIME, TimeValue.class ),
    DATE_TIME( ValueGroup.ZONED_DATE_TIME, DateTimeValue.class ),
    CARTESIAN_POINT( ValueGroup.GEOMETRY, PointValue.class ),
    CARTESIAN_POINT_3D( ValueGroup.GEOMETRY, PointValue.class ),
    GEOGRAPHIC_POINT( ValueGroup.GEOMETRY, PointValue.class ),
    GEOGRAPHIC_POINT_3D( ValueGroup.GEOMETRY, PointValue.class ),
    BOOLEAN_ARRAY( ValueGroup.BOOLEAN_ARRAY, BooleanArray.class, true ),
    BYTE_ARRAY( ValueGroup.NUMBER_ARRAY, ByteArray.class, true ),
    SHORT_ARRAY( ValueGroup.NUMBER_ARRAY, ShortArray.class, true ),
    INT_ARRAY( ValueGroup.NUMBER_ARRAY, IntArray.class, true ),
    LONG_ARRAY( ValueGroup.NUMBER_ARRAY, LongArray.class, true ),
    FLOAT_ARRAY( ValueGroup.NUMBER_ARRAY, FloatArray.class, true ),
    DOUBLE_ARRAY( ValueGroup.NUMBER_ARRAY, DoubleArray.class, true ),
    CHAR_ARRAY( ValueGroup.TEXT_ARRAY, CharArray.class, true ),
    STRING_ARRAY( ValueGroup.TEXT_ARRAY, StringArray.class, true ),
    STRING_ALPHANUMERIC_ARRAY( ValueGroup.TEXT_ARRAY, StringArray.class, true ),
    STRING_ASCII_ARRAY( ValueGroup.TEXT_ARRAY, StringArray.class, true ),
    STRING_BMP_ARRAY( ValueGroup.TEXT_ARRAY, StringArray.class, true ),
    LOCAL_DATE_TIME_ARRAY( ValueGroup.LOCAL_DATE_TIME_ARRAY, LocalDateTimeArray.class, true ),
    DATE_ARRAY( ValueGroup.DATE_ARRAY, DateArray.class, true ),
    LOCAL_TIME_ARRAY( ValueGroup.LOCAL_TIME_ARRAY, LocalTimeArray.class, true ),
    PERIOD_ARRAY( ValueGroup.DURATION_ARRAY, DurationArray.class, true ),
    DURATION_ARRAY( ValueGroup.DURATION_ARRAY, DurationArray.class, true ),
    TIME_ARRAY( ValueGroup.ZONED_TIME_ARRAY, TimeArray.class, true ),
    DATE_TIME_ARRAY( ValueGroup.ZONED_DATE_TIME_ARRAY, DateTimeArray.class, true ),
    CARTESIAN_POINT_ARRAY( ValueGroup.GEOMETRY_ARRAY, PointArray.class, true ),
    CARTESIAN_POINT_3D_ARRAY( ValueGroup.GEOMETRY_ARRAY, PointArray.class, true ),
    GEOGRAPHIC_POINT_ARRAY( ValueGroup.GEOMETRY_ARRAY, PointArray.class, true ),
    GEOGRAPHIC_POINT_3D_ARRAY( ValueGroup.GEOMETRY_ARRAY, PointArray.class, true );

    public final ValueGroup valueGroup;
    public final Class<? extends Value> valueClass;
    public final boolean arrayType;

    ValueType( ValueGroup valueGroup, Class<? extends Value> valueClass )
    {
        this( valueGroup, valueClass, false );
    }

    ValueType( ValueGroup valueGroup, Class<? extends Value> valueClass, boolean arrayType )
    {
        this.valueGroup = valueGroup;
        this.valueClass = valueClass;
        this.arrayType = arrayType;
    }

    static ValueType[] arrayTypes()
    {
        return Arrays.stream( ValueType.values() )
                .filter( t -> t.arrayType )
                .toArray( ValueType[]::new );
    }
}
