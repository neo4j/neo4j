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

import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_BOOLEAN;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_BOOLEAN_ARRAY;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_BYTE;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_BYTE_ARRAY;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_CARTESIAN_POINT;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_CARTESIAN_POINT_3D;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_CARTESIAN_POINT_3D_ARRAY;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_CARTESIAN_POINT_ARRAY;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_CHAR;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_CHAR_ARRAY;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_DATE;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_DATE_ARRAY;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_DATE_TIME;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_DATE_TIME_ARRAY;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_DOUBLE;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_DOUBLE_ARRAY;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_DURATION;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_DURATION_ARRAY;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_FLOAT;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_FLOAT_ARRAY;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_GEOGRAPHIC_POINT;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_GEOGRAPHIC_POINT_3D;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_GEOGRAPHIC_POINT_3D_ARRAY;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_GEOGRAPHIC_POINT_ARRAY;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_INT;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_INT_ARRAY;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_LOCAL_DATE_TIME;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_LOCAL_DATE_TIME_ARRAY;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_LOCAL_TIME;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_LOCAL_TIME_ARRAY;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_LONG;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_LONG_ARRAY;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_PERIOD;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_PERIOD_ARRAY;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_SHORT;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_SHORT_ARRAY;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_STRING;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_STRING_ALPHANUMERIC;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_STRING_ALPHANUMERIC_ARRAY;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_STRING_ARRAY;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_STRING_ASCII;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_STRING_ASCII_ARRAY;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_STRING_BMP;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_STRING_BMP_ARRAY;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_TIME;
import static org.neo4j.values.storable.ExtremeValuesLibrary.EXTREME_TIME_ARRAY;

public enum ValueType
{
    BOOLEAN( ValueGroup.BOOLEAN, BooleanValue.class, EXTREME_BOOLEAN ),
    BYTE( ValueGroup.NUMBER, ByteValue.class, EXTREME_BYTE ),
    SHORT( ValueGroup.NUMBER, ShortValue.class, EXTREME_SHORT ),
    INT( ValueGroup.NUMBER, IntValue.class, EXTREME_INT ),
    LONG( ValueGroup.NUMBER, LongValue.class, EXTREME_LONG ),
    FLOAT( ValueGroup.NUMBER, FloatValue.class, EXTREME_FLOAT ),
    DOUBLE( ValueGroup.NUMBER, DoubleValue.class, EXTREME_DOUBLE ),
    CHAR( ValueGroup.TEXT, CharValue.class, EXTREME_CHAR ),
    STRING( ValueGroup.TEXT, TextValue.class, EXTREME_STRING ),
    STRING_ALPHANUMERIC( ValueGroup.TEXT, TextValue.class, EXTREME_STRING_ALPHANUMERIC ),
    STRING_ASCII( ValueGroup.TEXT, TextValue.class, EXTREME_STRING_ASCII ),
    STRING_BMP( ValueGroup.TEXT, TextValue.class, EXTREME_STRING_BMP ),
    LOCAL_DATE_TIME( ValueGroup.LOCAL_DATE_TIME, LocalDateTimeValue.class, EXTREME_LOCAL_DATE_TIME ),
    DATE( ValueGroup.DATE, DateValue.class, EXTREME_DATE ),
    LOCAL_TIME( ValueGroup.LOCAL_TIME, LocalTimeValue.class, EXTREME_LOCAL_TIME ),
    PERIOD( ValueGroup.DURATION, DurationValue.class, EXTREME_PERIOD ),
    DURATION( ValueGroup.DURATION, DurationValue.class, EXTREME_DURATION ),
    TIME( ValueGroup.ZONED_TIME, TimeValue.class, EXTREME_TIME ),
    DATE_TIME( ValueGroup.ZONED_DATE_TIME, DateTimeValue.class, EXTREME_DATE_TIME ),
    CARTESIAN_POINT( ValueGroup.GEOMETRY, PointValue.class, EXTREME_CARTESIAN_POINT ),
    CARTESIAN_POINT_3D( ValueGroup.GEOMETRY, PointValue.class, EXTREME_CARTESIAN_POINT_3D ),
    GEOGRAPHIC_POINT( ValueGroup.GEOMETRY, PointValue.class, EXTREME_GEOGRAPHIC_POINT ),
    GEOGRAPHIC_POINT_3D( ValueGroup.GEOMETRY, PointValue.class, EXTREME_GEOGRAPHIC_POINT_3D ),
    BOOLEAN_ARRAY( ValueGroup.BOOLEAN_ARRAY, BooleanArray.class, true, EXTREME_BOOLEAN_ARRAY ),
    BYTE_ARRAY( ValueGroup.NUMBER_ARRAY, ByteArray.class, true, EXTREME_BYTE_ARRAY ),
    SHORT_ARRAY( ValueGroup.NUMBER_ARRAY, ShortArray.class, true, EXTREME_SHORT_ARRAY ),
    INT_ARRAY( ValueGroup.NUMBER_ARRAY, IntArray.class, true, EXTREME_INT_ARRAY ),
    LONG_ARRAY( ValueGroup.NUMBER_ARRAY, LongArray.class, true, EXTREME_LONG_ARRAY ),
    FLOAT_ARRAY( ValueGroup.NUMBER_ARRAY, FloatArray.class, true, EXTREME_FLOAT_ARRAY ),
    DOUBLE_ARRAY( ValueGroup.NUMBER_ARRAY, DoubleArray.class, true, EXTREME_DOUBLE_ARRAY ),
    CHAR_ARRAY( ValueGroup.TEXT_ARRAY, CharArray.class, true, EXTREME_CHAR_ARRAY ),
    STRING_ARRAY( ValueGroup.TEXT_ARRAY, StringArray.class, true, EXTREME_STRING_ARRAY ),
    STRING_ALPHANUMERIC_ARRAY( ValueGroup.TEXT_ARRAY, StringArray.class, true, EXTREME_STRING_ALPHANUMERIC_ARRAY ),
    STRING_ASCII_ARRAY( ValueGroup.TEXT_ARRAY, StringArray.class, true, EXTREME_STRING_ASCII_ARRAY ),
    STRING_BMP_ARRAY( ValueGroup.TEXT_ARRAY, StringArray.class, true, EXTREME_STRING_BMP_ARRAY ),
    LOCAL_DATE_TIME_ARRAY( ValueGroup.LOCAL_DATE_TIME_ARRAY, LocalDateTimeArray.class, true, EXTREME_LOCAL_DATE_TIME_ARRAY ),
    DATE_ARRAY( ValueGroup.DATE_ARRAY, DateArray.class, true, EXTREME_DATE_ARRAY ),
    LOCAL_TIME_ARRAY( ValueGroup.LOCAL_TIME_ARRAY, LocalTimeArray.class, true, EXTREME_LOCAL_TIME_ARRAY ),
    PERIOD_ARRAY( ValueGroup.DURATION_ARRAY, DurationArray.class, true, EXTREME_PERIOD_ARRAY ),
    DURATION_ARRAY( ValueGroup.DURATION_ARRAY, DurationArray.class, true, EXTREME_DURATION_ARRAY ),
    TIME_ARRAY( ValueGroup.ZONED_TIME_ARRAY, TimeArray.class, true, EXTREME_TIME_ARRAY ),
    DATE_TIME_ARRAY( ValueGroup.ZONED_DATE_TIME_ARRAY, DateTimeArray.class, true, EXTREME_DATE_TIME_ARRAY ),
    CARTESIAN_POINT_ARRAY( ValueGroup.GEOMETRY_ARRAY, PointArray.class, true, EXTREME_CARTESIAN_POINT_ARRAY ),
    CARTESIAN_POINT_3D_ARRAY( ValueGroup.GEOMETRY_ARRAY, PointArray.class, true, EXTREME_CARTESIAN_POINT_3D_ARRAY ),
    GEOGRAPHIC_POINT_ARRAY( ValueGroup.GEOMETRY_ARRAY, PointArray.class, true, EXTREME_GEOGRAPHIC_POINT_ARRAY ),
    GEOGRAPHIC_POINT_3D_ARRAY( ValueGroup.GEOMETRY_ARRAY, PointArray.class, true, EXTREME_GEOGRAPHIC_POINT_3D_ARRAY );

    public final ValueGroup valueGroup;
    public final Class<? extends Value> valueClass;
    public final boolean arrayType;
    private final Value[] extremeValues;

    ValueType( ValueGroup valueGroup, Class<? extends Value> valueClass, Value... extremeValues )
    {
        this( valueGroup, valueClass, false, extremeValues );
    }

    ValueType( ValueGroup valueGroup, Class<? extends Value> valueClass, boolean arrayType, Value... extremeValues )
    {
        this.valueGroup = valueGroup;
        this.valueClass = valueClass;
        this.arrayType = arrayType;
        this.extremeValues = extremeValues;
    }

    public Value[] extremeValues()
    {
        return extremeValues;
    }

    static ValueType[] arrayTypes()
    {
        return Arrays.stream( ValueType.values() )
                .filter( t -> t.arrayType )
                .toArray( ValueType[]::new );
    }
}
