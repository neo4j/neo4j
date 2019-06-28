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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.neo4j.values.storable.BufferValueWriter.Specials.beginArray;
import static org.neo4j.values.storable.BufferValueWriter.Specials.byteArray;
import static org.neo4j.values.storable.BufferValueWriter.Specials.endArray;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.ValueWriter.ArrayType.BOOLEAN;
import static org.neo4j.values.storable.ValueWriter.ArrayType.CHAR;
import static org.neo4j.values.storable.ValueWriter.ArrayType.DOUBLE;
import static org.neo4j.values.storable.ValueWriter.ArrayType.FLOAT;
import static org.neo4j.values.storable.ValueWriter.ArrayType.INT;
import static org.neo4j.values.storable.ValueWriter.ArrayType.LOCAL_DATE_TIME;
import static org.neo4j.values.storable.ValueWriter.ArrayType.LONG;
import static org.neo4j.values.storable.ValueWriter.ArrayType.SHORT;
import static org.neo4j.values.storable.ValueWriter.ArrayType.STRING;

class ValueWriteToTest
{
    private static Stream<Arguments> parameters()
    {
        return Stream.of(
            Arguments.of( shouldWrite( true, true ) ),
            Arguments.of( shouldWrite( false, false ) ),
            Arguments.of( shouldWrite( (byte) 0, (byte) 0 ) ),
            Arguments.of( shouldWrite( (byte) 42, (byte) 42 ) ),
            Arguments.of( shouldWrite( (short) 42, (short) 42 ) ),
            Arguments.of( shouldWrite( 42, 42 ) ),
            Arguments.of( shouldWrite( 42L, 42L ) ),
            Arguments.of( shouldWrite( 42.0f, 42.0f ) ),
            Arguments.of( shouldWrite( 42.0, 42.0 ) ),
            Arguments.of( shouldWrite( 'x', 'x' ) ),
            Arguments.of( shouldWrite( "Hi", "Hi" ) ),
            Arguments.of( shouldWrite( Values.NO_VALUE, (Object) null ) ),
            Arguments.of( shouldWrite( Values.pointValue( Cartesian, 1, 2 ), Values.pointValue( Cartesian, 1, 2 ) ) ),
            Arguments.of( shouldWrite( Values.pointValue( WGS84, 1, 2 ), Values.pointValue( WGS84, 1, 2 ) ) ),
            Arguments.of( shouldWrite( LocalDate.of( 1991, 10, 18 ), DateValue.date( 1991, 10, 18 ) ) ),

            Arguments.of( shouldWrite( new byte[]{1, 2, 3}, byteArray( new byte[]{1, 2, 3} ) ) ),
            Arguments.of( shouldWrite( new short[]{1, 2, 3}, beginArray( 3, SHORT ), (short) 1, (short) 2, (short) 3, endArray() ) ),
            Arguments.of( shouldWrite( new int[]{1, 2, 3}, beginArray( 3, INT ), 1, 2, 3, endArray() ) ),
            Arguments.of( shouldWrite( new long[]{1, 2, 3}, beginArray( 3, LONG ), 1L, 2L, 3L, endArray() ) ),
            Arguments.of( shouldWrite( new float[]{1, 2, 3}, beginArray( 3, FLOAT ), 1.0f, 2.0f, 3.0f, endArray() ) ),
            Arguments.of( shouldWrite( new double[]{1, 2, 3}, beginArray( 3, DOUBLE ), 1.0, 2.0, 3.0, endArray() ) ),
            Arguments.of( shouldWrite( new char[]{'a', 'b'}, beginArray( 2, CHAR ), 'a', 'b', endArray() ) ),
            Arguments.of( shouldWrite( new String[]{"a", "b"}, beginArray( 2, STRING ), "a", "b", endArray() ) ),
            Arguments.of( shouldWrite( new boolean[]{true, false}, beginArray( 2, BOOLEAN ), true, false, endArray() ) ),
            Arguments.of( shouldWrite( new LocalDateTime[]{
                    LocalDateTime.of( 1991, 10, 18, 6, 37, 0, 0 ),
                    LocalDateTime.of( 1992, 10, 18, 6, 37, 0, 0 )
                },
                beginArray( 2, LOCAL_DATE_TIME ),
                LocalDateTimeValue.localDateTime( 1991, 10, 18, 6, 37, 0, 0 ),
                LocalDateTimeValue.localDateTime( 1992, 10, 18, 6, 37, 0, 0 ),
                endArray() ) ),

            Arguments.of( shouldWrite( new byte[]{1, 2, 3}, byteArray( new byte[]{1, 2, 3} ) ) )
        );
    }

    private static WriteTest shouldWrite( Object value, Object... expected )
    {
        return new WriteTest( Values.of( value ), expected );
    }

    private static WriteTest shouldWrite( Value value, Object... expected )
    {
        return new WriteTest( value, expected );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void runTest( WriteTest test )
    {
        test.verifyWriteTo();
    }

    private static class WriteTest
    {
        private final Value value;
        private final Object[] expected;

        private WriteTest( Value value, Object... expected )
        {
            this.value = value;
            this.expected = expected;
        }

        @Override
        public String toString()
        {
            return String.format( "%s should write %s", value, Arrays.toString( expected ) );
        }

        void verifyWriteTo()
        {
            BufferValueWriter writer = new BufferValueWriter();
            value.writeTo( writer );
            writer.assertBuffer( expected );
        }
    }
}
