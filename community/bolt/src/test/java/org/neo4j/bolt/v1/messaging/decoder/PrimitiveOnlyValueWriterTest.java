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
package org.neo4j.bolt.v1.messaging.decoder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.stream.Stream;

import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.values.storable.Values.byteArray;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;
import static org.neo4j.values.virtual.VirtualValues.nodeValue;
import static org.neo4j.values.virtual.VirtualValues.relationshipValue;

class PrimitiveOnlyValueWriterTest
{
    @Test
    void shouldConvertStringValueToString()
    {
        PrimitiveOnlyValueWriter writer = new PrimitiveOnlyValueWriter();
        TextValue value = stringValue( "Hello" );

        assertEquals( "Hello", writer.valueAsObject( value ) );
    }

    @Test
    void shouldConvertLongValueToLong()
    {
        PrimitiveOnlyValueWriter writer = new PrimitiveOnlyValueWriter();
        LongValue value = longValue( 42 );

        assertEquals( 42L, writer.valueAsObject( value ) );
    }

    @Test
    void shouldConvertMultipleValues()
    {
        PrimitiveOnlyValueWriter writer = new PrimitiveOnlyValueWriter();

        TextValue value1 = stringValue( "Hello" );
        TextValue value2 = stringValue( " " );
        TextValue value3 = stringValue( "World!" );
        LongValue value4 = longValue( 42 );

        assertEquals( "Hello", writer.valueAsObject( value1 ) );
        assertEquals( " ", writer.valueAsObject( value2 ) );
        assertEquals( "World!", writer.valueAsObject( value3 ) );
        assertEquals( 42L, writer.valueAsObject( value4 ) );
    }

    @ParameterizedTest
    @MethodSource( "unsupportedValues" )
    void shouldFailToWriteComplexValue( AnyValue value )
    {
        PrimitiveOnlyValueWriter writer = new PrimitiveOnlyValueWriter();
        assertThrows( UnsupportedOperationException.class, () -> writer.valueAsObject( value ) );
    }

    private static Stream<AnyValue> unsupportedValues()
    {
        return Stream.of(
                nodeValue( 42, stringArray( "Person" ), EMPTY_MAP ),
                newRelationshipValue(),
                pointValue( CoordinateReferenceSystem.WGS84, new double[2] ),
                byteArray( new byte[]{1, 2, 3} ),
                Values.of( Duration.ofHours( 1 ) ),
                Values.of( LocalDate.now() ),
                Values.of( LocalTime.now() ),
                Values.of( OffsetTime.now() ),
                Values.of( LocalDateTime.now() ),
                Values.of( ZonedDateTime.now() )
        );
    }

    private static RelationshipValue newRelationshipValue()
    {
        NodeValue startNode = nodeValue( 24, stringArray( "Person" ), EMPTY_MAP );
        NodeValue endNode = nodeValue( 42, stringArray( "Person" ), EMPTY_MAP );
        return relationshipValue( 42, startNode, endNode, stringValue( "KNOWS" ), EMPTY_MAP );
    }
}
