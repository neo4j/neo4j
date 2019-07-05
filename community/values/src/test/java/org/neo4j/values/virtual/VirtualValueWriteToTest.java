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
package org.neo4j.values.virtual;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.Stream;

import org.neo4j.values.AnyValue;
import org.neo4j.values.BufferAnyValueWriter;
import org.neo4j.values.storable.BufferValueWriter.Specials;

import static org.junit.jupiter.params.provider.Arguments.of;
import static org.neo4j.values.BufferAnyValueWriter.Specials.beginList;
import static org.neo4j.values.BufferAnyValueWriter.Specials.beginMap;
import static org.neo4j.values.BufferAnyValueWriter.Specials.endList;
import static org.neo4j.values.BufferAnyValueWriter.Specials.endMap;
import static org.neo4j.values.BufferAnyValueWriter.Specials.writeNode;
import static org.neo4j.values.BufferAnyValueWriter.Specials.writeNodeReference;
import static org.neo4j.values.BufferAnyValueWriter.Specials.writePath;
import static org.neo4j.values.BufferAnyValueWriter.Specials.writeRelationship;
import static org.neo4j.values.BufferAnyValueWriter.Specials.writeRelationshipReference;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.byteArray;
import static org.neo4j.values.storable.Values.charValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;
import static org.neo4j.values.virtual.VirtualValues.map;
import static org.neo4j.values.virtual.VirtualValues.nodeValue;
import static org.neo4j.values.virtual.VirtualValues.relationship;
import static org.neo4j.values.virtual.VirtualValues.relationshipValue;

@SuppressWarnings( "ReferenceEquality" )
class VirtualValueWriteToTest
{
    private static Stream<Arguments> parameters()
    {
        return Stream.of(
                of(shouldWrite(
                        VirtualValues.list(
                                booleanValue( false ),
                                byteArray( new byte[]{3, 4, 5} ),
                                stringValue( "yo" )
                        ),
                        beginList( 3 ),
                        false,
                        Specials.byteArray( new byte[]{3, 4, 5} ),
                        "yo",
                        endList()
                ) ),
                of(shouldWrite(
                        VirtualValues.map(
                                new String[]{"foo", "bar"},
                                new AnyValue[]{intValue( 100 ), charValue( 'c' )}
                        ),
                        beginMap( 2 ),
                        "bar", 'c',
                        "foo", 100,
                        endMap()
                ) ),
                of(shouldWrite(
                        VirtualValues.node( 1L ),
                        writeNodeReference( 1L )
                ) ),
                of(shouldWrite(
                        relationship( 2L ),
                        writeRelationshipReference( 2L )
                ) ),
                of(shouldWrite(
                        VirtualValues.path(
                                new NodeValue[]{nodeValue( 20L, stringArray( "L" ), EMPTY_MAP ),
                                        nodeValue( 40L, stringArray( "L" ), EMPTY_MAP )},
                                new RelationshipValue[]{
                                        relationshipValue( 100L, nodeValue( 40L, stringArray( "L" ), EMPTY_MAP ),
                                                nodeValue( 20L, stringArray( "L" ), EMPTY_MAP ),
                                                stringValue( "T" ), EMPTY_MAP )} ),
                        writePath(
                                new NodeValue[]{nodeValue( 20L, stringArray( "L" ), EMPTY_MAP ),
                                        nodeValue( 40L, stringArray( "L" ), EMPTY_MAP )},
                                new RelationshipValue[]{
                                        relationshipValue( 100L, nodeValue( 40L, stringArray( "L" ), EMPTY_MAP ),
                                                nodeValue( 20L, stringArray( "L" ), EMPTY_MAP ),
                                                stringValue( "T" ), EMPTY_MAP )} )
                ) ),
                // map( list( map( list() ) ) )
                of(shouldWrite(
                        VirtualValues.map(
                                new String[]{"foo"},
                                new AnyValue[]{
                                        VirtualValues.list(
                                                VirtualValues.map(
                                                        new String[]{"bar"},
                                                        new AnyValue[]{
                                                                VirtualValues.list()}
                                                )
                                        )}
                        ),
                        beginMap( 1 ),
                        "foo",
                        beginList( 1 ),
                        beginMap( 1 ),
                        "bar",
                        beginList( 0 ),
                        endList(),
                        endMap(),
                        endList(),
                        endMap()
                ) ) ,
                of(shouldWrite(
                        nodeValue( 1337L,
                                stringArray( "L1", "L2" ),
                                map( new String[]{"foo"}, new AnyValue[]{stringValue( "foo" )} ) ),
                        writeNode( 1337L,
                                stringArray( "L1", "L2" ) ,
                                map( new String[]{"foo"}, new AnyValue[]{stringValue( "foo" )} ) )
                ) ),
                of(shouldWrite(
                        relationshipValue( 1337L, nodeValue( 42L, stringArray( "L" ), EMPTY_MAP ),
                                nodeValue( 43L, stringArray( "L" ), EMPTY_MAP ),
                                stringValue( "T" ),
                                map( new String[]{"foo"}, new AnyValue[]{stringValue( "foo" )} ) ),
                        writeRelationship( 1337L, 42L, 43L,
                                stringValue( "T" ),
                                map( new String[]{"foo"}, new AnyValue[]{stringValue( "foo" )} ) ) ) )
        );
    }

    private static WriteTest shouldWrite( AnyValue value, Object... expected )
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
        private final AnyValue value;
        private final Object[] expected;

        private WriteTest( AnyValue value, Object... expected )
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
            BufferAnyValueWriter writer = new BufferAnyValueWriter();
            value.writeTo( writer );
            writer.assertBuffer( expected );
        }
    }
}
