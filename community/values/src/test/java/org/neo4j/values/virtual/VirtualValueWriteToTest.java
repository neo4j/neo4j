/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.values.virtual;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import org.neo4j.values.AnyValue;
import org.neo4j.values.Values;

import static org.neo4j.values.BufferValueWriter.Specials.beginArray;
import static org.neo4j.values.BufferValueWriter.Specials.endArray;
import static org.neo4j.values.ValueWriter.ArrayType.BYTE;
import static org.neo4j.values.Values.booleanValue;
import static org.neo4j.values.Values.byteArray;
import static org.neo4j.values.Values.charValue;
import static org.neo4j.values.Values.intValue;
import static org.neo4j.values.Values.stringValue;
import static org.neo4j.values.virtual.BufferAnyValueWriter.Specials.beginLabels;
import static org.neo4j.values.virtual.BufferAnyValueWriter.Specials.beginList;
import static org.neo4j.values.virtual.BufferAnyValueWriter.Specials.beginMap;
import static org.neo4j.values.virtual.BufferAnyValueWriter.Specials.beginPath;
import static org.neo4j.values.virtual.BufferAnyValueWriter.Specials.beginPoint;
import static org.neo4j.values.virtual.BufferAnyValueWriter.Specials.endLabels;
import static org.neo4j.values.virtual.BufferAnyValueWriter.Specials.endPath;
import static org.neo4j.values.virtual.BufferAnyValueWriter.Specials.endPoint;
import static org.neo4j.values.virtual.BufferAnyValueWriter.Specials.writeEdgeReference;
import static org.neo4j.values.virtual.BufferAnyValueWriter.Specials.writeLabel;
import static org.neo4j.values.virtual.BufferAnyValueWriter.Specials.writeNodeReference;
import static org.neo4j.values.virtual.BufferAnyValueWriter.Specials.endList;
import static org.neo4j.values.virtual.BufferAnyValueWriter.Specials.endMap;
import static org.neo4j.values.virtual.BufferAnyValueWriter.Specials.writeKeyId;
import static org.neo4j.values.virtual.VirtualValueTestUtil.edges;
import static org.neo4j.values.virtual.VirtualValueTestUtil.nodes;

@RunWith( value = Parameterized.class )
public class VirtualValueWriteToTest
{

    @Parameterized.Parameters( name = "{0}" )
    public static Iterable<WriteTest> data()
    {
        return Arrays.asList(
                shouldWrite(
                        VirtualValues.list(
                            booleanValue( false ),
                            byteArray( new byte[]{3, 4, 5} ),
                            stringValue( "yo" )
                        ),
                        beginList( 3 ),
                        false,
                        beginArray( 3, BYTE ), (byte)3, (byte)4, (byte)5, endArray(),
                        "yo",
                        endList()
                ),
                shouldWrite(
                        VirtualValues.map(
                            new int[]{1, 2},
                            new AnyValue[]{ intValue( 100 ), charValue( 'c' ) }
                        ),
                        beginMap( 2 ),
                        writeKeyId( 1 ), 100,
                        writeKeyId( 2 ), 'c',
                        endMap()
                ),
                shouldWrite(
                        VirtualValues.labels( 1, 2, 3 ),
                        beginLabels( 3 ),
                        writeLabel( 1 ),
                        writeLabel( 2 ),
                        writeLabel( 3 ),
                        endLabels()
                ),
                shouldWrite(
                        VirtualValues.node( 1L ),
                        writeNodeReference( 1L )
                ),
                shouldWrite(
                        VirtualValues.edge( 2L ),
                        writeEdgeReference( 2L )
                ),
                shouldWrite(
                        VirtualValues.path( nodes( 20L, 40L ), edges( 100L ) ),
                        beginPath( 1 ),
                        writeNodeReference( 20L ),
                        writeNodeReference( 40L ),
                        writeEdgeReference( 100L ),
                        endPath()
                ),
                shouldWrite(
                        VirtualValues.pointCartesian( 2.0, -4.0 ),
                        beginPoint( CoordinateReferenceSystem.Cartesian ),
                        2.0,
                        -4.0,
                        endPoint()
                ),
                shouldWrite(
                        VirtualValues.pointGeographic( 2.0, -4.0 ),
                        beginPoint( CoordinateReferenceSystem.WGS84 ),
                        2.0,
                        -4.0,
                        endPoint()
                ),
                // map( list( map( list() ) ) )
                shouldWrite(
                        VirtualValues.map(
                            new int[]{5},
                            new AnyValue[]{
                                    VirtualValues.list(
                                        VirtualValues.map(
                                                new int[]{50},
                                                new AnyValue[]{
                                                        VirtualValues.list() }
                                        )
                                    ) }
                        ),
                        beginMap( 1 ),
                        writeKeyId( 5 ),
                            beginList( 1 ),
                                beginMap( 1 ),
                                writeKeyId( 50 ),
                                    beginList( 0 ),
                                    endList(),
                                endMap(),
                            endList(),
                        endMap()
                )
        );
    }

    private WriteTest currentTest;

    public VirtualValueWriteToTest( WriteTest currentTest )
    {
        this.currentTest = currentTest;
    }

    private static WriteTest shouldWrite( AnyValue value, Object... expected )
    {
        return new WriteTest( value, expected );
    }

    @Test
    public void runTest()
    {
        currentTest.verifyWriteTo();
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
