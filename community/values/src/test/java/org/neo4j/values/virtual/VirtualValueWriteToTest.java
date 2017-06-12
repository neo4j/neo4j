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
import static org.neo4j.values.virtual.BufferAnyValueWriter.Specials.beginList;
import static org.neo4j.values.virtual.BufferAnyValueWriter.Specials.endList;

@RunWith( value = Parameterized.class )
public class VirtualValueWriteToTest
{

    @Parameterized.Parameters( name = "{0}" )
    public static Iterable<WriteTest> data()
    {
        return Arrays.asList(
                shouldWrite(
                    VirtualValues.list(
                        Values.booleanValue( false ),
                        Values.byteArray( new byte[]{3, 4, 5} ),
                        Values.stringValue( "yo" )
                    ),
                        beginList( 3 ),
                        false,
                        beginArray( 3, BYTE ), (byte)3, (byte)4, (byte)5, endArray(),
                        "yo",
                        endList()
                    )
        );
    }

    private WriteTest currentTest;

    public VirtualValueWriteToTest( WriteTest currentTest )
    {
        this.currentTest = currentTest;
    }

    private static WriteTest shouldWrite( Object value, Object... expected )
    {
        return new WriteTest( Values.of( value ), expected );
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
