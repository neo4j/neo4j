/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.unsafe.impl.batchimport.input;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.io.fs.OpenMode;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableChannel;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;

public class ValueTypeTest
{
    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory( fs );

    @Test
    public void arraysShouldCalculateCorrectLength() throws IOException
    {
        // given
        int[] value = new int[3];
        for ( int i = 0; i < value.length; i++ )
        {
            value[i] = 100 + i;
        }
        ValueType valueType = ValueType.typeOf( value );
        PhysicalFlushableChannel channel = new PhysicalFlushableChannel( fs.open( directory.file( "file" ), OpenMode.READ_WRITE ) );

        // when
        int length = valueType.length( value );
        valueType.write( value, channel );

        // then
        int expected =
                1 +                           // component type
                Integer.BYTES +               // array length
                value.length * Integer.BYTES; // array data
        assertEquals( expected, length );
        assertEquals( expected, channel.position() );
    }
}
