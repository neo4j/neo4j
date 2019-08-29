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
package org.neo4j.internal.batchimport.input;

import org.junit.jupiter.api.Test;

import java.io.Flushable;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FlushableChannel;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;

@EphemeralTestDirectoryExtension
class ValueTypeTest
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory directory;

    @Test
    void arraysShouldCalculateCorrectLength() throws IOException
    {
        // given
        int[] value = new int[3];
        for ( int i = 0; i < value.length; i++ )
        {
            value[i] = 100 + i;
        }
        ValueType valueType = ValueType.typeOf( value );
        CountingChannel channel = new CountingChannel();

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

    private static class CountingChannel implements FlushableChannel
    {
        private long position;

        @Override
        public Flushable prepareForFlush()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FlushableChannel put( byte value )
        {
            position += Byte.BYTES;
            return this;
        }

        @Override
        public FlushableChannel putShort( short value )
        {
            position += Short.BYTES;
            return this;
        }

        @Override
        public FlushableChannel putInt( int value )
        {
            position += Integer.BYTES;
            return this;
        }

        @Override
        public FlushableChannel putLong( long value )
        {
            position += Long.BYTES;
            return this;
        }

        @Override
        public FlushableChannel putFloat( float value )
        {
            position += Float.BYTES;
            return this;
        }

        @Override
        public FlushableChannel putDouble( double value )
        {
            position += Double.BYTES;
            return this;
        }

        @Override
        public FlushableChannel put( byte[] value, int length )
        {
            position += length;
            return this;
        }

        @Override
        public void close()
        {
        }

        long position()
        {
            return position;
        }
    }
}
