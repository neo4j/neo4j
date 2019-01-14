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
package org.neo4j.unsafe.impl.batchimport.cache;

import java.io.IOException;

import org.neo4j.io.pagecache.PagedFile;

import static org.junit.Assert.assertArrayEquals;

public class PageCacheByteArrayConcurrencyTest extends PageCacheNumberArrayConcurrencyTest
{
    @Override
    protected Runnable wholeFileRacer( NumberArray array, int contestant )
    {
        return new WholeFileRacer( (ByteArray) array );
    }

    @Override
    protected Runnable fileRangeRacer( NumberArray array, int contestant )
    {
        return new FileRangeRacer( (ByteArray) array, contestant );
    }

    @Override
    protected ByteArray getNumberArray( PagedFile file ) throws IOException
    {
        return new PageCacheByteArray( file, COUNT, new byte[]{-1, -1, -1, -1}, 0 );
    }

    private class WholeFileRacer implements Runnable
    {
        private ByteArray array;

        WholeFileRacer( ByteArray array )
        {
            this.array = array;
        }

        @Override
        public void run()
        {
            for ( int o = 0; o < LAPS; o++ )
            {
                for ( int i = 0; i < COUNT; i++ )
                {
                    byte[] value = {1, 2, 3, 4};
                    array.set( i, value );
                    byte[] actual = new byte[4];
                    array.get( i, actual );
                    assertArrayEquals( value, actual );
                }
            }
        }
    }

    private class FileRangeRacer implements Runnable
    {
        private ByteArray array;
        private int contestant;

        FileRangeRacer( ByteArray array, int contestant )
        {
            this.array = array;
            this.contestant = contestant;
        }

        @Override
        public void run()
        {
            for ( int o = 0; o < LAPS; o++ )
            {
                for ( int i = contestant; i < COUNT; i += CONTESTANTS )
                {
                    byte[] value = new byte[4];
                    byte[] actual = new byte[4];
                    random.nextBytes( value );
                    array.set( i, value );
                    array.get( i, actual );
                    assertArrayEquals( value, actual );
                }
            }
        }
    }
}
