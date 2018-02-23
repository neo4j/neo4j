/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.cache;

import java.io.IOException;

import org.neo4j.io.pagecache.PagedFile;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PageCacheIntArrayConcurrencyTest extends PageCacheNumberArrayConcurrencyTest
{
    @Override
    protected Runnable wholeFileRacer( NumberArray array, int contestant )
    {
        return new WholeFileRacer( (PageCacheIntArray) array );
    }

    @Override
    protected Runnable fileRangeRacer( NumberArray array, int contestant )
    {
        return new FileRangeRacer( (PageCacheIntArray) array, contestant );
    }

    @Override
    protected PageCacheIntArray getNumberArray( PagedFile file ) throws IOException
    {
        return new PageCacheIntArray( file, COUNT, 0, 0 );
    }

    private class WholeFileRacer implements Runnable
    {
        private IntArray array;

        WholeFileRacer( IntArray array )
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
                    array.set( i, i );
                    assertEquals( i, array.get( i ) );
                }
            }
        }
    }

    private class FileRangeRacer implements Runnable
    {
        private IntArray array;
        private int contestant;

        FileRangeRacer( IntArray array, int contestant )
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
                    int value = random.nextInt();
                    array.set( i, value );
                    assertEquals( value, array.get( i ) );
                }
            }
        }
    }
}
