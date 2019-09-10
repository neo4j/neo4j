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
package org.neo4j.internal.batchimport.cache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static org.junit.jupiter.api.Assertions.assertEquals;

@PageCacheExtension
@ExtendWith( RandomExtension.class )
class PageCacheLongArrayTest
{
    private static final int COUNT = 1_000_000;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private PageCache pageCache;
    @Inject
    private RandomRule random;

    @Test
    void verifyPageCacheLongArray() throws Exception
    {
        PagedFile file = pageCache.map( testDirectory.file( "file" ), pageCache.pageSize(), CREATE, DELETE_ON_CLOSE );

        try ( LongArray array = new PageCacheLongArray( file, COUNT, 0, 0 ) )
        {
            verifyBehaviour( array );
        }
    }

    @Test
    void verifyChunkingArrayWithPageCacheLongArray()
    {
        File directory = testDirectory.homeDir();
        NumberArrayFactory numberArrayFactory = NumberArrayFactory.auto( pageCache, directory, false, NumberArrayFactory.NO_MONITOR );
        try ( LongArray array = numberArrayFactory.newDynamicLongArray( COUNT / 1_000, 0 ) )
        {
            verifyBehaviour( array );
        }
    }

    private void verifyBehaviour( LongArray array )
    {
        // insert
        for ( int i = 0; i < COUNT; i++ )
        {
            array.set( i, i );
        }

        // verify inserted data
        for ( int i = 0; i < COUNT; i++ )
        {
            assertEquals( i, array.get( i ) );
        }

        // verify inserted data with random access patterns
        int stride = 12_345_678;
        int next = random.nextInt( COUNT );
        for ( int i = 0; i < COUNT; i++ )
        {
            assertEquals( next, array.get( next ) );
            next = (next + stride) % COUNT;
        }
    }
}
