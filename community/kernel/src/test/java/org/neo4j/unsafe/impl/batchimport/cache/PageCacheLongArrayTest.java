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
package org.neo4j.unsafe.impl.batchimport.cache;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.nio.file.StandardOpenOption;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import static java.lang.System.currentTimeMillis;

import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.unsafe.impl.batchimport.cache.PageCacheNumberArray.PAGE_SIZE;

public class PageCacheLongArrayTest
{
    private static final int COUNT = 100_000_000;

    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory dir = TestDirectory.testDirectory();
    private final RandomRule random = new RandomRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( fs ).around( dir ).around( random );

    @Test
    public void shouldTest() throws Exception
    {
        PageSwapperFactory swapper = new SingleFilePageSwapperFactory();
        swapper.setFileSystemAbstraction( fs );
        int pageSize = (int) mebiBytes( 1 );
        try ( PageCache pageCache = new MuninnPageCache( swapper, 1_000, pageSize, PageCacheTracer.NULL,
                PageCursorTracerSupplier.NULL );
              LongArray array = new PageCacheLongArray( pageCache.map( dir.file( "file" ), pageSize,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ,
                StandardOpenOption.DELETE_ON_CLOSE ) ) )
        {
            test( array );
        }
    }

    @Test
    public void shouldSKjdks() throws Exception
    {
        LongArray array = NumberArrayFactory.AUTO.newDynamicLongArray( PAGE_SIZE / Long.BYTES, 0 );
        test( array );
    }

    private void test( LongArray array )
    {
        long time = currentTimeMillis();
        for ( int i = 0; i < COUNT; i++ )
        {
            array.set( i, i );
        }
        long insertTime = currentTimeMillis() - time;
        time = currentTimeMillis();
        for ( int i = 0; i < COUNT; i++ )
        {
            array.get( i );
        }
        long scanTime = currentTimeMillis() - time;

        int stride = 12_345_678;
        int next = random.nextInt( COUNT );
        time = currentTimeMillis();
        for ( int i = 0; i < COUNT; i++ )
        {
            array.get( next );
            next = (next + stride) % COUNT;
        }
        long randomTime = currentTimeMillis() - time;

        System.out.println( "insert:" + insertTime + ", scan:" + scanTime + ", random:" + randomTime );
    }
}
