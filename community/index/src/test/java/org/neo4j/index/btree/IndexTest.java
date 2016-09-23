/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index.btree;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.cursor.Cursor;
import org.neo4j.index.BTreeHit;
import org.neo4j.index.SCIndexDescription;
import org.neo4j.index.SCInserter;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.index.btree.RangePredicate.greaterOrEqual;
import static org.neo4j.index.btree.RangePredicate.lower;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

public class IndexTest
{
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder( new File( "target" ) );
    private PageCache pageCache;
    private File indexFile;
    private final int pageSize = 1024;
    private final SCIndexDescription description = new SCIndexDescription( "a", "b", "c", OUTGOING, "d", null );

    @Before
    public void setUpPageCache() throws IOException
    {
        PageSwapperFactory swapperFactory = new SingleFilePageSwapperFactory();
        swapperFactory.setFileSystemAbstraction( new DefaultFileSystemAbstraction() );
        pageCache = new MuninnPageCache( swapperFactory, 100, 1024, NULL );
        indexFile = folder.newFile( "index" );
    }

    @After
    public void closePageCache() throws IOException
    {
        pageCache.close();
    }

    @Test
    public void shouldReadWrittenMetaData() throws Exception
    {
        // GIVEN
        try ( Index index = new Index( pageCache, indexFile, description, pageSize ) )
        {   // Open/close is enough
        }

        // WHEN
        try ( Index index = new Index( pageCache, indexFile ) )
        {
            SCIndexDescription readDescription = index.getDescription();

            // THEN
            assertEquals( description, readDescription );
        }
    }

    @Test
    public void shouldStayCorrectAfterRandomModifications() throws Exception
    {
        // GIVEN
        try ( Index index = new Index( pageCache, indexFile, description, pageSize ) )
        {
            Comparator<long[]> keyComparator = index.getTreeNode().keyComparator();
            Map<long[],long[]> data = new TreeMap<>( keyComparator );
            Random random = ThreadLocalRandom.current();
            int count = 100;
            for ( int i = 0; i < count; i++ )
            {
                data.put( randomTreeThing( random ), randomTreeThing( random ) );
            }

            // WHEN
            try ( SCInserter inserter = index.inserter() )
            {
                for ( Map.Entry<long[],long[]> entry : data.entrySet() )
                {
                    inserter.insert( entry.getKey(), entry.getValue() );
                }
            }

            for ( int round = 0; round < 10; round++ )
            {
                // THEN
                for ( int i = 0; i < count*10; i++ )
                {
                    long[] first = randomTreeThing( random );
                    long[] second = randomTreeThing( random );
                    long[] from, to;
                    if ( first[0] < second[0] )
                    {
                        from = first;
                        to = second;
                    }
                    else
                    {
                        from = second;
                        to = first;
                    }
                    RangePredicate fromPredicate = greaterOrEqual( from[0], from[1] );
                    RangePredicate toPredicate = lower( to[0], to[1] );
                    Map<long[],long[]> expectedHits = expectedHits( data, fromPredicate, toPredicate, keyComparator );
                    try ( Cursor<BTreeHit> result = index.seek( fromPredicate, toPredicate ) )
                    {
                        while ( result.next() )
                        {
                            long[] key = result.get().key();
                            if ( expectedHits.remove( key ) == null )
                            {
                                fail( "Unexpected hit " + Arrays.toString( key ) + " when searching for " +
                                        fromPredicate + " - " + toPredicate );
                            }
                            assertTrue( fromPredicate.inRange( key ) >= 0 );
                            assertTrue( toPredicate.inRange( key ) <= 0 ); // apparently "lower" range predicate
                                                                           // returns 0 for equal ids, even if prop is lower
                        }
                        if ( !expectedHits.isEmpty() )
                        {
                            fail( "There were results which were expected to be returned, but weren't:" + expectedHits );
                        }
                    }
                }

                randomlyModifyIndex( index, data, random );
            }
        }
    }

    private void randomlyModifyIndex( Index index, Map<long[],long[]> data, Random random ) throws IOException
    {
        int changeCount = random.nextInt( 10 ) + 10;
        try ( SCInserter modifier = index.inserter() )
        {
            for ( int i = 0; i < changeCount; i++ )
            {
                if ( random.nextBoolean() && data.size() > 0 )
                {   // remove
                    long[] key = randomKey( data, random );
                    long[] value = data.remove( key );
                    long[] removedValue = modifier.remove( key );
                    assertArrayEquals( "For " + Arrays.toString( key ), value, removedValue );
                }
                else
                {   // insert
                    long[] key = randomTreeThing( random );
                    long[] value = randomTreeThing( random );
                    modifier.insert( key, value );
                    data.put( key, value );
                }
            }
        }
    }

    private long[] randomKey( Map<long[],long[]> data, Random random )
    {
        long[][] keys = data.keySet().toArray( new long[data.size()][] );
        return keys[random.nextInt( keys.length )];
    }

    private Map<long[],long[]> expectedHits( Map<long[],long[]> data, RangePredicate fromPredicate,
            RangePredicate toPredicate, Comparator<long[]> comparator )
    {
        Map<long[],long[]> hits = new TreeMap<>( comparator );
        for ( Map.Entry<long[],long[]> candidate : data.entrySet() )
        {
            if ( fromPredicate.inRange( candidate.getKey() ) >= 0 && toPredicate.inRange( candidate.getKey() ) <= 0 )
            {
                hits.put( candidate.getKey(), candidate.getValue() );
            }
        }
        return hits;
    }

    private long[] randomTreeThing( Random random )
    {
        return new long[] {random.nextInt( 1_000 ), random.nextInt( 1_000 )};
    }
}
