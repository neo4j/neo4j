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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    public void shouldSeekToFind() throws Exception
    {
        // GIVEN
        try ( Index index = new Index( pageCache, indexFile, description, pageSize ) )
        {
            Map<long[],long[]> data = new TreeMap<>( BTreeNode.KEY_COMPARATOR );
            Random random = ThreadLocalRandom.current();
            int count = 1_000;
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
                Map<long[],long[]> expectedHits = expectedHits( data, fromPredicate, toPredicate );
                try ( Cursor<BTreeHit> result = index.seek( fromPredicate, toPredicate ) )
                {
                    while ( result.next() )
                    {
                        long[] key = result.get().key();
                        assertTrue( expectedHits.remove( key ) != null );
                        assertTrue( fromPredicate.inRange( key ) >= 0 );
                        assertTrue( toPredicate.inRange( key ) <= 0 ); // apparently "lower" range predicate
                                                                       // returns 0 for equal ids, even if prop is lower
                    }
                    assertTrue( expectedHits.isEmpty() );
                }
            }
        }
    }

    private Map<long[],long[]> expectedHits( Map<long[],long[]> data, RangePredicate fromPredicate,
            RangePredicate toPredicate )
    {
        Map<long[],long[]> hits = new TreeMap<>( BTreeNode.KEY_COMPARATOR );
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
