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
package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.cursor.RawCursor;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.rules.RuleChain.outerRule;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.test.rule.PageCacheRule.config;

public class GBPTreeIT
{
    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( getClass(), fs.get() );
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final RandomRule random = new RandomRule();

    @Rule
    public final RuleChain rules = outerRule( fs ).around( directory ).around( pageCacheRule ).around( random );

    private final Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();
    private GBPTree<MutableLong,MutableLong> index;
    private final ExecutorService threadPool = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() );
    private PageCache pageCache;

    private GBPTree<MutableLong,MutableLong> createIndex( int pageSize )
            throws IOException
    {
        return createIndex( pageSize, NO_MONITOR );
    }

    private GBPTree<MutableLong,MutableLong> createIndex( int pageSize, GBPTree.Monitor monitor )
            throws IOException
    {
        pageCache = pageCacheRule.getPageCache( fs.get(), config().withPageSize( pageSize ).withAccessChecks( true ) );
        return index = new GBPTreeBuilder<>( pageCache, directory.file( "index" ), layout ).build();
    }

    @After
    public void consistencyCheckAndClose() throws IOException
    {
        try
        {
            threadPool.shutdownNow();
            index.consistencyCheck();
        }
        finally
        {
            index.close();
        }
    }

    @Test
    public void shouldStayCorrectAfterRandomModifications() throws Exception
    {
        // GIVEN
        GBPTree<MutableLong,MutableLong> index = createIndex( 256 );
        Comparator<MutableLong> keyComparator = layout;
        Map<MutableLong,MutableLong> data = new TreeMap<>( keyComparator );
        int count = 100;
        int totalNumberOfRounds = 10;
        for ( int i = 0; i < count; i++ )
        {
            data.put( randomKey( random.random() ), randomKey( random.random() ) );
        }

        // WHEN
        try ( Writer<MutableLong,MutableLong> writer = index.writer() )
        {
            for ( Map.Entry<MutableLong,MutableLong> entry : data.entrySet() )
            {
                writer.put( entry.getKey(), entry.getValue() );
            }
        }

        for ( int round = 0; round < totalNumberOfRounds; round++ )
        {
            // THEN
            for ( int i = 0; i < count; i++ )
            {
                MutableLong first = randomKey( random.random() );
                MutableLong second = randomKey( random.random() );
                MutableLong from;
                MutableLong to;
                if ( first.longValue() < second.longValue() )
                {
                    from = first;
                    to = second;
                }
                else
                {
                    from = second;
                    to = first;
                }
                Map<MutableLong,MutableLong> expectedHits = expectedHits( data, from, to, keyComparator );
                try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> result = index.seek( from, to ) )
                {
                    while ( result.next() )
                    {
                        MutableLong key = result.get().key();
                        if ( expectedHits.remove( key ) == null )
                        {
                            fail( "Unexpected hit " + key + " when searching for " + from + " - " + to );
                        }

                        assertTrue( keyComparator.compare( key, from ) >= 0 );
                        if ( keyComparator.compare( from, to ) != 0 )
                        {
                            assertTrue( keyComparator.compare( key, to ) < 0 );
                        }
                    }
                    if ( !expectedHits.isEmpty() )
                    {
                        fail( "There were results which were expected to be returned, but weren't:" + expectedHits +
                                " when searching range " + from + " - " + to );
                    }
                }
            }

            index.checkpoint( IOLimiter.unlimited() );
            randomlyModifyIndex( index, data, random.random(), (double) round / totalNumberOfRounds );
        }
    }

    private static void randomlyModifyIndex( GBPTree<MutableLong,MutableLong> index,
            Map<MutableLong,MutableLong> data, Random random, double removeProbability ) throws IOException
    {
        int changeCount = random.nextInt( 10 ) + 10;
        try ( Writer<MutableLong,MutableLong> writer = index.writer() )
        {
            for ( int i = 0; i < changeCount; i++ )
            {
                if ( random.nextDouble() < removeProbability && data.size() > 0 )
                {   // remove
                    MutableLong key = randomKey( data, random );
                    MutableLong value = data.remove( key );
                    MutableLong removedValue = writer.remove( key );
                    assertEquals( "For " + key, value, removedValue );
                }
                else
                {   // put
                    MutableLong key = randomKey( random );
                    MutableLong value = randomKey( random );
                    writer.put( key, value );
                    data.put( key, value );
                }
            }
        }
    }

    private static Map<MutableLong,MutableLong> expectedHits( Map<MutableLong,MutableLong> data,
            MutableLong from, MutableLong to, Comparator<MutableLong> comparator )
    {
        Map<MutableLong,MutableLong> hits = new TreeMap<>( comparator );
        for ( Map.Entry<MutableLong,MutableLong> candidate : data.entrySet() )
        {
            if ( comparator.compare( from, to ) == 0 && comparator.compare( candidate.getKey(), from ) == 0 )
            {
                hits.put( candidate.getKey(), candidate.getValue() );
            }
            else if ( comparator.compare( candidate.getKey(), from ) >= 0 &&
                    comparator.compare( candidate.getKey(), to ) < 0 )
            {
                hits.put( candidate.getKey(), candidate.getValue() );
            }
        }
        return hits;
    }

    private static MutableLong randomKey( Map<MutableLong,MutableLong> data, Random random )
    {
        MutableLong[] keys = data.keySet().toArray( new MutableLong[data.size()] );
        return keys[random.nextInt( keys.length )];
    }

    private static MutableLong randomKey( Random random )
    {
        return new MutableLong( random.nextInt( 1_000 ) );
    }
}
