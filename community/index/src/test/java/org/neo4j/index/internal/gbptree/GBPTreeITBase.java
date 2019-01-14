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
package org.neo4j.index.internal.gbptree;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.neo4j.cursor.RawCursor;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.Integer.max;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.rules.RuleChain.outerRule;
import static org.neo4j.test.rule.PageCacheRule.config;

public abstract class GBPTreeITBase<KEY,VALUE>
{
    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( getClass(), fs.get() );
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    final RandomRule random = new RandomRule();

    @Rule
    public final RuleChain rules = outerRule( fs ).around( directory ).around( pageCacheRule ).around( random );

    private TestLayout<KEY,VALUE> layout;
    private GBPTree<KEY,VALUE> index;

    private GBPTree<KEY,VALUE> createIndex()
            throws IOException
    {
        // some random padding
        layout = getLayout( random );
        PageCache pageCache = pageCacheRule.getPageCache( fs.get(), config().withPageSize( 512 ).withAccessChecks( true ) );
        return index = new GBPTreeBuilder<>( pageCache, directory.file( "index" ), layout ).build();
    }

    abstract TestLayout<KEY,VALUE> getLayout( RandomRule random );

    abstract Class<KEY> getKeyClass();

    @Test
    public void shouldStayCorrectAfterRandomModifications() throws Exception
    {
        // GIVEN
        try ( GBPTree<KEY,VALUE> index = createIndex() )
        {
            Comparator<KEY> keyComparator = layout;
            Map<KEY,VALUE> data = new TreeMap<>( keyComparator );
            int count = 100;
            int totalNumberOfRounds = 10;
            for ( int i = 0; i < count; i++ )
            {
                data.put( randomKey( random.random() ), randomValue( random.random() ) );
            }

            // WHEN
            try ( Writer<KEY,VALUE> writer = index.writer() )
            {
                for ( Map.Entry<KEY,VALUE> entry : data.entrySet() )
                {
                    writer.put( entry.getKey(), entry.getValue() );
                }
            }

            for ( int round = 0; round < totalNumberOfRounds; round++ )
            {
                // THEN
                for ( int i = 0; i < count; i++ )
                {
                    KEY first = randomKey( random.random() );
                    KEY second = randomKey( random.random() );
                    KEY from;
                    KEY to;
                    if ( layout.keySeed( first ) < layout.keySeed( second ) )
                    {
                        from = first;
                        to = second;
                    }
                    else
                    {
                        from = second;
                        to = first;
                    }
                    Map<KEY,VALUE> expectedHits = expectedHits( data, from, to, keyComparator );
                    try ( RawCursor<Hit<KEY,VALUE>,IOException> result = index.seek( from, to ) )
                    {
                        while ( result.next() )
                        {
                            KEY key = result.get().key();
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

            // and finally
            index.consistencyCheck();
        }
    }

    @Test
    public void shouldHandleRemoveEntireTree() throws Exception
    {
        // given
        try ( GBPTree<KEY,VALUE> index = createIndex() )
        {
            int numberOfNodes = 200_000;
            try ( Writer<KEY,VALUE> writer = index.writer() )
            {
                for ( int i = 0; i < numberOfNodes; i++ )
                {
                    writer.put( key( i ), value( i ) );
                }
            }

            // when
            BitSet removed = new BitSet();
            try ( Writer<KEY,VALUE> writer = index.writer() )
            {
                for ( int i = 0; i < numberOfNodes - numberOfNodes / 10; i++ )
                {
                    int candidate;
                    do
                    {
                        candidate = random.nextInt( max( 1, random.nextInt( numberOfNodes ) ) );
                    }
                    while ( removed.get( candidate ) );
                    removed.set( candidate );

                    writer.remove( key( candidate ) );
                }
            }

            int next = 0;
            try ( Writer<KEY,VALUE> writer = index.writer() )
            {
                for ( int i = 0; i < numberOfNodes / 10; i++ )
                {
                    next = removed.nextClearBit( next );
                    removed.set( next );
                    writer.remove( key( next ) );
                }
            }

            // then
            try ( RawCursor<Hit<KEY,VALUE>,IOException> seek = index.seek( key( 0 ), key( numberOfNodes ) ) )
            {
                assertFalse( seek.next() );
            }

            // and finally
            index.consistencyCheck();
        }
    }

    private void randomlyModifyIndex( GBPTree<KEY,VALUE> index, Map<KEY,VALUE> data, Random random, double removeProbability )
            throws IOException
    {
        int changeCount = random.nextInt( 10 ) + 10;
        try ( Writer<KEY,VALUE> writer = index.writer() )
        {
            for ( int i = 0; i < changeCount; i++ )
            {
                if ( random.nextDouble() < removeProbability && data.size() > 0 )
                {   // remove
                    KEY key = randomKey( data, random );
                    VALUE value = data.remove( key );
                    VALUE removedValue = writer.remove( key );
                    assertEqualsValue( value, removedValue );
                }
                else
                {   // put
                    KEY key = randomKey( random );
                    VALUE value = randomValue( random );
                    writer.put( key, value );
                    data.put( key, value );
                }
            }
        }
    }

    private Map<KEY,VALUE> expectedHits( Map<KEY,VALUE> data, KEY from, KEY to, Comparator<KEY> comparator )
    {
        Map<KEY,VALUE> hits = new TreeMap<>( comparator );
        for ( Map.Entry<KEY,VALUE> candidate : data.entrySet() )
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

    private KEY randomKey( Map<KEY,VALUE> data, Random random )
    {
        //noinspection unchecked
        KEY[] keys = data.keySet().toArray( (KEY[]) Array.newInstance( getKeyClass(), data.size() ) );
        return keys[random.nextInt( keys.length )];
    }

    private KEY randomKey( Random random )
    {
        return key( random.nextInt( 1_000 ) );
    }

    private VALUE randomValue( Random random )
    {
        return value( random.nextInt( 1_000 ) );
    }

    private VALUE value( long seed )
    {
        return layout.value( seed );
    }

    private KEY key( long seed )
    {
        return layout.key( seed );
    }

    private void assertEqualsValue( VALUE expected, VALUE actual )
    {
        assertTrue( String.format( "expected equal, expected=%s, actual=%s", expected.toString(), actual.toString() ),
                layout.compareValue( expected, actual ) == 0 );
    }

    // KEEP even if unused
    @SuppressWarnings( "unused" )
    private void printTree() throws IOException
    {
        index.printTree( false, false, false, false );
    }

    @SuppressWarnings( "unused" )
    private void printNode( @SuppressWarnings( "SameParameterValue" ) int id ) throws IOException
    {
        index.printNode( id );
    }
}
