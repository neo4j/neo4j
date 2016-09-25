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
import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
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
import static org.junit.Assert.fail;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.index.ValueAmenders.overwrite;
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
        pageCache = new MuninnPageCache( swapperFactory, 100, pageSize, NULL );
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
        TreeItemLayout<TwoLongs,TwoLongs> layout = new PathIndexLayout();
        try ( Index<TwoLongs,TwoLongs> index = new Index<>( pageCache, indexFile, layout, description, pageSize ) )
        {   // Open/close is enough
        }

        // WHEN
        try ( Index<TwoLongs,TwoLongs> index = new Index<>( pageCache, indexFile, layout ) )
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
        try ( Index<TwoLongs,TwoLongs> index =
                new Index<>( pageCache, indexFile, new PathIndexLayout(), description, pageSize ) )
        {
            Comparator<TwoLongs> keyComparator = index.getTreeNode().keyComparator();
            Map<TwoLongs,TwoLongs> data = new TreeMap<>( keyComparator );
            long seed = currentTimeMillis();
            Random random = new Random( seed );
            int count = 1000;
            for ( int i = 0; i < count; i++ )
            {
                data.put( randomTreeThing( random ), randomTreeThing( random ) );
            }

            // WHEN
            try ( SCInserter<TwoLongs,TwoLongs> inserter = index.inserter() )
            {
                for ( Map.Entry<TwoLongs,TwoLongs> entry : data.entrySet() )
                {
                    inserter.insert( entry.getKey(), entry.getValue() );
                }
            }

            for ( int round = 0; round < 10; round++ )
            {
                // THEN
                for ( int i = 0; i < count; i++ )
                {
                    TwoLongs first = randomTreeThing( random );
                    TwoLongs second = randomTreeThing( random );
                    TwoLongs from, to;
                    if ( first.first < second.first )
                    {
                        from = first;
                        to = second;
                    }
                    else
                    {
                        from = second;
                        to = first;
                    }
                    Map<TwoLongs,TwoLongs> expectedHits = expectedHits( data, from, to, keyComparator );
                    try ( Cursor<BTreeHit<TwoLongs,TwoLongs>> result = index.seek( from, to ) )
                    {
                        while ( result.next() )
                        {
                            TwoLongs key = result.get().key();
                            if ( expectedHits.remove( key ) == null )
                            {
                                index.printTree();
                                fail( "Unexpected hit " + key + " when searching for " + from + " - " + to );
                            }

                            assertTrue( keyComparator.compare( key, from ) >= 0 );
                            assertTrue( keyComparator.compare( key, to ) < 0 );
                        }
                        if ( !expectedHits.isEmpty() )
                        {
                            fail( "There were results which were expected to be returned, but weren't:" + expectedHits +
                                    " when searching range " + from + " - " + to );
                        }
                    }
                }

                randomlyModifyIndex( index, data, random );
            }
        }
    }

    private void randomlyModifyIndex( Index<TwoLongs,TwoLongs> index, Map<TwoLongs,TwoLongs> data, Random random )
            throws IOException
    {
        int changeCount = random.nextInt( 10 ) + 10;
        try ( SCInserter<TwoLongs,TwoLongs> modifier = index.inserter() )
        {
            for ( int i = 0; i < changeCount; i++ )
            {
                if ( random.nextBoolean() && data.size() > 0 )
                {   // remove
                    TwoLongs key = randomKey( data, random );
                    TwoLongs value = data.remove( key );
                    TwoLongs removedValue = modifier.remove( key );
                    assertEquals( "For " + key, value, removedValue );
                }
                else
                {   // insert
                    TwoLongs key = randomTreeThing( random );
                    TwoLongs value = randomTreeThing( random );
                    modifier.insert( key, value, overwrite() );
                    data.put( key, value );
                }
            }
        }
    }

    private TwoLongs randomKey( Map<TwoLongs,TwoLongs> data, Random random )
    {
        TwoLongs[] keys = data.keySet().toArray( new TwoLongs[data.size()] );
        return keys[random.nextInt( keys.length )];
    }

    private Map<TwoLongs,TwoLongs> expectedHits( Map<TwoLongs,TwoLongs> data, TwoLongs from, TwoLongs to,
            Comparator<TwoLongs> comparator )
    {
        Map<TwoLongs,TwoLongs> hits = new TreeMap<>( comparator );
        for ( Map.Entry<TwoLongs,TwoLongs> candidate : data.entrySet() )
        {
            if ( comparator.compare( candidate.getKey(), from ) >= 0 &&
                    comparator.compare( candidate.getKey(), to ) < 0 )
            {
                hits.put( candidate.getKey(), candidate.getValue() );
            }
        }
        return hits;
    }

    private TwoLongs randomTreeThing( Random random )
    {
        return new TwoLongs( random.nextInt( 1_000 ), random.nextInt( 1_000 ) );
    }
}
