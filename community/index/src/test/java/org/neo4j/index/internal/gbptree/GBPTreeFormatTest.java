/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.cursor.RawCursor;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.FormatCompatibilityVerifier;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.index.internal.gbptree.SimpleLongLayout.longLayout;

@RunWith( Parameterized.class )
public class GBPTreeFormatTest extends FormatCompatibilityVerifier
{
    private static final String STORE = "store";
    private static final int INITIAL_KEY_COUNT = 10_000;
    private static final String CURRENT_FIXED_SIZE_FORMAT_ZIP = "current-format.zip";
    private static final String CURRENT_DYNAMIC_SIZE_FORMAT_ZIP = "current-dynamic-format.zip";

    @Parameters
    public static List<Object[]> data()
    {
        return asList(
                new Object[] {longLayout().withFixedSize( true ).build(), CURRENT_FIXED_SIZE_FORMAT_ZIP},
                new Object[] {longLayout().withFixedSize( false ).build(), CURRENT_DYNAMIC_SIZE_FORMAT_ZIP} );
    }

    @Parameter
    public SimpleLongLayout layout;
    @Parameter( 1 )
    public String zipName;

    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final RandomRule random = new RandomRule();
    private final List<Long> initialKeys = initialKeys();
    private final List<Long> keysToAdd = keysToAdd();
    private List<Long> allKeys;

    @Before
    public void setup()
    {
        allKeys = new ArrayList<>();
        allKeys.addAll( initialKeys );
        allKeys.addAll( keysToAdd );
        allKeys.sort( Long::compare );
    }

    @Rule
    public final RuleChain chain = RuleChain.outerRule( random ).around( pageCacheRule );

    @Override
    protected String zipName()
    {
        return zipName;
    }

    @Override
    protected String storeFileName()
    {
        return STORE;
    }

    @Override
    protected void createStoreFile( File storeFile ) throws IOException
    {
        List<Long> initialKeys = initialKeys();
        PageCache pageCache = pageCacheRule.getPageCache( globalFs.get() );
        try ( GBPTree<MutableLong,MutableLong> tree =
                      new GBPTreeBuilder<>( pageCache, storeFile, layout ).build() )
        {
            try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
            {
                for ( Long key : initialKeys )
                {
                    put( writer, key );
                }
            }
            tree.checkpoint( IOLimiter.UNLIMITED );
        }
    }

    /**
     * Throws {@link FormatViolationException} if format has changed.
     */
    @SuppressWarnings( "EmptyTryBlock" )
    @Override
    protected void verifyFormat( File storeFile ) throws IOException, FormatViolationException
    {
        PageCache pageCache = pageCacheRule.getPageCache( globalFs.get() );
        try ( GBPTree<MutableLong,MutableLong> ignored =
                      new GBPTreeBuilder<>( pageCache, storeFile, layout ).build() )
        {
        }
        catch ( MetadataMismatchException e )
        {
            throw new FormatViolationException( e );
        }
    }

    @Override
    public void verifyContent( File storeFile ) throws IOException
    {
        PageCache pageCache = pageCacheRule.getPageCache( globalFs.get() );
        try ( GBPTree<MutableLong,MutableLong> tree =
                      new GBPTreeBuilder<>( pageCache, storeFile, layout ).build() )
        {
            {
                // WHEN reading from the tree
                // THEN initial keys should be there
                tree.consistencyCheck();
                try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor =
                              tree.seek( layout.key( 0 ), layout.key( Long.MAX_VALUE ) ) )
                {
                    for ( Long expectedKey : initialKeys )
                    {
                        assertHit( cursor, expectedKey );
                    }
                    assertFalse( cursor.next() );
                }
            }

            {
                // WHEN writing more to the tree
                // THEN we should not see any format conflicts
                try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
                {
                    while ( keysToAdd.size() > 0 )
                    {
                        int next = random.nextInt( keysToAdd.size() );
                        put( writer, keysToAdd.get( next ) );
                        keysToAdd.remove( next );
                    }
                }
            }

            {
                // WHEN reading from the tree again
                // THEN all keys including newly added should be there
                tree.consistencyCheck();
                try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor =
                              tree.seek( layout.key( 0 ), layout.key( 2 * INITIAL_KEY_COUNT ) ) )
                {
                    for ( Long expectedKey : allKeys )
                    {
                        assertHit( cursor, expectedKey );
                    }
                    assertFalse( cursor.next() );
                }
            }

            {
                // WHEN randomly removing half of tree content
                // THEN we should not see any format conflicts
                try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
                {
                    int size = allKeys.size();
                    while ( allKeys.size() > size / 2 )
                    {
                        int next = random.nextInt( allKeys.size() );
                        MutableLong key = layout.key( allKeys.get( next ) );
                        writer.remove( key );
                        allKeys.remove( next );
                    }
                }
            }

            {
                // WHEN reading from the tree after remove
                // THEN we should see everything that is left in the tree
                tree.consistencyCheck();
                try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor =
                              tree.seek( layout.key( 0 ), layout.key( 2 * INITIAL_KEY_COUNT ) ) )
                {
                    for ( Long expectedKey : allKeys )
                    {
                        assertHit( cursor, expectedKey );
                    }
                    assertFalse( cursor.next() );
                }
            }
        }
    }

    private static long value( long key )
    {
        return key * 2;
    }

    private static List<Long> initialKeys()
    {
        List<Long> initialKeys = new ArrayList<>();
        for ( long i = 0, key = 0; i < INITIAL_KEY_COUNT; i++, key += 2 )
        {
            initialKeys.add( key );
        }
        return initialKeys;
    }

    private static List<Long> keysToAdd()
    {
        List<Long> keysToAdd = new ArrayList<>();
        for ( long i = 0, key = 1; i < INITIAL_KEY_COUNT; i++, key += 2 )
        {
            keysToAdd.add( key );
        }
        return keysToAdd;
    }

    private static void assertHit( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor, Long expectedKey ) throws IOException
    {
        assertTrue( "Had no next when expecting key " + expectedKey, cursor.next() );
        Hit<MutableLong,MutableLong> hit = cursor.get();
        assertEquals( expectedKey.longValue(), hit.key().longValue() );
        assertEquals( value( expectedKey ), hit.value().longValue() );
    }

    private void put( Writer<MutableLong,MutableLong> writer, long key )
    {
        MutableLong insertKey = layout.key( key );
        MutableLong insertValue = layout.value( value( key ) );
        writer.put( insertKey, insertValue );
    }
}
