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

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.FormatCompatibilityVerifier;
import org.neo4j.test.rule.PageCacheConfig;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.index.internal.gbptree.SimpleLongLayout.longLayout;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@RunWith( Parameterized.class )
public class GBPTreeFormatTest<KEY,VALUE> extends FormatCompatibilityVerifier
{
    private static final String STORE = "store";
    private static final int INITIAL_KEY_COUNT = 10_000;
    private static final int PAGE_SIZE_8K = (int) ByteUnit.kibiBytes( 8 );
    private static final int PAGE_SIZE_16K = (int) ByteUnit.kibiBytes( 16 );
    private static final int PAGE_SIZE_32K = (int) ByteUnit.kibiBytes( 32 );
    private static final int PAGE_SIZE_64K = (int) ByteUnit.kibiBytes( 64 );
    private static final int PAGE_SIZE_4M = (int) ByteUnit.mebiBytes( 4 );
    private static final String CURRENT_FIXED_SIZE_FORMAT_8k_ZIP = "current-format_8k.zip";
    private static final String CURRENT_DYNAMIC_SIZE_FORMAT_8k_ZIP = "current-dynamic-format_8k.zip";
    private static final String CURRENT_FIXED_SIZE_FORMAT_16k_ZIP = "current-format_16k.zip";
    private static final String CURRENT_DYNAMIC_SIZE_FORMAT_16k_ZIP = "current-dynamic-format_16k.zip";
    private static final String CURRENT_FIXED_SIZE_FORMAT_32k_ZIP = "current-format_32k.zip";
    private static final String CURRENT_DYNAMIC_SIZE_FORMAT_32k_ZIP = "current-dynamic-format_32k.zip";
    private static final String CURRENT_FIXED_SIZE_FORMAT_64k_ZIP = "current-format_64k.zip";
    private static final String CURRENT_DYNAMIC_SIZE_FORMAT_64k_ZIP = "current-dynamic-format_64k.zip";
    private static final String CURRENT_FIXED_SIZE_FORMAT_4M_ZIP = "current-format_4M.zip";
    private static final String CURRENT_DYNAMIC_SIZE_FORMAT_4M_ZIP = "current-dynamic-format_4M.zip";

    @Parameters( name = "{1}" )
    public static List<Object[]> data()
    {
        return asList(
                // 8k
                new Object[]{longLayout().withFixedSize( true ).build(), CURRENT_FIXED_SIZE_FORMAT_8k_ZIP, PAGE_SIZE_8K},
                new Object[]{new SimpleByteArrayLayout( 4000, 99 ), CURRENT_DYNAMIC_SIZE_FORMAT_8k_ZIP, PAGE_SIZE_8K},
                // 16k
                new Object[]{longLayout().withFixedSize( true ).build(), CURRENT_FIXED_SIZE_FORMAT_16k_ZIP, PAGE_SIZE_16K},
                new Object[]{new SimpleByteArrayLayout( 4000, 99 ), CURRENT_DYNAMIC_SIZE_FORMAT_16k_ZIP, PAGE_SIZE_16K},
                // 32k
                new Object[]{longLayout().withFixedSize( true ).build(), CURRENT_FIXED_SIZE_FORMAT_32k_ZIP, PAGE_SIZE_32K},
                new Object[]{new SimpleByteArrayLayout( 4000, 99 ), CURRENT_DYNAMIC_SIZE_FORMAT_32k_ZIP, PAGE_SIZE_32K},
                // 64k
                new Object[]{longLayout().withFixedSize( true ).build(), CURRENT_FIXED_SIZE_FORMAT_64k_ZIP, PAGE_SIZE_64K},
                new Object[]{new SimpleByteArrayLayout( 4000, 99 ), CURRENT_DYNAMIC_SIZE_FORMAT_64k_ZIP, PAGE_SIZE_64K},
                // 4M
                new Object[]{longLayout().withFixedSize( true ).build(), CURRENT_FIXED_SIZE_FORMAT_4M_ZIP, PAGE_SIZE_4M},
                new Object[]{new SimpleByteArrayLayout( 4000, 99 ), CURRENT_DYNAMIC_SIZE_FORMAT_4M_ZIP, PAGE_SIZE_4M}
        );
    }

    private final TestLayout<KEY,VALUE> layout;
    private final String zipName;
    private final int pageSize;

    public GBPTreeFormatTest( TestLayout<KEY,VALUE> layout, String zipName, int pageSize )
    {
        this.layout = layout;
        this.zipName = zipName;
        this.pageSize = pageSize;
    }

    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final RandomRule random = new RandomRule();
    private final List<Long> initialKeys = initialKeys();
    private final List<Long> keysToAdd = keysToAdd();
    private List<Long> allKeys;
    private PageCache pageCache;

    @Before
    public void setup()
    {
        allKeys = new ArrayList<>();
        allKeys.addAll( initialKeys );
        allKeys.addAll( keysToAdd );
        allKeys.sort( Long::compare );
        PageCacheConfig overriddenConfig = PageCacheConfig.config().withPageSize( pageSize );
        if ( pageSize == PAGE_SIZE_4M )
        {
            overriddenConfig.withMemory( "16MiB" );
        }
        pageCache = pageCacheRule.getPageCache( globalFs, overriddenConfig );
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
    protected void createStoreFile( Path storeFile ) throws IOException
    {
        List<Long> initialKeys = initialKeys();
        try ( GBPTree<KEY,VALUE> tree = new GBPTreeBuilder<>( pageCache, storeFile, layout ).build() )
        {
            try ( Writer<KEY,VALUE> writer = tree.writer( NULL ) )
            {
                for ( Long key : initialKeys )
                {
                    put( writer, key );
                }
            }
            tree.checkpoint( IOLimiter.UNLIMITED, NULL );
        }
    }

    /**
     * Throws {@link FormatViolationException} if format has changed.
     */
    @SuppressWarnings( "EmptyTryBlock" )
    @Override
    protected void verifyFormat( Path storeFile ) throws IOException, FormatViolationException
    {
        try ( GBPTree<KEY,VALUE> ignored = new GBPTreeBuilder<>( pageCache, storeFile, layout ).build() )
        {
        }
        catch ( MetadataMismatchException e )
        {
            throw new FormatViolationException( e );
        }
    }

    @Override
    public void verifyContent( Path storeFile ) throws IOException
    {
        try ( GBPTree<KEY,VALUE> tree = new GBPTreeBuilder<>( pageCache, storeFile, layout ).build() )
        {
            {
                // WHEN reading from the tree
                // THEN initial keys should be there
                tree.consistencyCheck( NULL );
                try ( Seeker<KEY,VALUE> cursor = tree.seek( layout.key( 0 ), layout.key( Long.MAX_VALUE ), NULL ) )
                {
                    for ( Long expectedKey : initialKeys )
                    {
                        assertHit( cursor, layout, expectedKey );
                    }
                    assertFalse( cursor.next() );
                }
            }

            {
                // WHEN writing more to the tree
                // THEN we should not see any format conflicts
                try ( Writer<KEY,VALUE> writer = tree.writer( NULL ) )
                {
                    while ( keysToAdd.size() > 0 )
                    {
                        int next = random.nextInt( keysToAdd.size() );
                        Long key = keysToAdd.get( next );
                        put( writer, key );
                        keysToAdd.remove( next );
                    }
                }
            }

            {
                // WHEN reading from the tree again
                // THEN all keys including newly added should be there
                tree.consistencyCheck( NULL );
                try ( Seeker<KEY,VALUE> cursor = tree.seek( layout.key( 0 ), layout.key( 2 * INITIAL_KEY_COUNT ), NULL ) )
                {
                    for ( Long expectedKey : allKeys )
                    {
                        assertHit( cursor, layout, expectedKey );
                    }
                    assertFalse( cursor.next() );
                }
            }

            {
                // WHEN randomly removing half of tree content
                // THEN we should not see any format conflicts
                try ( Writer<KEY,VALUE> writer = tree.writer( NULL ) )
                {
                    int size = allKeys.size();
                    while ( allKeys.size() > size / 2 )
                    {
                        int next = random.nextInt( allKeys.size() );
                        KEY key = layout.key( allKeys.get( next ) );
                        writer.remove( key );
                        allKeys.remove( next );
                    }
                }
            }

            {
                // WHEN reading from the tree after remove
                // THEN we should see everything that is left in the tree
                tree.consistencyCheck( NULL );
                try ( Seeker<KEY,VALUE> cursor = tree.seek( layout.key( 0 ), layout.key( 2 * INITIAL_KEY_COUNT ), NULL ) )
                {
                    for ( Long expectedKey : allKeys )
                    {
                        assertHit( cursor, layout, expectedKey );
                    }
                    assertFalse( cursor.next() );
                }
            }
        }
    }

    private static long value( long key )
    {
        return (long) (key * 1.5);
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

    private static <KEY,VALUE> void assertHit( Seeker<KEY,VALUE> cursor, TestLayout<KEY,VALUE> layout, Long expectedKey ) throws IOException
    {
        assertTrue( "Had no next when expecting key " + expectedKey, cursor.next() );
        assertEquals( expectedKey.longValue(), layout.keySeed( cursor.key() ) );
        assertEquals( value( expectedKey ), layout.valueSeed( cursor.value() ) );
    }

    private void put( Writer<KEY,VALUE> writer, long key )
    {
        KEY insertKey = layout.key( key );
        VALUE insertValue = layout.value( value( key ) );
        writer.put( insertKey, insertValue );
    }
}
