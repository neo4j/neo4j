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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.cursor.RawCursor;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.string.UTF8;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.index.internal.gbptree.TreeNodeDynamicSize.keyValueSizeCapFromPageSize;
import static org.neo4j.io.pagecache.PageCache.PAGE_SIZE;
import static org.neo4j.test.Randoms.CSA_LETTERS_AND_DIGITS;
import static org.neo4j.test.rule.PageCacheRule.config;

public class LargeDynamicKeysIT
{
    @Rule
    public final PageCacheAndDependenciesRule storage = new PageCacheAndDependenciesRule( DefaultFileSystemRule::new, getClass() );

    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void mustStayCorrectWhenInsertingValuesOfIncreasingLength() throws IOException
    {
        Layout<RawBytes,RawBytes> layout = layout();
        try ( GBPTree<RawBytes,RawBytes> index = createIndex( layout );
              Writer<RawBytes,RawBytes> writer = index.writer() )
        {
            RawBytes emptyValue = layout.newValue();
            emptyValue.bytes = new byte[0];
            for ( int keySize = 1; keySize < index.keyValueSizeCap(); keySize++ )
            {
                RawBytes key = layout.newKey();
                key.bytes = new byte[keySize];
                writer.put( key, emptyValue );
            }
        }
    }

    @Test
    public void shouldWriteAndReadSmallToSemiLargeEntries() throws IOException
    {
        int keyValueSizeCap = keyValueSizeCapFromPageSize( PAGE_SIZE );
        int minValueSize = 0;
        int maxValueSize = random.nextInt( 200 );
        int minKeySize = 4;
        int maxKeySize = keyValueSizeCap / 5;
        shouldWriteAndReadEntriesOfRandomSizes( minKeySize, maxKeySize, minValueSize, maxValueSize );
    }

    @Test
    public void shouldWriteAndReadSmallToLargeEntries() throws IOException
    {
        int keyValueSizeCap = keyValueSizeCapFromPageSize( PAGE_SIZE );
        int minValueSize = 0;
        int maxValueSize = random.nextInt( 200 );
        int minKeySize = 4;
        int maxKeySize = keyValueSizeCap - maxValueSize;
        shouldWriteAndReadEntriesOfRandomSizes( minKeySize, maxKeySize, minValueSize, maxValueSize );
    }

    @Test
    public void shouldWriteAndReadSemiLargeToLargeEntries() throws IOException
    {
        int keyValueSizeCap = keyValueSizeCapFromPageSize( PAGE_SIZE );
        int minValueSize = 0;
        int maxValueSize = random.nextInt( 200 );
        int minKeySize = keyValueSizeCap / 5;
        int maxKeySize = keyValueSizeCap - maxValueSize;
        shouldWriteAndReadEntriesOfRandomSizes( minKeySize, maxKeySize, minValueSize, maxValueSize );
    }

    private void shouldWriteAndReadEntriesOfRandomSizes( int minKeySize, int maxKeySize, int minValueSize, int maxValueSize ) throws IOException
    {
        // given
        try ( GBPTree<RawBytes,RawBytes> tree = createIndex( layout() ) )
        {
            // when
            Set<String> generatedStrings = new HashSet<>();
            List<Pair<RawBytes,RawBytes>> entries = new ArrayList<>();
            try ( Writer<RawBytes,RawBytes> writer = tree.writer() )
            {
                for ( int i = 0; i < 1_000; i++ )
                {
                    // value, based on i
                    RawBytes value = new RawBytes();
                    value.bytes = new byte[random.nextInt( minValueSize, maxValueSize )];
                    random.nextBytes( value.bytes );

                    // key, randomly generated
                    String string;
                    do
                    {
                        string = random.string( minKeySize, maxKeySize, CSA_LETTERS_AND_DIGITS );
                    }
                    while ( !generatedStrings.add( string ) );
                    RawBytes key = new RawBytes();
                    key.bytes = UTF8.encode( string );
                    entries.add( Pair.of( key, value ) );

                    // write
                    writer.put( key, value );
                }
            }

            // then
            for ( Pair<RawBytes,RawBytes> entry : entries )
            {
                try ( RawCursor<Hit<RawBytes,RawBytes>,IOException> seek = tree.seek( entry.first(), entry.first() ) )
                {
                    assertTrue( seek.next() );
                    assertArrayEquals( entry.first().bytes, seek.get().key().bytes );
                    assertArrayEquals( entry.other().bytes, seek.get().value().bytes );
                    assertFalse( seek.next() );
                }
            }
        }
    }

    private SimpleByteArrayLayout layout()
    {
        return new SimpleByteArrayLayout( false );
    }

    private GBPTree<RawBytes,RawBytes> createIndex( Layout<RawBytes,RawBytes> layout ) throws IOException
    {
        // some random padding
        PageCache pageCache = storage.pageCacheRule().getPageCache( storage.fileSystem(), config().withAccessChecks( true ) );
        return new GBPTreeBuilder<>( pageCache, storage.directory().file( "index" ), layout ).build();
    }
}
