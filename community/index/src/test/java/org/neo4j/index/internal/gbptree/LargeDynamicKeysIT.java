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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.string.UTF8;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.TreeNodeDynamicSize.keyValueSizeCapFromPageSize;
import static org.neo4j.io.pagecache.PageCache.PAGE_SIZE;

@PageCacheExtension
@ExtendWith( RandomExtension.class )
class LargeDynamicKeysIT
{
    private static final Layout<RawBytes,RawBytes> layout = new SimpleByteArrayLayout( false );

    @Inject
    private RandomRule random;
    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory testDirectory;

    @Test
    void putSingleKeyLargeThanInlineCap() throws IOException
    {
        try ( GBPTree<RawBytes,RawBytes> tree = createIndex() )
        {
            int keySize = tree.inlineKeyValueSizeCap();
            RawBytes key = key( keySize + 1 );
            RawBytes value = value( 0 );
            try ( Writer<RawBytes,RawBytes> writer = tree.writer() )
            {
                writer.put( key, value );
            }
            assertFindExact( tree, key, value );
        }
    }

    @Test
    void removeSingleKeyLargeThanInlineCap() throws IOException
    {
        try ( GBPTree<RawBytes,RawBytes> tree = createIndex() )
        {
            int keySize = tree.inlineKeyValueSizeCap();
            RawBytes key = key( keySize + 1 );
            RawBytes value = value( 0 );
            try ( Writer<RawBytes,RawBytes> writer = tree.writer() )
            {
                writer.put( key, value );
                writer.remove( key );
            }
            assertDontFind( tree, key );
        }
    }

    @Test
    void putSingleKeyOnKeyValueSizeCap() throws IOException
    {
        try ( GBPTree<RawBytes,RawBytes> tree = createIndex() )
        {
            int keySize = tree.keyValueSizeCap();
            RawBytes key = key( keySize );
            RawBytes value = value( 0 );
            try ( Writer<RawBytes,RawBytes> writer = tree.writer() )
            {
                writer.put( key, value );
            }
            assertFindExact( tree, key, value );
        }
    }

    @Test
    void mustThrowWhenPutSingleKeyLargerThanKeyValueSizeCap() throws IOException
    {
        try ( GBPTree<RawBytes,RawBytes> tree = createIndex() )
        {
            int keySize = tree.keyValueSizeCap() + 1;
            RawBytes key = key( keySize );
            RawBytes value = value( 0 );
            try ( Writer<RawBytes,RawBytes> writer = tree.writer() )
            {
                assertThrows( IllegalArgumentException.class, () -> writer.put( key, value ) );
            }
        }
    }

    @Test
    void putAndRemoveRandomlyDistributedKeys() throws IOException
    {
        try ( GBPTree<RawBytes,RawBytes> tree = createIndex() )
        {
            int keyValueSizeOverflow = tree.keyValueSizeCap() + 1;

            RawBytes value = value( 0 );
            List<Pair<RawBytes,RawBytes>> entries = new ArrayList<>();
            for ( int i = 0; i < 10_000; i++ )
            {
                int keySize = inValidRange( 4, keyValueSizeOverflow, random.nextInt( keyValueSizeOverflow ) );
                entries.add( Pair.of( key( keySize, asBytes( i ) ), value ) );
            }
            Collections.shuffle( entries, random.random() );
            insertAndValidate( tree, entries );
            tree.consistencyCheck();
            removeAndValidate( tree, entries );
            tree.consistencyCheck();
        }
    }

    @Test
    void mustStayCorrectWhenInsertingValuesOfIncreasingLength() throws IOException
    {
        mustStayCorrectWhenInsertingValuesOfIncreasingLength( false );
    }

    @Test
    void mustStayCorrectWhenInsertingValuesOfIncreasingLengthInRandomOrder() throws IOException
    {
        mustStayCorrectWhenInsertingValuesOfIncreasingLength( true );
    }

    private void mustStayCorrectWhenInsertingValuesOfIncreasingLength( boolean shuffle ) throws IOException
    {
        try ( GBPTree<RawBytes,RawBytes> index = createIndex() )
        {
            RawBytes emptyValue = layout.newValue();
            emptyValue.bytes = new byte[0];
            List<Pair<RawBytes,RawBytes>> entries = new ArrayList<>();
            for ( int keySize = 1; keySize < index.keyValueSizeCap(); keySize++ )
            {
                entries.add( Pair.of( key( keySize ), emptyValue ) );
            }
            if ( shuffle )
            {
                Collections.shuffle( entries, random.random() );
            }

            insertAndValidate( index, entries );
            index.consistencyCheck();
        }
    }

    @Test
    void shouldWriteAndReadSmallToSemiLargeEntries() throws IOException
    {
        int keyValueSizeCap = keyValueSizeCapFromPageSize( PAGE_SIZE );
        int minValueSize = 0;
        int maxValueSize = random.nextInt( 200 );
        int minKeySize = 4;
        int maxKeySize = keyValueSizeCap / 5;
        shouldWriteAndReadEntriesOfRandomSizes( minKeySize, maxKeySize, minValueSize, maxValueSize );
    }

    @Test
    void shouldWriteAndReadSmallToLargeEntries() throws IOException
    {
        int keyValueSizeCap = keyValueSizeCapFromPageSize( PAGE_SIZE );
        int minValueSize = 0;
        int maxValueSize = random.nextInt( 200 );
        int minKeySize = 4;
        int maxKeySize = keyValueSizeCap - maxValueSize;
        shouldWriteAndReadEntriesOfRandomSizes( minKeySize, maxKeySize, minValueSize, maxValueSize );
    }

    @Test
    void shouldWriteAndReadSemiLargeToLargeEntries() throws IOException
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
        try ( GBPTree<RawBytes,RawBytes> tree = createIndex() )
        {
            // when
            Set<String> generatedStrings = new HashSet<>();
            List<Pair<RawBytes,RawBytes>> entries = new ArrayList<>();
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
                    string = random.nextAlphaNumericString( minKeySize, maxKeySize );
                }
                while ( !generatedStrings.add( string ) );
                RawBytes key = new RawBytes();
                key.bytes = UTF8.encode( string );
                entries.add( Pair.of( key, value ) );
            }

            insertAndValidate( tree, entries );
            tree.consistencyCheck();
        }
    }

    private void insertAndValidate( GBPTree<RawBytes,RawBytes> tree, List<Pair<RawBytes,RawBytes>> entries ) throws IOException
    {
        processWithCheckpoints( tree, entries, ( writer, entry ) -> writer.put( entry.first(), entry.other() ) );

        for ( Pair<RawBytes,RawBytes> entry : entries )
        {
            assertFindExact( tree, entry.first(), entry.other() );
        }
    }

    private void removeAndValidate( GBPTree<RawBytes,RawBytes> tree, List<Pair<RawBytes,RawBytes>> entries ) throws IOException
    {
        processWithCheckpoints( tree, entries, ( writer, entry ) ->
        {
            RawBytes removed = writer.remove( entry.first() );
            assertEquals( 0, layout.compare( removed, entry.other() ) );
        } );

        for ( Pair<RawBytes,RawBytes> entry : entries )
        {
            assertDontFind( tree, entry.first() );
        }
    }

    private void processWithCheckpoints( GBPTree<RawBytes,RawBytes> tree, List<Pair<RawBytes,RawBytes>> entries,
            BiConsumer<Writer<RawBytes,RawBytes>,Pair<RawBytes,RawBytes>> writerAction )
            throws IOException
    {
        double checkpointFrequency = 0.05;
        Iterator<Pair<RawBytes,RawBytes>> iterator = entries.iterator();
        while ( iterator.hasNext() )
        {
            try ( Writer<RawBytes,RawBytes> writer = tree.writer() )
            {
                while ( iterator.hasNext() && random.nextDouble() > checkpointFrequency )
                {
                    Pair<RawBytes,RawBytes> entry = iterator.next();
                    writerAction.accept( writer, entry );
                }
            }
            tree.checkpoint( IOLimiter.UNLIMITED );
        }
    }

    private void assertDontFind( GBPTree<RawBytes,RawBytes> tree, RawBytes key ) throws IOException
    {
        try ( Seeker<RawBytes,RawBytes> seek = tree.seek( key, key ) )
        {
            assertFalse( seek.next() );
        }
    }

    private void assertFindExact( GBPTree<RawBytes,RawBytes> tree, RawBytes key, RawBytes value ) throws IOException
    {
        try ( Seeker<RawBytes,RawBytes> seek = tree.seek( key, key ) )
        {
            assertTrue( seek.next() );
            assertEquals( 0, layout.compare( key, seek.key() ) );
            assertEquals( 0, layout.compare( value, seek.value() ) );
            assertFalse( seek.next() );
        }
    }

    private GBPTree<RawBytes,RawBytes> createIndex() throws IOException
    {
        // some random padding
        return new GBPTreeBuilder<>( pageCache, testDirectory.file( "index" ), layout ).build();
    }

    private byte[] asBytes( int value )
    {
        byte[] intBytes = new byte[Integer.BYTES];
        for ( int i = 0, j = intBytes.length - 1; i < intBytes.length; i++, j-- )
        {
            intBytes[j] = (byte) (value >>> i * Byte.SIZE);
        }
        return intBytes;
    }

    private RawBytes key( int keySize, byte... firstBytes )
    {
        RawBytes key = layout.newKey();
        key.bytes = new byte[keySize];
        for ( int i = 0; i < firstBytes.length && i < keySize; i++ )
        {
            key.bytes[i] = firstBytes[i];
        }
        return key;
    }

    private RawBytes value( int valueSize )
    {
        RawBytes value = layout.newValue();
        value.bytes = new byte[valueSize];
        return value;
    }

    private int inValidRange( int min, int max, int value )
    {
        return Math.min( max, Math.max( min, value ) );
    }
}
