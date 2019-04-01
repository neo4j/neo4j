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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.cursor.RawCursor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.test.rule.PageCacheConfig.config;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class, RandomExtension.class} )
class GBPTreeOffloadTest
{
    private static final int DEFAULT_PAGE_SIZE = 256;
    private static final SimpleByteArrayLayout layout = new SimpleByteArrayLayout( false );

    @RegisterExtension
    static PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension( config().withAccessChecks( true ) );
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private RandomRule random;

    private File indexFile;

    @BeforeEach
    void setUp()
    {
        indexFile = testDirectory.file( "index" );
    }

    /* PUT */

    @Test
    void putSingleKeyLargeThanOffloadCap() throws IOException
    {
        try ( GBPTree<RawBytes,RawBytes> tree = index().build() )
        {
            int keySize = tree.needOffloadCap();
            RawBytes key = key( keySize + 1, (byte) 0 );
            RawBytes value = value( 0 );
            try ( Writer<RawBytes,RawBytes> writer = tree.writer() )
            {
                writer.put( key, value );
            }
            assertFindExact( tree, key, value );
        }
    }

    @Test
    void putSingleKeyOnKeyValueSizeCap() throws IOException
    {
        try ( GBPTree<RawBytes,RawBytes> tree = index().build() )
        {
            int keySize = tree.keyValueSizeCap();
            RawBytes key = key( keySize, (byte) 0 );
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
        try ( GBPTree<RawBytes,RawBytes> tree = index().build() )
        {
            int keySize = tree.keyValueSizeCap() + 1;
            RawBytes key = key( keySize, (byte) 0 );
            RawBytes value = value( 0 );
            try ( Writer<RawBytes,RawBytes> writer = tree.writer() )
            {
                assertThrows( IllegalArgumentException.class, () -> writer.put( key, value ) );
            }
        }
    }

    @Test
    void putSmallAndLargeKeysInGaussianDistribution() throws IOException
    {
        try ( GBPTree<RawBytes,RawBytes> tree = index().build() )
        {
            int keyValueSizeOverflow = tree.keyValueSizeCap() + 1;

            List<RawBytes> keys = new ArrayList<>();
            for ( int i = 0; i < 10_000; i++ )
            {
                int keySize = inValidRange( 4, keyValueSizeOverflow, random.nextInt( keyValueSizeOverflow ) );
                keys.add( key( keySize, asBytes( i ) ) );
            }
            insertAndValidate( tree, keys );
        }
    }

    private void insertAndValidate( GBPTree<RawBytes,RawBytes> tree, List<RawBytes> keys ) throws IOException
    {
        Collections.shuffle( keys, random.random() );
        RawBytes value = value( 0 );
        try ( Writer<RawBytes,RawBytes> writer = tree.writer() )
        {
            for ( RawBytes key : keys )
            {
                writer.put( key, value );
            }
        }
        keys.sort( layout );
        for ( RawBytes key : keys )
        {
            assertFindExact( tree, key, value );
        }
    }

    private int inValidRange( int min, int max, int value )
    {
        return Math.min( max, Math.max( min, value ) );
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

    private void assertFindExact( GBPTree<RawBytes,RawBytes> tree, RawBytes key, RawBytes value ) throws IOException
    {
        try ( RawCursor<Hit<RawBytes,RawBytes>,IOException> seek = tree.seek( key, key ) )
        {
            assertTrue( seek.next() );
            Hit<RawBytes,RawBytes> hit = seek.get();
            assertEquals( 0, layout.compare( key, hit.key() ) );
            assertEquals( 0, layout.compare( value, hit.value() ) );
            assertFalse( seek.next() );
        }
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

    private GBPTreeBuilder<RawBytes,RawBytes> index()
    {
        PageCache pageCache = pageCacheExtension.getPageCache( fileSystem, config().withPageSize( DEFAULT_PAGE_SIZE ) );
        return new GBPTreeBuilder<>( pageCache, indexFile, layout );
    }

    private static Stream<Checkpointer> checkpointerStream()
    {
        return Stream.of(
                tree -> {}, // No checkpoint
                tree -> tree.checkpoint( IOLimiter.UNLIMITED ) // Do checkpoint
        );
    }

    private interface Checkpointer
    {
        void checkpoint( GBPTree<?,?> tree );
    }
}
