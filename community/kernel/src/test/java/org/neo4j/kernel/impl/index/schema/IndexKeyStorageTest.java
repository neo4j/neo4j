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
package org.neo4j.kernel.impl.index.schema;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.Value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.index.schema.ByteBufferFactory.HEAP_ALLOCATOR;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;

@ExtendWith( {TestDirectoryExtension.class, RandomExtension.class} )
class IndexKeyStorageTest
{
    private static final int BLOCK_SIZE = 2000;
    private static final IndexSpecificSpaceFillingCurveSettingsCache spatialSettings =
            new IndexSpecificSpaceFillingCurveSettingsCache( new ConfiguredSpaceFillingCurveSettingsCache( Config.defaults() ), new HashMap<>() );

    @Inject
    protected TestDirectory directory;

    @Inject
    protected RandomRule random;

    private GenericLayout layout;
    private int numberOfSlots;

    @BeforeEach
    void createLayout()
    {
        this.numberOfSlots = random.nextInt( 1, 3 );
        this.layout = new GenericLayout( numberOfSlots, spatialSettings );
    }

    @Test
    void shouldAddAndReadZeroKey() throws IOException
    {
        try ( IndexKeyStorage<GenericKey> keyStorage = keyStorage() )
        {
            keyStorage.doneAdding();
            try ( IndexKeyStorage.KeyEntryCursor<GenericKey> reader = keyStorage.reader() )
            {
                assertFalse( reader.next(), "Didn't expect reader to have any entries." );
            }
        }
    }

    @Test
    void shouldAddAndReadOneKey() throws IOException
    {
        try ( IndexKeyStorage<GenericKey> keyStorage = keyStorage() )
        {
            GenericKey expected = randomKey( 1 );
            keyStorage.add( expected );
            keyStorage.doneAdding();
            try ( IndexKeyStorage.KeyEntryCursor<GenericKey> reader = keyStorage.reader() )
            {
                assertTrue( reader.next(), "Expected reader to have one entry" );
                GenericKey actual = reader.key();
                assertEquals( 0, layout.compare( expected, actual ), "Expected stored key to be equal to original." );
                assertFalse( reader.next(), "Expected reader to have only one entry, second entry was " + reader.key() );
            }
        }
    }

    @Test
    void shouldAddAndReadMultipleKeys() throws IOException
    {
        List<GenericKey> keys = new ArrayList<>();
        int numberOfKeys = 1000;
        for ( int i = 0; i < numberOfKeys; i++ )
        {
            keys.add( randomKey( i ) );
        }
        try ( IndexKeyStorage<GenericKey> keyStorage = keyStorage() )
        {
            for ( GenericKey key : keys )
            {
                keyStorage.add( key );
            }
            keyStorage.doneAdding();
            try ( IndexKeyStorage.KeyEntryCursor<GenericKey> reader = keyStorage.reader() )
            {
                for ( GenericKey expected : keys )
                {
                    assertTrue( reader.next() );
                    GenericKey actual = reader.key();
                    assertEquals( 0, layout.compare( expected, actual ), "Expected stored key to be equal to original." );
                }
                assertFalse( reader.next(), "Expected reader to have no more entries, but had at least one additional " + reader.key() );
            }
        }
    }

    @Test
    void shouldNotCreateFileIfNoData() throws IOException
    {
        FileSystemAbstraction fs = directory.getFileSystem();
        File makeSureImDeleted = directory.file( "makeSureImDeleted" );
        try ( IndexKeyStorage<GenericKey> keyStorage = keyStorage( makeSureImDeleted ) )
        {
            assertFalse( fs.fileExists( makeSureImDeleted ), "Expected this file to exist now so that we can assert deletion later." );
            keyStorage.doneAdding();
            assertFalse( fs.fileExists( makeSureImDeleted ), "Expected this file to exist now so that we can assert deletion later." );
        }
        assertFalse( fs.fileExists( makeSureImDeleted ), "Expected this file to be deleted on close." );
    }

    @Test
    void shouldDeleteFileOnCloseWithData() throws IOException
    {
        FileSystemAbstraction fs = directory.getFileSystem();
        File makeSureImDeleted = directory.file( "makeSureImDeleted" );
        try ( IndexKeyStorage<GenericKey> keyStorage = keyStorage( makeSureImDeleted ) )
        {
            keyStorage.add( randomKey( 1 ) );
            keyStorage.doneAdding();
            assertTrue( fs.fileExists( makeSureImDeleted ), "Expected this file to exist now so that we can assert deletion later." );
        }
        assertFalse( fs.fileExists( makeSureImDeleted ), "Expected this file to be deleted on close." );
    }

    @Test
    void shouldDeleteFileOnCloseWithDataBeforeDoneAdding() throws IOException
    {
        FileSystemAbstraction fs = directory.getFileSystem();
        File makeSureImDeleted = directory.file( "makeSureImDeleted" );
        try ( IndexKeyStorage<GenericKey> keyStorage = keyStorage( makeSureImDeleted ) )
        {
            keyStorage.add( randomKey( 1 ) );
            assertTrue( fs.fileExists( makeSureImDeleted ), "Expected this file to exist now so that we can assert deletion later." );
        }
        assertFalse( fs.fileExists( makeSureImDeleted ), "Expected this file to be deleted on close." );
    }

    @Test
    void mustAllocateResourcesLazilyAndCleanUpOnClose() throws IOException
    {
        FileSystemAbstraction fs = directory.getFileSystem();
        LocalMemoryTracker allocationTracker = new LocalMemoryTracker();
        File file = directory.file( "file" );
        try ( UnsafeDirectByteBufferAllocator bufferFactory = new UnsafeDirectByteBufferAllocator( allocationTracker );
              IndexKeyStorage<GenericKey> keyStorage = keyStorage( file, bufferFactory ) )
        {
            assertEquals( 0, allocationTracker.usedDirectMemory(), "Expected to not have any buffers allocated yet" );
            assertFalse( fs.fileExists( file ), "Expected file to be created lazily" );
            keyStorage.add( randomKey( 1 ) );
            assertEquals( BLOCK_SIZE, allocationTracker.usedDirectMemory(), "Expected to have exactly one buffer allocated by now" );
            assertTrue( fs.fileExists( file ), "Expected file to be created by now" );
        }
        assertFalse( fs.fileExists( file ), "Expected file to be deleted on close" );
    }

    @Test
    void shouldReportCorrectCount() throws IOException
    {
        try ( IndexKeyStorage<GenericKey> keyStorage = keyStorage() )
        {
            assertEquals( 0, keyStorage.count() );
            keyStorage.add( randomKey( 1 ) );
            assertEquals( 1, keyStorage.count() );
            keyStorage.add( randomKey( 2 ) );
            assertEquals( 2, keyStorage.count() );
            keyStorage.doneAdding();
            assertEquals( 2, keyStorage.count() );
        }
    }

    private GenericKey randomKey( int entityId )
    {
        GenericKey key = layout.newKey();
        key.initialize( entityId );
        for ( int i = 0; i < numberOfSlots; i++ )
        {
            Value value = random.randomValues().nextValue();
            key.initFromValue( i, value, NEUTRAL );
        }
        return key;
    }

    private IndexKeyStorage<GenericKey> keyStorage() throws IOException
    {
        return keyStorage( directory.file( "file" ) );
    }

    private IndexKeyStorage<GenericKey> keyStorage( File file ) throws IOException
    {
        return keyStorage( file, HEAP_ALLOCATOR );
    }

    private IndexKeyStorage<GenericKey> keyStorage( File file, ByteBufferFactory.Allocator bufferFactory ) throws IOException
    {
        return new IndexKeyStorage<>( directory.getFileSystem(), file, bufferFactory, BLOCK_SIZE, layout );
    }
}
