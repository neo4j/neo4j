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
package org.neo4j.kernel.impl.index.schema;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeBuilder;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static org.apache.commons.lang3.exception.ExceptionUtils.hasCause;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.internal.kernel.api.InternalIndexState.FAILED;
import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;
import static org.neo4j.kernel.impl.index.schema.NativeIndexPopulator.BYTE_FAILED;
import static org.neo4j.kernel.impl.index.schema.NativeIndexPopulator.BYTE_ONLINE;
import static org.neo4j.kernel.impl.index.schema.NativeIndexPopulator.BYTE_POPULATING;

abstract class NativeIndexPopulatorTests<KEY extends NativeIndexKey<KEY>,VALUE extends NativeIndexValue>
        extends NativeIndexTestUtil<KEY,VALUE>
{
    private static final int LARGE_AMOUNT_OF_UPDATES = 1_000;
    static final NodePropertyAccessor null_property_accessor = ( nodeId, propertyKeyId ) ->
    {
        throw new RuntimeException( "Did not expect an attempt to go to store" );
    };

    NativeIndexPopulator<KEY,VALUE> populator;

    @BeforeEach
    void setupPopulator() throws IOException
    {
        populator = createPopulator();
    }

    abstract NativeIndexPopulator<KEY,VALUE> createPopulator() throws IOException;

    @Test
    void createShouldCreateFile()
    {
        // given
        assertFileNotPresent();

        // when
        populator.create();

        // then
        assertFilePresent();
        populator.close( true );
    }

    @Test
    void createShouldClearExistingFile() throws Exception
    {
        // given
        byte[] someBytes = fileWithContent();

        // when
        populator.create();

        // then
        try ( StoreChannel r = fs.read( indexFiles.getStoreFile() ) )
        {
            byte[] firstBytes = new byte[someBytes.length];
            r.readAll( ByteBuffer.wrap( firstBytes ) );
            assertNotEquals(
                someBytes, firstBytes, "Expected previous file content to have been cleared but was still there" );
        }
        populator.close( true );
    }

    @Test
    void dropShouldDeleteExistingFile()
    {
        // given
        populator.create();

        // when
        populator.drop();

        // then
        assertFileNotPresent();
    }

    @Test
    void dropShouldDeleteExistingDirectory()
    {
        // given
        populator.create();

        // when
        assertTrue( fs.fileExists( indexFiles.getBase() ) );
        populator.drop();

        // then
        assertFalse( fs.fileExists( indexFiles.getBase() ), "expected drop to delete index base" );
    }

    @Test
    void dropShouldSucceedOnNonExistentFile()
    {
        // given
        assertFileNotPresent();

        // when
        populator.drop();

        // then
        assertFileNotPresent();
    }

    @Test
    void addShouldHandleEmptyCollection() throws Exception
    {
        // given
        populator.create();
        List<IndexEntryUpdate<?>> updates = Collections.emptyList();

        // when
        populator.add( updates );
        populator.scanCompleted( nullInstance, jobScheduler );

        // then
        populator.close( true );
    }

    @Test
    void addShouldApplyAllUpdatesOnce() throws Exception
    {
        // given
        populator.create();
        IndexEntryUpdate<IndexDescriptor>[] updates = valueCreatorUtil.someUpdates( random );

        // when
        populator.add( asList( updates ) );
        populator.scanCompleted( nullInstance, jobScheduler );

        // then
        populator.close( true );
        verifyUpdates( updates );
    }

    @Test
    void updaterShouldApplyUpdates() throws Exception
    {
        // given
        populator.create();
        IndexEntryUpdate<IndexDescriptor>[] updates = valueCreatorUtil.someUpdates( random );
        try ( IndexUpdater updater = populator.newPopulatingUpdater( null_property_accessor ) )
        {
            // when
            for ( IndexEntryUpdate<IndexDescriptor> update : updates )
            {
                updater.process( update );
            }
        }

        // then
        populator.scanCompleted( nullInstance, jobScheduler );
        populator.close( true );
        verifyUpdates( updates );
    }

    @Test
    void updaterMustThrowIfProcessAfterClose() throws Exception
    {
        // given
        populator.create();
        IndexUpdater updater = populator.newPopulatingUpdater( null_property_accessor );

        // when
        updater.close();

        assertThrows( IllegalStateException.class, () -> updater.process( valueCreatorUtil.add( 1, Values.of( Long.MAX_VALUE ) ) ) );
        populator.close( true );
    }

    @Test
    void shouldApplyInterleavedUpdatesFromAddAndUpdater() throws Exception
    {
        // given
        populator.create();
        IndexEntryUpdate<IndexDescriptor>[] updates = valueCreatorUtil.someUpdates( random );

        // when
        applyInterleaved( updates, populator );

        // then
        populator.scanCompleted( nullInstance, jobScheduler );
        populator.close( true );
        verifyUpdates( updates );
    }

    @Test
    void successfulCloseMustCloseGBPTree() throws Exception
    {
        // given
        populator.create();
        Optional<PagedFile> existingMapping = pageCache.getExistingMapping( indexFiles.getStoreFile() );
        if ( existingMapping.isPresent() )
        {
            existingMapping.get().close();
        }
        else
        {
            fail( "Expected underlying GBPTree to have a mapping for this file" );
        }

        // when
        populator.close( true );

        // then
        existingMapping = pageCache.getExistingMapping( indexFiles.getStoreFile() );
        assertFalse( existingMapping.isPresent() );
    }

    @Test
    void successfulCloseMustMarkIndexAsOnline() throws Exception
    {
        // given
        populator.create();

        // when
        populator.close( true );

        // then
        assertHeader( ONLINE, null, false );
    }

    @Test
    void unsuccessfulCloseMustSucceedWithoutMarkAsFailed()
    {
        // given
        populator.create();

        // then
        populator.close( false );
    }

    @Test
    void unsuccessfulCloseMustCloseGBPTree() throws Exception
    {
        // given
        populator.create();
        Optional<PagedFile> existingMapping = pageCache.getExistingMapping( indexFiles.getStoreFile() );
        if ( existingMapping.isPresent() )
        {
            existingMapping.get().close();
        }
        else
        {
            fail( "Expected underlying GBPTree to have a mapping for this file" );
        }

        // when
        populator.close( false );

        // then
        existingMapping = pageCache.getExistingMapping( indexFiles.getStoreFile() );
        assertFalse( existingMapping.isPresent() );
    }

    @Test
    void unsuccessfulCloseMustNotMarkIndexAsOnline() throws Exception
    {
        // given
        populator.create();

        // when
        populator.close( false );

        // then
        assertHeader( POPULATING, null, false );
    }

    @Test
    void closeMustWriteFailureMessageAfterMarkedAsFailed() throws Exception
    {
        // given
        populator.create();

        // when
        String failureMessage = "Fly, you fools!";
        populator.markAsFailed( failureMessage );
        populator.close( false );

        // then
        assertHeader( FAILED, failureMessage, false );
    }

    @Test
    void closeMustWriteFailureMessageAfterMarkedAsFailedWithLongMessage() throws Exception
    {
        // given
        populator.create();

        // when
        String failureMessage = longString( pageCache.pageSize() );
        populator.markAsFailed( failureMessage );
        populator.close( false );

        // then
        assertHeader( FAILED, failureMessage, true );
    }

    @Test
    void successfulCloseMustThrowIfMarkedAsFailed()
    {
        // given
        populator.create();

        // when
        populator.markAsFailed( "" );

        // then
        var e = assertThrows( RuntimeException.class, () -> populator.close( true ) );
        assertTrue( hasCause( e, IllegalStateException.class ), "Expected cause to contain " + IllegalStateException.class );
        populator.close( false );
    }

    @Test
    void shouldApplyLargeAmountOfInterleavedRandomUpdates() throws Exception
    {
        // given
        populator.create();
        random.reset();
        Random updaterRandom = new Random( random.seed() );
        Iterator<IndexEntryUpdate<IndexDescriptor>> updates = valueCreatorUtil.randomUpdateGenerator( random );

        // when
        int count = interleaveLargeAmountOfUpdates( updaterRandom, updates );

        // then
        populator.scanCompleted( nullInstance, jobScheduler );
        populator.close( true );
        random.reset();
        verifyUpdates( valueCreatorUtil.randomUpdateGenerator( random ), count );
    }

    @Test
    void dropMustSucceedAfterSuccessfulClose()
    {
        // given
        populator.create();
        populator.close( true );

        // when
        populator.drop();

        // then
        assertFileNotPresent();
    }

    @Test
    void dropMustSucceedAfterUnsuccessfulClose()
    {
        // given
        populator.create();
        populator.close( false );

        // when
        populator.drop();

        // then
        assertFileNotPresent();
    }

    @Test
    void successfulCloseMustThrowWithoutPriorSuccessfulCreate()
    {
        // given
        assertFileNotPresent();

        // when
        var e = assertThrows( RuntimeException.class, () -> populator.close( true ) );
        assertTrue( hasCause( e, IllegalStateException.class ), "Expected cause to contain " + IllegalStateException.class );
    }

    @Test
    void unsuccessfulCloseMustSucceedWithoutSuccessfulPriorCreate() throws Exception
    {
        // given
        assertFileNotPresent();
        String failureMessage = "There is no spoon";
        populator.markAsFailed( failureMessage );

        // when
        populator.close( false );

        // then
        assertHeader( FAILED, failureMessage, false );
    }

    @Test
    void successfulCloseMustThrowAfterDrop()
    {
        // given
        populator.create();

        // when
        populator.drop();

        // then
        var e = assertThrows( RuntimeException.class, () -> populator.close( true ) );
        assertTrue( hasCause( e, IllegalStateException.class ), "Expected cause to contain " + IllegalStateException.class );
    }

    @Test
    void unsuccessfulCloseMustThrowAfterDrop()
    {
        // given
        populator.create();

        // when
        populator.drop();

        // then
        var e = assertThrows( RuntimeException.class, () -> populator.close( false ) );
        assertTrue( hasCause( e, IllegalStateException.class ), "Expected cause to contain " + IllegalStateException.class );
    }

    private int interleaveLargeAmountOfUpdates( Random updaterRandom,
            Iterator<IndexEntryUpdate<IndexDescriptor>> updates ) throws IndexEntryConflictException
    {
        int count = 0;
        for ( int i = 0; i < LARGE_AMOUNT_OF_UPDATES; i++ )
        {
            if ( updaterRandom.nextFloat() < 0.1 )
            {
                try ( IndexUpdater indexUpdater = populator.newPopulatingUpdater( null_property_accessor ) )
                {
                    int numberOfUpdaterUpdates = updaterRandom.nextInt( 100 );
                    for ( int j = 0; j < numberOfUpdaterUpdates; j++ )
                    {
                        indexUpdater.process( updates.next() );
                        count++;
                    }
                }
            }
            populator.add( Collections.singletonList( updates.next() ) );
            count++;
        }
        return count;
    }

    private void assertHeader( InternalIndexState expectedState, String failureMessage, boolean messageTruncated ) throws IOException
    {
        NativeIndexHeaderReader headerReader = new NativeIndexHeaderReader( NO_HEADER_READER );
        try ( GBPTree<KEY,VALUE> ignored = new GBPTreeBuilder<>( pageCache, indexFiles.getStoreFile(), layout ).with( headerReader ).build() )
        {
            switch ( expectedState )
            {
            case ONLINE:
                assertEquals( BYTE_ONLINE, headerReader.state, "Index was not marked as online when expected not to be." );
                assertNull( headerReader.failureMessage, "Expected failure message to be null when marked as online." );
                break;
            case FAILED:
                assertEquals( BYTE_FAILED, headerReader.state, "Index was marked as online when expected not to be." );
                if ( messageTruncated )
                {
                    assertTrue( headerReader.failureMessage.length() < failureMessage.length() );
                    assertTrue( failureMessage.startsWith( headerReader.failureMessage ) );
                }
                else
                {
                    assertEquals( failureMessage, headerReader.failureMessage );
                }
                break;
            case POPULATING:
                assertEquals( BYTE_POPULATING, headerReader.state, "Index was not left as populating when expected to be." );
                assertNull( headerReader.failureMessage, "Expected failure message to be null when marked as populating." );
                break;
            default:
                throw new UnsupportedOperationException( "Unexpected index state " + expectedState );
            }
        }
    }

    private static String longString( int length )
    {
        return RandomStringUtils.random( length, true, true );
    }

    private void applyInterleaved( IndexEntryUpdate<IndexDescriptor>[] updates, NativeIndexPopulator<KEY,VALUE> populator )
            throws IndexEntryConflictException
    {
        boolean useUpdater = true;
        Collection<IndexEntryUpdate<IndexDescriptor>> populatorBatch = new ArrayList<>();
        IndexUpdater updater = populator.newPopulatingUpdater( null_property_accessor );
        for ( IndexEntryUpdate<IndexDescriptor> update : updates )
        {
            if ( random.nextInt( 100 ) < 20 )
            {
                if ( useUpdater )
                {
                    updater.close();
                    populatorBatch = new ArrayList<>();
                }
                else
                {
                    populator.add( populatorBatch );
                    updater = populator.newPopulatingUpdater( null_property_accessor );
                }
                useUpdater = !useUpdater;
            }
            if ( useUpdater )
            {
                updater.process( update );
            }
            else
            {
                populatorBatch.add( update );
            }
        }
        if ( useUpdater )
        {
            updater.close();
        }
        else
        {
            populator.add( populatorBatch );
        }
    }

    private void verifyUpdates( Iterator<IndexEntryUpdate<IndexDescriptor>> indexEntryUpdateIterator, int count )
            throws IOException
    {
        @SuppressWarnings( "unchecked" )
        IndexEntryUpdate<IndexDescriptor>[] updates = new IndexEntryUpdate[count];
        for ( int i = 0; i < count; i++ )
        {
            updates[i] = indexEntryUpdateIterator.next();
        }
        verifyUpdates( updates );
    }

    private byte[] fileWithContent() throws IOException
    {
        int size = 1000;
        fs.mkdirs( indexFiles.getStoreFile().getParentFile() );
        try ( StoreChannel storeChannel = fs.write( indexFiles.getStoreFile() ) )
        {
            byte[] someBytes = new byte[size];
            random.nextBytes( someBytes );
            storeChannel.writeAll( ByteBuffer.wrap( someBytes ) );
            return someBytes;
        }
    }
}
