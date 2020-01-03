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

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import org.neo4j.helpers.Exceptions;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeBuilder;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.internal.kernel.api.InternalIndexState.FAILED;
import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;
import static org.neo4j.kernel.impl.index.schema.NativeIndexPopulator.BYTE_FAILED;
import static org.neo4j.kernel.impl.index.schema.NativeIndexPopulator.BYTE_ONLINE;
import static org.neo4j.kernel.impl.index.schema.NativeIndexPopulator.BYTE_POPULATING;
import static org.neo4j.kernel.impl.index.schema.ValueCreatorUtil.countUniqueValues;

public abstract class NativeIndexPopulatorTests<KEY extends NativeIndexKey<KEY>,VALUE extends NativeIndexValue>
        extends NativeIndexTestUtil<KEY,VALUE>
{
    private static final int LARGE_AMOUNT_OF_UPDATES = 1_000;
    static final NodePropertyAccessor null_property_accessor = ( nodeId, propKeyId ) ->
    {
        throw new RuntimeException( "Did not expect an attempt to go to store" );
    };

    NativeIndexPopulator<KEY,VALUE> populator;

    @Before
    public void setupPopulator() throws IOException
    {
        populator = createPopulator();
    }

    abstract NativeIndexPopulator<KEY,VALUE> createPopulator() throws IOException;

    @Test
    public void createShouldCreateFile()
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
    public void createShouldClearExistingFile() throws Exception
    {
        // given
        byte[] someBytes = fileWithContent();

        // when
        populator.create();

        // then
        try ( StoreChannel r = fs.open( getIndexFile(), OpenMode.READ ) )
        {
            byte[] firstBytes = new byte[someBytes.length];
            r.readAll( ByteBuffer.wrap( firstBytes ) );
            assertNotEquals( "Expected previous file content to have been cleared but was still there",
                    someBytes, firstBytes );
        }
        populator.close( true );
    }

    @Test
    public void dropShouldDeleteExistingFile()
    {
        // given
        populator.create();

        // when
        populator.drop();

        // then
        assertFileNotPresent();
    }

    @Test
    public void dropShouldSucceedOnNonExistentFile()
    {
        // given
        assertFileNotPresent();

        // when
        populator.drop();

        // then
        assertFileNotPresent();
    }

    @Test
    public void addShouldHandleEmptyCollection() throws Exception
    {
        // given
        populator.create();
        List<IndexEntryUpdate<?>> updates = Collections.emptyList();

        // when
        populator.add( updates );
        populator.scanCompleted( nullInstance );

        // then
        populator.close( true );
    }

    @Test
    public void addShouldApplyAllUpdatesOnce() throws Exception
    {
        // given
        populator.create();
        @SuppressWarnings( "unchecked" )
        IndexEntryUpdate<IndexDescriptor>[] updates = valueCreatorUtil.someUpdates( random );

        // when
        populator.add( Arrays.asList( updates ) );
        populator.scanCompleted( nullInstance );

        // then
        populator.close( true );
        verifyUpdates( updates );
    }

    @Test
    public void updaterShouldApplyUpdates() throws Exception
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
        populator.scanCompleted( nullInstance );
        populator.close( true );
        verifyUpdates( updates );
    }

    @Test
    public void updaterMustThrowIfProcessAfterClose() throws Exception
    {
        // given
        populator.create();
        IndexUpdater updater = populator.newPopulatingUpdater( null_property_accessor );

        // when
        updater.close();

        // then
        try
        {
            updater.process( valueCreatorUtil.add( 1, Values.of( Long.MAX_VALUE ) ) );
            fail( "Expected process to throw on closed updater" );
        }
        catch ( IllegalStateException e )
        {
            // good
        }
        populator.close( true );
    }

    @Test
    public void shouldApplyInterleavedUpdatesFromAddAndUpdater() throws Exception
    {
        // given
        populator.create();
        @SuppressWarnings( "unchecked" )
        IndexEntryUpdate<IndexDescriptor>[] updates = valueCreatorUtil.someUpdates( random );

        // when
        applyInterleaved( updates, populator );

        // then
        populator.scanCompleted( nullInstance );
        populator.close( true );
        verifyUpdates( updates );
    }

    @Test
    public void successfulCloseMustCloseGBPTree() throws Exception
    {
        // given
        populator.create();
        Optional<PagedFile> existingMapping = pageCache.getExistingMapping( getIndexFile() );
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
        existingMapping = pageCache.getExistingMapping( getIndexFile() );
        assertFalse( existingMapping.isPresent() );
    }

    @Test
    public void successfulCloseMustMarkIndexAsOnline() throws Exception
    {
        // given
        populator.create();

        // when
        populator.close( true );

        // then
        assertHeader( ONLINE, null, false );
    }

    @Test
    public void unsuccessfulCloseMustSucceedWithoutMarkAsFailed()
    {
        // given
        populator.create();

        // then
        populator.close( false );
    }

    @Test
    public void unsuccessfulCloseMustCloseGBPTree() throws Exception
    {
        // given
        populator.create();
        Optional<PagedFile> existingMapping = pageCache.getExistingMapping( getIndexFile() );
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
        existingMapping = pageCache.getExistingMapping( getIndexFile() );
        assertFalse( existingMapping.isPresent() );
    }

    @Test
    public void unsuccessfulCloseMustNotMarkIndexAsOnline() throws Exception
    {
        // given
        populator.create();

        // when
        populator.close( false );

        // then
        assertHeader( POPULATING, null, false );
    }

    @Test
    public void closeMustWriteFailureMessageAfterMarkedAsFailed() throws Exception
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
    public void closeMustWriteFailureMessageAfterMarkedAsFailedWithLongMessage() throws Exception
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
    public void successfulCloseMustThrowIfMarkedAsFailed()
    {
        // given
        populator.create();

        // when
        populator.markAsFailed( "" );

        // then
        try
        {
            populator.close( true );
            fail( "Expected successful close to fail after markedAsFailed" );
        }
        catch ( RuntimeException e )
        {
            // then good
            assertTrue( "Expected cause to contain " + IllegalStateException.class, Exceptions.contains( e, IllegalStateException.class ) );
        }
        populator.close( false );
    }

    @Test
    public void shouldApplyLargeAmountOfInterleavedRandomUpdates() throws Exception
    {
        // given
        populator.create();
        random.reset();
        Random updaterRandom = new Random( random.seed() );
        Iterator<IndexEntryUpdate<IndexDescriptor>> updates = valueCreatorUtil.randomUpdateGenerator( random );

        // when
        int count = interleaveLargeAmountOfUpdates( updaterRandom, updates );

        // then
        populator.scanCompleted( nullInstance );
        populator.close( true );
        random.reset();
        verifyUpdates( valueCreatorUtil.randomUpdateGenerator( random ), count );
    }

    @Test
    public void dropMustSucceedAfterSuccessfulClose()
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
    public void dropMustSucceedAfterUnsuccessfulClose()
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
    public void successfulCloseMustThrowWithoutPriorSuccessfulCreate()
    {
        // given
        assertFileNotPresent();

        // when
        try
        {
            populator.close( true );
            fail( "Should have failed" );
        }
        catch ( RuntimeException e )
        {
            // then good
            assertTrue( "Expected cause to contain " + IllegalStateException.class, Exceptions.contains( e, IllegalStateException.class ) );
        }
    }

    @Test
    public void unsuccessfulCloseMustSucceedWithoutSuccessfulPriorCreate() throws Exception
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
    public void successfulCloseMustThrowAfterDrop()
    {
        // given
        populator.create();

        // when
        populator.drop();

        // then
        try
        {
            populator.close( true );
            fail( "Should have failed" );
        }
        catch ( RuntimeException e )
        {
            // then good
            assertTrue( "Expected cause to contain " + IllegalStateException.class, Exceptions.contains( e, IllegalStateException.class ) );
        }
    }

    @Test
    public void unsuccessfulCloseMustThrowAfterDrop()
    {
        // given
        populator.create();

        // when
        populator.drop();

        // then
        try
        {
            populator.close( false );
            fail( "Should have failed" );
        }
        catch ( RuntimeException e )
        {
            // then good
            assertTrue( "Expected cause to contain " + IllegalStateException.class, Exceptions.contains( e, IllegalStateException.class ) );
        }
    }

    public abstract static class Unique<K extends NativeIndexKey<K>, V extends NativeIndexValue> extends NativeIndexPopulatorTests<K,V>
    {
        @Test
        public void addShouldThrowOnDuplicateValues()
        {
            // given
            populator.create();
            IndexEntryUpdate<IndexDescriptor>[] updates = valueCreatorUtil.someUpdatesWithDuplicateValues( random );

            // when
            try
            {
                populator.add( Arrays.asList( updates ) );
                populator.scanCompleted( nullInstance );
                fail( "Updates should have conflicted" );
            }
            catch ( IndexEntryConflictException e )
            {
                // then good
            }
            finally
            {
                populator.close( true );
            }
        }

        @Test
        public void updaterShouldThrowOnDuplicateValues() throws Exception
        {
            // given
            populator.create();
            IndexEntryUpdate<IndexDescriptor>[] updates = valueCreatorUtil.someUpdatesWithDuplicateValues( random );
            IndexUpdater updater = populator.newPopulatingUpdater( null_property_accessor );

            // when
            for ( IndexEntryUpdate<IndexDescriptor> update : updates )
            {
                updater.process( update );
            }
            try
            {
                updater.close();
                populator.scanCompleted( nullInstance );
                fail( "Updates should have conflicted" );
            }
            catch ( Exception e )
            {
                // then
                assertTrue( e.getMessage(), Exceptions.contains( e, IndexEntryConflictException.class ) );
            }
            finally
            {
                populator.close( true );
            }
        }

        @Test
        public void shouldSampleUpdates() throws Exception
        {
            // GIVEN
            populator.create();
            IndexEntryUpdate<IndexDescriptor>[] updates = valueCreatorUtil.someUpdates( random );

            // WHEN
            populator.add( asList( updates ) );
            for ( IndexEntryUpdate<IndexDescriptor> update : updates )
            {
                populator.includeSample( update );
            }
            populator.scanCompleted( nullInstance );
            IndexSample sample = populator.sampleResult();

            // THEN
            assertEquals( updates.length, sample.sampleSize() );
            assertEquals( updates.length, sample.uniqueValues() );
            assertEquals( updates.length, sample.indexSize() );
            populator.close( true );
        }
    }

    public abstract static class NonUnique<K extends NativeIndexKey<K>, V extends NativeIndexValue> extends NativeIndexPopulatorTests<K,V>
    {
        @Test
        public void addShouldApplyDuplicateValues() throws Exception
        {
            // given
            populator.create();
            IndexEntryUpdate<IndexDescriptor>[] updates = valueCreatorUtil.someUpdatesWithDuplicateValues( random );

            // when
            populator.add( Arrays.asList( updates ) );

            // then
            populator.scanCompleted( nullInstance );
            populator.close( true );
            verifyUpdates( updates );
        }

        @Test
        public void updaterShouldApplyDuplicateValues() throws Exception
        {
            // given
            populator.create();
            IndexEntryUpdate<IndexDescriptor>[] updates = valueCreatorUtil.someUpdatesWithDuplicateValues( random );
            try ( IndexUpdater updater = populator.newPopulatingUpdater( null_property_accessor ) )
            {
                // when
                for ( IndexEntryUpdate<IndexDescriptor> update : updates )
                {
                    updater.process( update );
                }
            }

            // then
            populator.scanCompleted( nullInstance );
            populator.close( true );
            verifyUpdates( updates );
        }

        @Test
        public void shouldSampleUpdatesIfConfiguredForOnlineSampling() throws Exception
        {
            // GIVEN
            try
            {
                populator.create();
                IndexEntryUpdate<IndexDescriptor>[] scanUpdates = valueCreatorUtil.someUpdates( random );
                populator.add( Arrays.asList( scanUpdates ) );
                Iterator<IndexEntryUpdate<IndexDescriptor>> generator = valueCreatorUtil.randomUpdateGenerator( random );
                Value[] updates = new Value[5];
                updates[0] = generator.next().values()[0];
                updates[1] = generator.next().values()[0];
                updates[2] = updates[1];
                updates[3] = generator.next().values()[0];
                updates[4] = updates[3];
                try ( IndexUpdater updater = populator.newPopulatingUpdater( null_property_accessor ) )
                {
                    long nodeId = 1000;
                    for ( Value value : updates )
                    {
                        IndexEntryUpdate<IndexDescriptor> update = valueCreatorUtil.add( nodeId++, value );
                        updater.process( update );
                    }
                }

                // WHEN
                populator.scanCompleted( nullInstance );
                IndexSample sample = populator.sampleResult();

                // THEN
                Value[] allValues = Arrays.copyOf( updates, updates.length + scanUpdates.length );
                System.arraycopy( asValues( scanUpdates ), 0, allValues, updates.length, scanUpdates.length );
                assertEquals( updates.length + scanUpdates.length, sample.sampleSize() );
                assertEquals( countUniqueValues( allValues ), sample.uniqueValues() );
                assertEquals( updates.length + scanUpdates.length, sample.indexSize() );
            }
            finally
            {
                populator.close( true );
            }
        }

        private Value[] asValues( IndexEntryUpdate<IndexDescriptor>[] updates )
        {
            Value[] values = new Value[updates.length];
            for ( int i = 0; i < updates.length; i++ )
            {
                values[i] = updates[i].values()[0];
            }
            return values;
        }
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
        try ( GBPTree<KEY,VALUE> ignored = new GBPTreeBuilder<>( pageCache, getIndexFile(), layout ).with( headerReader ).build() )
        {
            switch ( expectedState )
            {
            case ONLINE:
                assertEquals( "Index was not marked as online when expected not to be.", BYTE_ONLINE, headerReader.state );
                assertNull( "Expected failure message to be null when marked as online.", headerReader.failureMessage );
                break;
            case FAILED:
                assertEquals( "Index was marked as online when expected not to be.", BYTE_FAILED, headerReader.state );
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
                assertEquals( "Index was not left as populating when expected to be.", BYTE_POPULATING, headerReader.state );
                assertNull( "Expected failure message to be null when marked as populating.", headerReader.failureMessage );
                break;
            default:
                throw new UnsupportedOperationException( "Unexpected index state " + expectedState );
            }
        }
    }

    private String longString( int length )
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
        fs.mkdirs( getIndexFile().getParentFile() );
        try ( StoreChannel storeChannel = fs.create( getIndexFile() ) )
        {
            byte[] someBytes = new byte[size];
            random.nextBytes( someBytes );
            storeChannel.writeAll( ByteBuffer.wrap( someBytes ) );
            return someBytes;
        }
    }
}
