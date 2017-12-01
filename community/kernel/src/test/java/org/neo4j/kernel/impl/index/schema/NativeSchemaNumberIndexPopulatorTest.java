/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.schema;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
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

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.kernel.impl.index.schema.NativeSchemaNumberIndexPopulator.BYTE_FAILED;
import static org.neo4j.kernel.impl.index.schema.NativeSchemaNumberIndexPopulator.BYTE_ONLINE;

public abstract class NativeSchemaNumberIndexPopulatorTest<KEY extends SchemaNumberKey,VALUE extends SchemaNumberValue>
        extends SchemaNumberIndexTestUtil<KEY,VALUE>
{
    private static final int LARGE_AMOUNT_OF_UPDATES = 1_000;
    static final PropertyAccessor null_property_accessor = ( nodeId, propKeyId ) ->
    {
        throw new RuntimeException( "Did not expect an attempt to go to store" );
    };

    NativeSchemaNumberIndexPopulator<KEY,VALUE> populator;

    @Before
    public void setupPopulator()
    {
        IndexSamplingConfig samplingConfig = new IndexSamplingConfig( Config.defaults() );
        populator = createPopulator( pageCache, fs, indexFile, layout, samplingConfig );
    }

    abstract NativeSchemaNumberIndexPopulator<KEY,VALUE> createPopulator( PageCache pageCache, FileSystemAbstraction fs, File indexFile,
            Layout<KEY,VALUE> layout, IndexSamplingConfig samplingConfig );

    @Test
    public void createShouldCreateFile() throws Exception
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
        try ( StoreChannel r = fs.open( indexFile, OpenMode.READ ) )
        {
            byte[] firstBytes = new byte[someBytes.length];
            r.readAll( ByteBuffer.wrap( firstBytes ) );
            assertNotEquals( "Expected previous file content to have been cleared but was still there",
                    someBytes, firstBytes );
        }
        populator.close( true );
    }

    @Test
    public void dropShouldDeleteExistingFile() throws Exception
    {
        // given
        populator.create();

        // when
        populator.drop();

        // then
        assertFileNotPresent();
    }

    @Test
    public void dropShouldSucceedOnNonExistentFile() throws Exception
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

        // then
        populator.close( true );
    }

    @Test
    public void addShouldApplyAllUpdatesOnce() throws Exception
    {
        // given
        populator.create();
        @SuppressWarnings( "unchecked" )
        IndexEntryUpdate<IndexDescriptor>[] updates = layoutUtil.someUpdates();

        // when
        populator.add( Arrays.asList( updates ) );

        // then
        populator.close( true );
        verifyUpdates( updates );
    }

    @Test
    public void updaterShouldApplyUpdates() throws Exception
    {
        // given
        populator.create();
        IndexEntryUpdate<IndexDescriptor>[] updates = layoutUtil.someUpdates();
        try ( IndexUpdater updater = populator.newPopulatingUpdater( null_property_accessor ) )
        {
            // when
            for ( IndexEntryUpdate<IndexDescriptor> update : updates )
            {
                updater.process( update );
            }
        }

        // then
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
            updater.process( layoutUtil.add( 1, Values.of( Long.MAX_VALUE ) ) );
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
        IndexEntryUpdate<IndexDescriptor>[] updates = layoutUtil.someUpdates();

        // when
        applyInterleaved( updates, populator );

        // then
        populator.close( true );
        verifyUpdates( updates );
    }

    @Test
    public void successfulCloseMustCloseGBPTree() throws Exception
    {
        // given
        populator.create();
        Optional<PagedFile> existingMapping = pageCache.getExistingMapping( indexFile );
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
        existingMapping = pageCache.getExistingMapping( indexFile );
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
        assertHeader( true, null, false );
    }

    @Test
    public void unsuccessfulCloseMustSucceedWithoutMarkAsFailed() throws Exception
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
        Optional<PagedFile> existingMapping = pageCache.getExistingMapping( indexFile );
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
        existingMapping = pageCache.getExistingMapping( indexFile );
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
        assertHeader( false, "", false );
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
        assertHeader( false, failureMessage, false );
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
        assertHeader( false, failureMessage, true );
    }

    @Test
    public void successfulCloseMustThrowIfMarkedAsFailed() throws Exception
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
        catch ( IllegalStateException e )
        {
            // good
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
        Iterator<IndexEntryUpdate<IndexDescriptor>> updates = layoutUtil.randomUpdateGenerator( random );

        // when
        int count = interleaveLargeAmountOfUpdates( updaterRandom, updates );

        // then
        populator.close( true );
        random.reset();
        verifyUpdates( layoutUtil.randomUpdateGenerator( random ), count );
    }

    @Test
    public void dropMustSucceedAfterSuccessfulClose() throws Exception
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
    public void dropMustSucceedAfterUnsuccessfulClose() throws Exception
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
    public void successfulCloseMustThrowWithoutPriorSuccessfulCreate() throws Exception
    {
        // given
        assertFileNotPresent();

        // when
        try
        {
            populator.close( true );
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            // then good
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
        assertHeader( false, failureMessage, false );
    }

    @Test
    public void successfulCloseMustThrowAfterDrop() throws Exception
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
        catch ( IllegalStateException e )
        {
            // then good
        }
    }

    @Test
    public void unsuccessfulCloseMustThrowAfterDrop() throws Exception
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
        catch ( IllegalStateException e )
        {
            // then good
        }
    }

    private int interleaveLargeAmountOfUpdates( Random updaterRandom,
            Iterator<IndexEntryUpdate<IndexDescriptor>> updates ) throws IOException, IndexEntryConflictException
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

    private void assertHeader( boolean online, String failureMessage, boolean messageTruncated ) throws IOException
    {
        NativeSchemaIndexHeaderReader headerReader = new NativeSchemaIndexHeaderReader();
        try ( GBPTree<KEY,VALUE> ignored = new GBPTree<>( pageCache, indexFile, layout, 0, GBPTree.NO_MONITOR,
                headerReader, NO_HEADER_WRITER, RecoveryCleanupWorkCollector.IMMEDIATE ) )
        {
            if ( online )
            {
                assertEquals( "Index was not marked as online when expected not to be.", BYTE_ONLINE, headerReader.state );
                assertNull( "Expected failure message to be null when marked as online.", headerReader.failureMessage );
            }
            else
            {
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
            }
        }
    }

    private String longString( int length )
    {
        return RandomStringUtils.random( length, true, true );
    }

    private void applyInterleaved( IndexEntryUpdate<IndexDescriptor>[] updates, NativeSchemaNumberIndexPopulator<KEY,VALUE> populator )
            throws IOException, IndexEntryConflictException
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
        try ( StoreChannel storeChannel = fs.create( indexFile ) )
        {
            byte[] someBytes = new byte[size];
            random.nextBytes( someBytes );
            storeChannel.writeAll( ByteBuffer.wrap( someBytes ) );
            return someBytes;
        }
    }
}
