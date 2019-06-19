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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.LongPredicate;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.ThreadSafePeakMemoryAllocationTracker;
import org.neo4j.storageengine.api.schema.IndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.test.Barrier;
import org.neo4j.test.Race;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.test.rule.concurrent.OtherThreadRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexProvider.Monitor.EMPTY;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;
import static org.neo4j.kernel.impl.index.schema.BlockStorage.Monitor.NO_MONITOR;
import static org.neo4j.kernel.impl.index.schema.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.test.OtherThreadExecutor.command;
import static org.neo4j.test.Race.throwing;
import static org.neo4j.values.storable.Values.stringValue;

public class BlockBasedIndexPopulatorTest
{
    private static final StoreIndexDescriptor INDEX_DESCRIPTOR = IndexDescriptorFactory.forSchema( SchemaDescriptorFactory.forLabel( 1, 1 ) ).withId( 1 );

    @Rule
    public final PageCacheAndDependenciesRule storage = new PageCacheAndDependenciesRule();

    @Rule
    public final OtherThreadRule<Void> t2 = new OtherThreadRule<>( "MERGER" );

    @Rule
    public final OtherThreadRule<Void> t3 = new OtherThreadRule<>( "CLOSER" );

    private IndexDirectoryStructure directoryStructure;
    private File indexDir;
    private File indexFile;
    private FileSystemAbstraction fs;
    private IndexDropAction dropAction;

    @Before
    public void setup()
    {
        IndexProviderDescriptor providerDescriptor = new IndexProviderDescriptor( "test", "v1" );
        directoryStructure = directoriesByProvider( storage.directory().databaseDir() ).forProvider( providerDescriptor );
        indexDir = directoryStructure.directoryForIndex( INDEX_DESCRIPTOR.getId() );
        indexFile = new File( indexDir, "index" );
        fs = storage.fileSystem();
        dropAction = new FileSystemIndexDropAction( fs, directoryStructure );
    }

    @Test
    public void shouldAwaitMergeToBeFullyAbortedBeforeLeavingCloseMethod() throws Exception
    {
        // given
        TrappingMonitor monitor = new TrappingMonitor( ignore -> false );
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( monitor );
        boolean closed = false;
        try
        {
            populator.add( batchOfUpdates() );

            // when starting to merge (in a separate thread)
            Future<Object> mergeFuture = t2.execute( command( () -> populator.scanCompleted( nullInstance ) ) );
            // and waiting for merge to get going
            monitor.barrier.awaitUninterruptibly();
            // calling close here should wait for the merge future, so that checking the merge future for "done" immediately afterwards must say true
            Future<Object> closeFuture = t3.execute( command( () -> populator.close( false ) ) );
            t3.get().waitUntilWaiting();
            monitor.barrier.release();
            closeFuture.get();
            closed = true;

            // then
            assertTrue( mergeFuture.isDone() );
        }
        finally
        {
            if ( !closed )
            {
                populator.close( true );
            }
        }
    }

    @Test
    public void shouldHandleBeingAbortedWhileMerging() throws Exception
    {
        // given
        TrappingMonitor monitor = new TrappingMonitor( numberOfBlocks -> numberOfBlocks == 2 );
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( monitor );
        boolean closed = false;
        try
        {
            populator.add( batchOfUpdates() );

            // when starting to merge (in a separate thread)
            Future<Object> mergeFuture = t2.execute( command( () -> populator.scanCompleted( nullInstance ) ) );
            // and waiting for merge to get going
            monitor.barrier.await();
            monitor.barrier.release();
            monitor.mergeFinishedBarrier.awaitUninterruptibly();
            // calling close here should wait for the merge future, so that checking the merge future for "done" immediately afterwards must say true
            Future<Object> closeFuture = t3.execute( command( () -> populator.close( false ) ) );
            t3.get().waitUntilWaiting();
            monitor.mergeFinishedBarrier.release();
            closeFuture.get();
            closed = true;

            // then let's make sure scanComplete was cancelled, not throwing exception or anything.
            mergeFuture.get();
        }
        finally
        {
            if ( !closed )
            {
                populator.close( false );
            }
        }
    }

    @Test
    public void shouldReportAccurateProgressThroughoutThePhases() throws Exception
    {
        // given
        TrappingMonitor monitor = new TrappingMonitor( numberOfBlocks -> numberOfBlocks == 1 );
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( monitor );
        try
        {
            populator.add( batchOfUpdates() );

            // when starting to merge (in a separate thread)
            Future<Object> mergeFuture = t2.execute( command( () -> populator.scanCompleted( nullInstance ) ) );
            // and waiting for merge to get going
            monitor.barrier.awaitUninterruptibly();
            // this is a bit fuzzy, but what we want is to assert that the scan doesn't represent 100% of the work
            assertEquals( 0.5f, populator.progress( PopulationProgress.DONE ).getProgress(), 0.1f );
            monitor.barrier.release();
            monitor.mergeFinishedBarrier.awaitUninterruptibly();
            assertEquals( 0.7f, populator.progress( PopulationProgress.DONE ).getProgress(), 0.1f );
            monitor.mergeFinishedBarrier.release();
            mergeFuture.get();
            assertEquals( 1f, populator.progress( PopulationProgress.DONE ).getProgress(), 0f );
        }
        finally
        {
            populator.close( true );
        }
    }

    @Test
    public void shouldCorrectlyDecideToAwaitMergeDependingOnProgress() throws Throwable
    {
        // given
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( NO_MONITOR );
        boolean closed = false;
        try
        {
            populator.add( batchOfUpdates() );

            // when
            Race race = new Race();
            race.addContestant( throwing( () -> populator.scanCompleted( nullInstance ) ) );
            race.addContestant( throwing( () -> populator.close( false ) ) );
            race.go();
            closed = true;

            // then regardless of who wins (close/merge) after close call returns no files should still be mapped
            EphemeralFileSystemAbstraction ephemeralFileSystem = (EphemeralFileSystemAbstraction) fs;
            ephemeralFileSystem.assertNoOpenFiles();
        }
        finally
        {
            if ( !closed )
            {
                populator.close( true );
            }
        }
    }

    @Test
    public void shouldDeleteDirectoryOnDrop() throws Exception
    {
        // given
        TrappingMonitor monitor = new TrappingMonitor( ignore -> false );
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( monitor );
        boolean closed = false;
        try
        {
            populator.add( batchOfUpdates() );

            // when starting to merge (in a separate thread)
            Future<Object> mergeFuture = t2.execute( command( () -> populator.scanCompleted( nullInstance ) ) );
            // and waiting for merge to get going
            monitor.barrier.awaitUninterruptibly();
            // calling drop here should wait for the merge future and then delete index directory
            assertTrue( fs.fileExists( indexDir ) );
            assertTrue( fs.isDirectory( indexDir ) );
            assertTrue( fs.listFiles( indexDir ).length > 0 );

            Future<Object> dropFuture = t3.execute( command( populator::drop ) );
            t3.get().waitUntilWaiting();
            monitor.barrier.release();
            dropFuture.get();
            closed = true;

            // then
            assertTrue( mergeFuture.isDone() );
            assertFalse( fs.fileExists( indexDir ) );
        }
        finally
        {
            if ( !closed )
            {
                populator.close( true );
            }
        }
    }

    @Test
    public void shouldDeallocateAllAllocatedMemoryOnClose() throws IndexEntryConflictException
    {
        // given
        ThreadSafePeakMemoryAllocationTracker memoryTracker = new ThreadSafePeakMemoryAllocationTracker( new LocalMemoryTracker() );
        ByteBufferFactory bufferFactory = new ByteBufferFactory( () -> new UnsafeDirectByteBufferAllocator( memoryTracker ), 100 );
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( NO_MONITOR, bufferFactory );
        boolean closed = false;
        try
        {
            // when
            Collection<IndexEntryUpdate<?>> updates = batchOfUpdates();
            populator.add( updates );
            int nextId = updates.size();
            externalUpdates( populator, nextId, nextId + 10 );
            nextId = nextId + 10;
            long memoryBeforeScanCompleted = memoryTracker.usedDirectMemory();
            populator.scanCompleted( nullInstance );
            externalUpdates( populator, nextId, nextId + 10 );

            // then
            assertTrue( "expected some memory to have been temporarily allocated in scanCompleted",
                    memoryTracker.peakMemoryUsage() > memoryBeforeScanCompleted );
            populator.close( true );
            assertEquals( "expected all allocated memory to have been freed on close", memoryBeforeScanCompleted, memoryTracker.usedDirectMemory() );
            closed = true;

            bufferFactory.close();
            assertEquals( 0, memoryTracker.usedDirectMemory() );
        }
        finally
        {
            if ( !closed )
            {
                populator.close( true );
            }
        }
    }

    @Test
    public void shouldDeallocateAllAllocatedMemoryOnDrop() throws IndexEntryConflictException
    {
        // given
        ThreadSafePeakMemoryAllocationTracker memoryTracker = new ThreadSafePeakMemoryAllocationTracker( new LocalMemoryTracker() );
        ByteBufferFactory bufferFactory = new ByteBufferFactory( () -> new UnsafeDirectByteBufferAllocator( memoryTracker ), 100 );
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( NO_MONITOR, bufferFactory );
        boolean closed = false;
        try
        {
            // when
            Collection<IndexEntryUpdate<?>> updates = batchOfUpdates();
            populator.add( updates );
            int nextId = updates.size();
            externalUpdates( populator, nextId, nextId + 10 );
            nextId = nextId + 10;
            long memoryBeforeScanCompleted = memoryTracker.usedDirectMemory();
            populator.scanCompleted( nullInstance );
            externalUpdates( populator, nextId, nextId + 10 );

            // then
            assertTrue( "expected some memory to have been temporarily allocated in scanCompleted",
                    memoryTracker.peakMemoryUsage() > memoryBeforeScanCompleted );
            populator.drop();
            closed = true;
            assertEquals( "expected all allocated memory to have been freed on drop", memoryBeforeScanCompleted, memoryTracker.usedDirectMemory() );

            bufferFactory.close();
            assertEquals( 0, memoryTracker.usedDirectMemory() );
        }
        finally
        {
            if ( !closed )
            {
                populator.close( true );
            }
        }
    }

    private void externalUpdates( BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator, int firstId, int lastId )
            throws IndexEntryConflictException
    {
        try ( IndexUpdater updater = populator.newPopulatingUpdater() )
        {
            for ( int i = firstId; i < lastId; i++ )
            {
                updater.process( add( i ) );
            }
        }
    }

    private BlockBasedIndexPopulator<GenericKey,NativeIndexValue> instantiatePopulator( BlockStorage.Monitor monitor )
    {
        return instantiatePopulator( monitor, heapBufferFactory( 100 ) );
    }

    private BlockBasedIndexPopulator<GenericKey,NativeIndexValue> instantiatePopulator( BlockStorage.Monitor monitor, ByteBufferFactory bufferFactory )
    {
        Config config = Config.defaults();
        ConfiguredSpaceFillingCurveSettingsCache settingsCache = new ConfiguredSpaceFillingCurveSettingsCache( config );
        IndexSpecificSpaceFillingCurveSettingsCache spatialSettings = new IndexSpecificSpaceFillingCurveSettingsCache( settingsCache, new HashMap<>() );
        GenericLayout layout = new GenericLayout( 1, spatialSettings );
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator =
                new BlockBasedIndexPopulator<GenericKey,NativeIndexValue>( storage.pageCache(), fs, indexFile, layout, EMPTY,
                        INDEX_DESCRIPTOR, spatialSettings, directoryStructure, dropAction, false, bufferFactory, 2, monitor )
                {
                    @Override
                    NativeIndexReader<GenericKey,NativeIndexValue> newReader()
                    {
                        throw new UnsupportedOperationException( "Not needed in this test" );
                    }
                };
        populator.create();
        return populator;
    }

    private static Collection<IndexEntryUpdate<?>> batchOfUpdates()
    {
        List<IndexEntryUpdate<?>> updates = new ArrayList<>();
        for ( int i = 0; i < 50; i++ )
        {
            updates.add( add( i ) );
        }
        return updates;
    }

    private static IndexEntryUpdate<StoreIndexDescriptor> add( int i )
    {
        return IndexEntryUpdate.add( i, INDEX_DESCRIPTOR, stringValue( "Value" + i ) );
    }

    private static class TrappingMonitor extends BlockStorage.Monitor.Adapter
    {
        private final Barrier.Control barrier = new Barrier.Control();
        private final Barrier.Control mergeFinishedBarrier = new Barrier.Control();
        private final LongPredicate trapForMergeIterationFinished;

        TrappingMonitor( LongPredicate trapForMergeIterationFinished )
        {
            this.trapForMergeIterationFinished = trapForMergeIterationFinished;
        }

        @Override
        public void mergedBlocks( long resultingBlockSize, long resultingEntryCount, long numberOfBlocks )
        {
            barrier.reached();
        }

        @Override
        public void mergeIterationFinished( long numberOfBlocksBefore, long numberOfBlocksAfter )
        {
            if ( trapForMergeIterationFinished.test( numberOfBlocksAfter ) )
            {
                mergeFinishedBarrier.reached();
            }
        }
    }
}
