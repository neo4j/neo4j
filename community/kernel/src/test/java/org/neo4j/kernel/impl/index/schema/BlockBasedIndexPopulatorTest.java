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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.LongPredicate;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.schema.SchemaDescriptorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProviderDescriptor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryAllocationTracker;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.Barrier;
import org.neo4j.test.Race;
import org.neo4j.test.rule.OtherThreadRule;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexProvider.Monitor.EMPTY;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;
import static org.neo4j.kernel.impl.index.schema.BlockStorage.Monitor.NO_MONITOR;
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

    private IndexFiles indexFiles;
    private FileSystemAbstraction fs;

    @Before
    public void setup()
    {
        fs = storage.fileSystem();
        IndexProviderDescriptor providerDescriptor = new IndexProviderDescriptor( "test", "v1" );
        IndexDirectoryStructure directoryStructure = directoriesByProvider( storage.directory().storeDir() ).forProvider( providerDescriptor );
        indexFiles = new IndexFiles.Directory( fs, directoryStructure, INDEX_DESCRIPTOR.getId() );
    }

    @Test
    public void shouldAwaitMergeToBeFullyAbortedBeforeLeavingCloseMethod() throws Exception
    {
        // given
        TrappingMonitor monitor = new TrappingMonitor( ignore -> false );
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( monitor, new LocalMemoryTracker() );
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
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( monitor, new LocalMemoryTracker() );
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
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( monitor, new LocalMemoryTracker() );
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
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( NO_MONITOR, new LocalMemoryTracker() );
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
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( monitor, new LocalMemoryTracker() );
        boolean closed = false;
        try
        {
            populator.add( batchOfUpdates() );

            // when starting to merge (in a separate thread)
            Future<Object> mergeFuture = t2.execute( command( () -> populator.scanCompleted( nullInstance ) ) );
            // and waiting for merge to get going
            monitor.barrier.awaitUninterruptibly();
            // calling drop here should wait for the merge future and then delete index directory
            assertTrue( fs.fileExists( indexFiles.getBase() ) );
            assertTrue( fs.isDirectory( indexFiles.getBase() ) );
            assertTrue( fs.listFiles( indexFiles.getBase() ).length > 0 );

            Future<Object> dropFuture = t3.execute( command( populator::drop ) );
            t3.get().waitUntilWaiting();
            monitor.barrier.release();
            dropFuture.get();
            closed = true;

            // then
            assertTrue( mergeFuture.isDone() );
            assertFalse( fs.fileExists( indexFiles.getBase() ) );
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
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( NO_MONITOR, memoryTracker );
        boolean closed = false;
        try
        {
            // when
            Collection<IndexEntryUpdate<?>> updates = batchOfUpdates();
            populator.add( updates );
            int nextId = updates.size();
            externalUpdates( populator, nextId, nextId + 10 );
            nextId = nextId + 10;
            populator.scanCompleted( nullInstance );
            externalUpdates( populator, nextId, nextId + 10 );

            // then
            assertTrue( "expected some memory to be in use", memoryTracker.usedDirectMemory() > 0 );
            populator.close( true );
            assertEquals( "expected all allocated memory to have been freed on close", 0, memoryTracker.usedDirectMemory() );
            closed = true;
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
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( NO_MONITOR, memoryTracker );
        boolean closed = false;
        try
        {
            // when
            Collection<IndexEntryUpdate<?>> updates = batchOfUpdates();
            populator.add( updates );
            int nextId = updates.size();
            externalUpdates( populator, nextId, nextId + 10 );
            nextId = nextId + 10;
            populator.scanCompleted( nullInstance );
            externalUpdates( populator, nextId, nextId + 10 );

            // then
            assertTrue( "expected some memory to be in use", memoryTracker.usedDirectMemory() > 0 );
            populator.drop();
            closed = true;
            assertEquals( "expected all allocated memory to have been freed on drop", 0, memoryTracker.usedDirectMemory() );
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

    private BlockBasedIndexPopulator<GenericKey,NativeIndexValue> instantiatePopulator( BlockStorage.Monitor monitor, MemoryAllocationTracker memoryTracker )
    {
        Config config = Config.defaults();
        ConfiguredSpaceFillingCurveSettingsCache settingsCache = new ConfiguredSpaceFillingCurveSettingsCache( config );
        IndexSpecificSpaceFillingCurveSettingsCache spatialSettings = new IndexSpecificSpaceFillingCurveSettingsCache( settingsCache, new HashMap<>() );
        GenericLayout layout = new GenericLayout( 1, spatialSettings );
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator =
                new BlockBasedIndexPopulator<GenericKey,NativeIndexValue>( storage.pageCache(), storage.fileSystem(), indexFiles, layout, EMPTY,
                        INDEX_DESCRIPTOR, spatialSettings, false, 100, 2, monitor, memoryTracker )
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
