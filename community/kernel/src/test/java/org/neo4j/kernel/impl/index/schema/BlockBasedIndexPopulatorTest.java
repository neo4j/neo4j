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

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongPredicate;

import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.memory.ThreadSafePeakMemoryAllocationTracker;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.Barrier;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.actors.Actor;
import org.neo4j.test.extension.actors.ActorsExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.scheduler.JobSchedulerAdapter;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;
import static org.neo4j.kernel.impl.index.schema.BlockStorage.Monitor.NO_MONITOR;
import static org.neo4j.test.Race.throwing;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

@ActorsExtension
@EphemeralPageCacheExtension
class BlockBasedIndexPopulatorTest
{
    private static final LabelSchemaDescriptor SCHEMA_DESCRIPTOR = SchemaDescriptor.forLabel( 1, 1 );
    private static final IndexDescriptor INDEX_DESCRIPTOR = IndexPrototype.forSchema( SCHEMA_DESCRIPTOR ).withName( "index" ).materialise( 1 );

    @Inject
    Actor merger;
    @Inject
    Actor closer;
    @Inject
    FileSystemAbstraction fs;
    @Inject
    TestDirectory testDir;
    @Inject
    PageCache pageCache;

    private IndexFiles indexFiles;
    private DatabaseIndexContext databaseIndexContext;
    private JobScheduler jobScheduler;

    @BeforeEach
    void setup()
    {
        IndexProviderDescriptor providerDescriptor = new IndexProviderDescriptor( "test", "v1" );
        IndexDirectoryStructure directoryStructure = directoriesByProvider( testDir.homeDir() ).forProvider( providerDescriptor );
        indexFiles = new IndexFiles.Directory( fs, directoryStructure, INDEX_DESCRIPTOR.getId() );
        databaseIndexContext = DatabaseIndexContext.builder( pageCache, fs ).build();
        jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
    }

    @AfterEach
    void tearDown() throws Exception
    {
        jobScheduler.shutdown();
    }

    @Test
    void shouldAwaitMergeToBeFullyAbortedBeforeLeavingCloseMethod() throws Exception
    {
        // given
        TrappingMonitor monitor = new TrappingMonitor( ignore -> false );
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( monitor );
        boolean closed = false;
        try
        {
            populator.add( batchOfUpdates() );

            // when starting to merge (in a separate thread)
            Future<Object> mergeFuture = merger.submit( scanCompletedTask( populator ) );
            // and waiting for merge to get going
            monitor.barrier.awaitUninterruptibly();
            // calling close here should wait for the merge future, so that checking the merge future for "done" immediately afterwards must say true
            Future<Void> closeFuture = closer.submit( () -> populator.close( false ) );
            closer.untilWaiting();
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

    private Callable<Object> scanCompletedTask( BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator )
    {
        return () ->
        {
            populator.scanCompleted( nullInstance, jobScheduler );
            return null;
        };
    }

    @Test
    void shouldHandleBeingAbortedWhileMerging() throws Exception
    {
        // given
        TrappingMonitor monitor = new TrappingMonitor( numberOfBlocks -> numberOfBlocks == 2 );
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( monitor );
        boolean closed = false;
        try
        {
            populator.add( batchOfUpdates() );

            // when starting to merge (in a separate thread)
            Future<Object> mergeFuture = merger.submit( scanCompletedTask( populator ) );
            // and waiting for merge to get going
            monitor.barrier.await();
            monitor.barrier.release();
            monitor.mergeFinishedBarrier.awaitUninterruptibly();
            // calling close here should wait for the merge future, so that checking the merge future for "done" immediately afterwards must say true
            Future<Void> closeFuture = closer.submit( () -> populator.close( false ) );
            closer.untilWaiting();
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
    void shouldReportAccurateProgressThroughoutThePhases() throws Exception
    {
        // given
        TrappingMonitor monitor = new TrappingMonitor( numberOfBlocks -> numberOfBlocks == 1 );
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( monitor );
        try
        {
            populator.add( batchOfUpdates() );

            // when starting to merge (in a separate thread)
            Future<Object> mergeFuture = merger.submit( scanCompletedTask( populator ) );
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
    void shouldCorrectlyDecideToAwaitMergeDependingOnProgress() throws Throwable
    {
        // given
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( NO_MONITOR );
        boolean closed = false;
        try
        {
            populator.add( batchOfUpdates() );

            // when
            Race race = new Race();
            race.addContestant( throwing( () -> populator.scanCompleted( nullInstance, jobScheduler ) ) );
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
    void shouldDeleteDirectoryOnDrop() throws Exception
    {
        // given
        TrappingMonitor monitor = new TrappingMonitor( ignore -> false );
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( monitor );
        boolean closed = false;
        try
        {
            populator.add( batchOfUpdates() );

            // when starting to merge (in a separate thread)
            Future<Object> mergeFuture = merger.submit( scanCompletedTask( populator ) );
            // and waiting for merge to get going
            monitor.barrier.awaitUninterruptibly();
            // calling drop here should wait for the merge future and then delete index directory
            assertTrue( fs.fileExists( indexFiles.getBase() ) );
            assertTrue( fs.isDirectory( indexFiles.getBase() ) );
            assertTrue( fs.listFiles( indexFiles.getBase() ).length > 0 );

            Future<Void> dropFuture = closer.submit( populator::drop );
            closer.untilWaiting();
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
    void shouldDeallocateAllAllocatedMemoryOnClose() throws IndexEntryConflictException
    {
        // given
        ThreadSafePeakMemoryAllocationTracker memoryTracker = new ThreadSafePeakMemoryAllocationTracker();
        ByteBufferFactory bufferFactory = new ByteBufferFactory( () -> new UnsafeDirectByteBufferAllocator( memoryTracker ), 100 );
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( NO_MONITOR, GBPTree.NO_MONITOR, bufferFactory );
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
            populator.scanCompleted( nullInstance, jobScheduler );
            externalUpdates( populator, nextId, nextId + 10 );

            // then
            assertTrue( memoryTracker.peakMemoryUsage() > memoryBeforeScanCompleted,
                    "expected some memory to have been temporarily allocated in scanCompleted" );
            populator.close( true );
            assertEquals( memoryBeforeScanCompleted, memoryTracker.usedDirectMemory(), "expected all allocated memory to have been freed on close" );
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
    void shouldDeallocateAllAllocatedMemoryOnDrop() throws IndexEntryConflictException
    {
        // given
        ThreadSafePeakMemoryAllocationTracker memoryTracker = new ThreadSafePeakMemoryAllocationTracker();
        ByteBufferFactory bufferFactory = new ByteBufferFactory( () -> new UnsafeDirectByteBufferAllocator( memoryTracker ), 100 );
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( NO_MONITOR, GBPTree.NO_MONITOR, bufferFactory );
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
            populator.scanCompleted( nullInstance, jobScheduler );
            externalUpdates( populator, nextId, nextId + 10 );

            // then
            assertTrue( memoryTracker.peakMemoryUsage() > memoryBeforeScanCompleted,
                    "expected some memory to have been temporarily allocated in scanCompleted" );
            populator.drop();
            closed = true;
            assertEquals( memoryBeforeScanCompleted, memoryTracker.usedDirectMemory(), "expected all allocated memory to have been freed on drop" );

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
    void shouldBuildNonUniqueSampleAsPartOfScanCompleted() throws IndexEntryConflictException
    {
        // given
        ThreadSafePeakMemoryAllocationTracker memoryTracker = new ThreadSafePeakMemoryAllocationTracker();
        ByteBufferFactory bufferFactory = new ByteBufferFactory( () -> new UnsafeDirectByteBufferAllocator( memoryTracker ), 100 );
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( NO_MONITOR, GBPTree.NO_MONITOR, bufferFactory );
        Collection<IndexEntryUpdate<?>> populationUpdates = batchOfUpdates();
        populator.add( populationUpdates );

        // when
        populator.scanCompleted( nullInstance, jobScheduler );
        // Also a couple of updates afterwards
        int numberOfUpdatesAfterCompleted = 4;
        try ( IndexUpdater updater = populator.newPopulatingUpdater() )
        {
            for ( int i = 0; i < numberOfUpdatesAfterCompleted; i++ )
            {
                updater.process( IndexEntryUpdate.add( 10_000 + i, SCHEMA_DESCRIPTOR, intValue( i ) ) );
            }
        }
        populator.close( true );

        // then
        IndexSample sample = populator.sampleResult();
        assertEquals( populationUpdates.size(), sample.indexSize() );
        assertEquals( populationUpdates.size(), sample.sampleSize() );
        assertEquals( populationUpdates.size(), sample.uniqueValues() );
        assertEquals( numberOfUpdatesAfterCompleted, sample.updates() );
    }

    @Test
    void shouldFlushTreeOnScanCompleted() throws IndexEntryConflictException
    {
        // given
        ThreadSafePeakMemoryAllocationTracker memoryTracker = new ThreadSafePeakMemoryAllocationTracker();
        ByteBufferFactory bufferFactory = new ByteBufferFactory( () -> new UnsafeDirectByteBufferAllocator( memoryTracker ), 100 );
        AtomicInteger checkpoints = new AtomicInteger();
        GBPTree.Monitor treeMonitor = new GBPTree.Monitor.Adaptor()
        {
            @Override
            public void checkpointCompleted()
            {
                checkpoints.incrementAndGet();
            }
        };
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( NO_MONITOR, treeMonitor, bufferFactory );
        try
        {
            // when
            int numberOfCheckPointsBeforeScanCompleted = checkpoints.get();
            populator.scanCompleted( nullInstance, jobScheduler );

            // then
            assertEquals( numberOfCheckPointsBeforeScanCompleted + 1, checkpoints.get() );
        }
        finally
        {
            populator.close( true );
        }
    }

    @Test
    void shouldScheduleMergeOnJobSchedulerWithCorrectGroup() throws IndexEntryConflictException
    {
        // given
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( NO_MONITOR );
        boolean closed = false;
        try
        {
            populator.add( batchOfUpdates() );

            // when
            MutableBoolean called = new MutableBoolean();
            JobScheduler trackingJobScheduler = new JobSchedulerAdapter()
            {
                @Override
                public <T> JobHandle<T> schedule( Group group, Callable<T> job )
                {
                    called.setTrue();
                    assertThat( group ).isSameAs( Group.INDEX_POPULATION_WORK );
                    return jobScheduler.schedule( group, job );
                }
            };
            populator.scanCompleted( nullInstance, trackingJobScheduler );
            assertTrue( called.booleanValue() );
            populator.close( true );
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
        return instantiatePopulator( monitor, GBPTree.NO_MONITOR, heapBufferFactory( 100 ) );
    }

    private BlockBasedIndexPopulator<GenericKey,NativeIndexValue> instantiatePopulator( BlockStorage.Monitor monitor, GBPTree.Monitor treeMonitor,
            ByteBufferFactory bufferFactory )
    {
        IndexSpecificSpaceFillingCurveSettings spatialSettings = IndexSpecificSpaceFillingCurveSettings.fromConfig( Config.defaults() );
        GenericLayout layout = new GenericLayout( 1, spatialSettings );
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator =
                new BlockBasedIndexPopulator<>( databaseIndexContext, indexFiles, layout, INDEX_DESCRIPTOR, false, bufferFactory, 2, monitor, treeMonitor )
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

    private static IndexEntryUpdate<IndexDescriptor> add( int i )
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
