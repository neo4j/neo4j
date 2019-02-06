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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Arrays.asList;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier.EMPTY;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexEntryUpdate.add;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.kernel.configuration.Config.defaults;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
import static org.neo4j.storageengine.api.schema.IndexDescriptorFactory.forSchema;
import static org.neo4j.values.storable.Values.longValue;

@ExtendWith( TestDirectoryExtension.class )
class ParallelNativeIndexPopulatorTest
{
    private static final int THREADS = 4;
    private static final StoreIndexDescriptor DESCRIPTOR = forSchema( forLabel( 1, 1 ), GenericNativeIndexProvider.DESCRIPTOR ).withId( 1 );

    @Inject
    TestDirectory directory;

    private final Config defaults = defaults();
    private final ConfiguredSpaceFillingCurveSettingsCache settingsCache = new ConfiguredSpaceFillingCurveSettingsCache( defaults );
    private final IndexSpecificSpaceFillingCurveSettingsCache spatialSettings =
            new IndexSpecificSpaceFillingCurveSettingsCache( settingsCache, new HashMap<>() );
    private final GenericLayout layout = new GenericLayout( 1, spatialSettings );
    private final AtomicLong next = new AtomicLong();
    private ExecutorService executorService;
    private File baseIndexFile;

    @BeforeEach
    void startExecutor()
    {
        executorService = Executors.newFixedThreadPool( THREADS );
        baseIndexFile = directory.file( "index" );
    }

    @AfterEach
    void stopExecutor()
    {
        executorService.shutdown();
    }

    @Test
    void shouldCreateThreadLocalParts() throws ExecutionException, InterruptedException, IndexEntryConflictException
    {
        // given
        Thread mainThread = Thread.currentThread();
        ConcurrentMap<Thread,NativeIndexPopulator> partPopulators = new ConcurrentHashMap<>();
        ParallelNativeIndexPopulator<GenericKey,NativeIndexValue> populator =
                new ParallelNativeIndexPopulator<>( baseIndexFile, layout, mockPartSupplier( partPopulators, this::mockNativeIndexPopulator ) );

        // when
        int batchCountPerThread = 10;
        applyBatchesInParallel( populator, batchCountPerThread );

        // then
        assertEquals( THREADS, partPopulators.size() );
        for ( Thread thread : partPopulators.keySet() )
        {
            if ( thread != mainThread )
            {
                NativeIndexPopulator partPopulator = partPopulators.get( thread );
                verify( partPopulator, times( batchCountPerThread ) ).add( anyCollection() );
            }
        }
    }

    @Test
    void shouldApplyUpdatesOnEachPart() throws ExecutionException, InterruptedException, IndexEntryConflictException
    {
        // given
        Thread mainThread = Thread.currentThread();
        ConcurrentMap<Thread,NativeIndexPopulator> partPopulators = new ConcurrentHashMap<>();
        ParallelNativeIndexPopulator<GenericKey,NativeIndexValue> populator =
                new ParallelNativeIndexPopulator<>( baseIndexFile, layout, mockPartSupplier( partPopulators, this::mockNativeIndexPopulator ) );
        int batchCountPerThread = 10;

        // when
        applyBatchesInParallel( populator, batchCountPerThread );
        applyUpdates( populator, next );
        applyBatchesInParallel( populator, batchCountPerThread );
        applyUpdates( populator, next );
        applyBatchesInParallel( populator, batchCountPerThread );

        // then
        assertEquals( THREADS, partPopulators.size() );
        for ( Thread thread : partPopulators.keySet() )
        {
            if ( thread != mainThread )
            {
                NativeIndexPopulator partPopulator = partPopulators.get( thread );
                verify( partPopulator, times( batchCountPerThread * 3 ) ).add( anyCollection() );
                CountingIndexUpdater updater = (CountingIndexUpdater) partPopulator.newPopulatingUpdater();
                assertEquals( 10, updater.count );
            }
        }
    }

    @Test
    void shouldDropAllPartsOnClose() throws ExecutionException, InterruptedException
    {
        // Only test false since merging on these mocked populators will fail
        shouldDropAllParts( populator -> populator.close( false ) );
    }

    @Test
    void shouldDropAllPartsOnDrop() throws ExecutionException, InterruptedException
    {
        shouldDropAllParts( ParallelNativeIndexPopulator::drop );
    }

    private void shouldDropAllParts( Consumer<ParallelNativeIndexPopulator<GenericKey,NativeIndexValue>> method )
            throws ExecutionException, InterruptedException
    {
        // given
        ConcurrentMap<Thread,NativeIndexPopulator> partPopulators = new ConcurrentHashMap<>();
        ParallelNativeIndexPopulator<GenericKey,NativeIndexValue> populator =
                new ParallelNativeIndexPopulator<>( baseIndexFile, layout, mockPartSupplier( partPopulators, this::failOnDropNativeIndexPopulator ) );
        populator.create();
        applyBatchesInParallel( populator, 1 );

        // when
        try
        {
            method.accept( populator );
            fail( "Should have failed" );
        }
        catch ( CustomFailure e )
        {
            // then good
        }

        // then
        for ( NativeIndexPopulator part : partPopulators.values() )
        {
            verify( part ).drop();
        }
    }

    @Test
    void shouldMergePartsOnAccessingVerifyDeferredConstraintsAfterPopulation() throws Exception
    {
        shouldMergePartsOnAccessingFirstCompleteMethodAfterPopulation( populator -> populator.verifyDeferredConstraints( mock( NodePropertyAccessor.class ) ) );
    }

    @Test
    void shouldMergePartsOnAccessingSampleResultAfterPopulation() throws Exception
    {
        shouldMergePartsOnAccessingFirstCompleteMethodAfterPopulation( ParallelNativeIndexPopulator::sampleResult );
    }

    private void shouldMergePartsOnAccessingFirstCompleteMethodAfterPopulation(
            ThrowingConsumer<ParallelNativeIndexPopulator<GenericKey,NativeIndexValue>,IndexEntryConflictException> method ) throws Exception
    {
        try ( EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction(); JobScheduler jobScheduler = createInitialisedScheduler() )
        {
            SingleFilePageSwapperFactory swapper = new SingleFilePageSwapperFactory();
            swapper.open( fs, defaults );
            try ( PageCache pageCache = new MuninnPageCache( swapper, 1_000, PageCacheTracer.NULL, PageCursorTracerSupplier.NULL, EMPTY, jobScheduler ) )
            {
                // given
                NativeIndexPopulatorPartSupplier<GenericKey,NativeIndexValue> partSupplier =
                        file -> new GenericNativeIndexPopulator( pageCache, (FileSystemAbstraction) fs, file, layout, IndexProvider.Monitor.EMPTY, DESCRIPTOR,
                                spatialSettings, directoriesByProvider( directory.directory() ).forProvider( GenericNativeIndexProvider.DESCRIPTOR ),
                                mock( SpaceFillingCurveConfiguration.class ), false, !file.equals( baseIndexFile ) );
                ParallelNativeIndexPopulator<GenericKey,NativeIndexValue> populator = new ParallelNativeIndexPopulator<>( baseIndexFile, layout, partSupplier );
                try
                {
                    populator.create();
                    applyBatchesInParallel( populator, 100 );

                    // when
                    method.accept( populator );

                    // then
                    NodeValueIterator results = new NodeValueIterator();
                    try ( NativeIndexReader<GenericKey,NativeIndexValue> reader = populator.newReader() )
                    {
                        reader.query( results, IndexOrder.NONE, true, IndexQuery.exists( 1 ) );
                        long nextExpectedId = 0;
                        while ( results.hasNext() )
                        {
                            long id = results.next();
                            assertEquals( nextExpectedId++, id );
                        }
                        assertEquals( nextExpectedId, next.get() );
                    }
                }
                finally
                {
                    populator.close( true );
                }
            }
        }
    }

    private NativeIndexPopulatorPartSupplier<GenericKey,NativeIndexValue> mockPartSupplier( ConcurrentMap<Thread,NativeIndexPopulator> partPopulators,
            Supplier<NativeIndexPopulator<GenericKey,NativeIndexValue>> partSupplier )
    {
        return file ->
        {
            NativeIndexPopulator<GenericKey,NativeIndexValue> part = partSupplier.get();
            if ( !file.equals( baseIndexFile ) )
            {
                assertNull( partPopulators.put( Thread.currentThread(), part ) );
            }
            return part;
        };
    }

    private NativeIndexPopulator<GenericKey,NativeIndexValue> mockNativeIndexPopulator()
    {
        NativeIndexPopulator<GenericKey,NativeIndexValue> populator = mock( NativeIndexPopulator.class );
        when( populator.newPopulatingUpdater() ).thenReturn( new CountingIndexUpdater() );
        return populator;
    }

    private NativeIndexPopulator<GenericKey,NativeIndexValue> failOnDropNativeIndexPopulator()
    {
        NativeIndexPopulator<GenericKey,NativeIndexValue> populator = mockNativeIndexPopulator();
        doThrow( CustomFailure.class ).when( populator ).drop();
        return populator;
    }

    private void applyUpdates( ParallelNativeIndexPopulator<GenericKey,NativeIndexValue> populator, AtomicLong next ) throws IndexEntryConflictException
    {
        try ( IndexUpdater updater = populator.newPopulatingUpdater( mock( NodePropertyAccessor.class ) ) )
        {
            for ( int i = 0; i < 5; i++ )
            {
                updater.process( update( next.incrementAndGet() ) );
            }
        }
    }

    private void applyBatchesInParallel( ParallelNativeIndexPopulator<GenericKey,NativeIndexValue> populator, int batchCountPerThread )
            throws ExecutionException, InterruptedException
    {
        CountDownLatch startSignal = new CountDownLatch( THREADS );
        List<Future<Void>> futures = new ArrayList<>();
        for ( int i = 0; i < THREADS; i++ )
        {
            futures.add( executorService.submit( () ->
            {
                // Wait for all to get into pole position, this is because we want to make sure all threads are used.
                startSignal.countDown();
                startSignal.await();

                for ( int j = 0; j < batchCountPerThread; j++ )
                {
                    populator.add( asList( update( next.getAndIncrement() ) ) );
                }
                return null;
            } ) );
        }
        for ( Future<Void> future : futures )
        {
            future.get();
        }
    }

    private IndexEntryUpdate<?> update( long id )
    {
        return add( id, DESCRIPTOR, longValue( id ) );
    }

    private static class CustomFailure extends RuntimeException
    {
    }

    private static class CountingIndexUpdater implements IndexUpdater
    {
        private int count;

        @Override
        public void process( IndexEntryUpdate<?> update )
        {
            count++;
        }

        @Override
        public void close()
        {
        }
    }
}
