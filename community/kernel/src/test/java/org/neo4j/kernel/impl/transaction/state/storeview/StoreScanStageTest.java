/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.index.StoreScan.ExternalUpdatesCheck;
import org.neo4j.kernel.impl.transaction.state.storeview.PropertyAwareEntityStoreScan.CursorEntityIdIterator;
import org.neo4j.lock.Lock;
import org.neo4j.lock.LockService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StubStorageCursors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.eclipse.collections.impl.block.factory.primitive.IntPredicates.alwaysTrue;
import static org.neo4j.internal.batchimport.staging.ExecutionSupervisors.superviseDynamicExecution;
import static org.neo4j.internal.batchimport.staging.ProcessorAssignmentStrategies.eagerRandomSaturation;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.api.index.StoreScan.NO_EXTERNAL_UPDATES;
import static org.neo4j.test.DoubleLatch.awaitLatch;
import static org.neo4j.values.storable.Values.stringValue;

class StoreScanStageTest
{
    private static final int PARALLELISM = 4;
    private static final int LABEL = 1;
    private static final String KEY = "key";
    private static final int NUMBER_OF_BATCHES = 4;

    private final Config dbConfig = Config.defaults( GraphDatabaseInternalSettings.index_population_workers, PARALLELISM );
    private final Configuration config = new Configuration()
    {
        @Override
        public int maxNumberOfProcessors()
        {
            return PARALLELISM * 2;
        }

        @Override
        public int batchSize()
        {
            return 10;
        }
    };

    @ValueSource( booleans = {true, false} )
    @ParameterizedTest( name = "parallelWrite={0}" )
    void shouldGenerateUpdatesInParallel( boolean parallelWrite )
    {
        // given
        StubStorageCursors data = someData();
        EntityIdIterator entityIdIterator = new CursorEntityIdIterator<>( data.allocateNodeCursor( NULL ) );
        ThreadCapturingWriter<List<EntityUpdates>> propertyUpdateVisitor = new ThreadCapturingWriter<>();
        ThreadCapturingWriter<List<EntityTokenUpdate>> tokenUpdateVisitor = new ThreadCapturingWriter<>();
        ControlledLockFunction lockFunction = new ControlledLockFunction();
        StoreScanStage<RuntimeException,StorageNodeCursor> scan =
                new StoreScanStage<>( dbConfig, config, ct -> entityIdIterator, NO_EXTERNAL_UPDATES, new AtomicBoolean( true ), data, new int[]{LABEL},
                        alwaysTrue(), propertyUpdateVisitor, tokenUpdateVisitor, new NodeCursorBehaviour( data ), lockFunction, parallelWrite,
                        PageCacheTracer.NULL, EmptyMemoryTracker.INSTANCE );

        // when
        runScan( scan );

        // then it completes and we see > 1 threads
        assertThat( lockFunction.seenThreads.size() ).isGreaterThan( 1 );
        if ( parallelWrite )
        {
            assertThat( propertyUpdateVisitor.seenThreads.size() ).isGreaterThan( 1 );
            assertThat( tokenUpdateVisitor.seenThreads.size() ).isGreaterThan( 1 );
        }
        else
        {
            assertThat( propertyUpdateVisitor.seenThreads.size() ).isEqualTo( 1 );
            assertThat( tokenUpdateVisitor.seenThreads.size() ).isEqualTo( 1 );
        }
    }

    @Test
    void shouldPanicAndExitStageOnWriteFailure()
    {
        // given
        StubStorageCursors data = someData();
        EntityIdIterator entityIdIterator = new CursorEntityIdIterator<>( data.allocateNodeCursor( NULL ) );
        Visitor<List<EntityUpdates>,RuntimeException> failingWriter = updates ->
        {
            throw new IllegalStateException( "Failed to write" );
        };
        StoreScanStage<RuntimeException,StorageNodeCursor> scan =
                new StoreScanStage<>( dbConfig, config, ct -> entityIdIterator, NO_EXTERNAL_UPDATES, new AtomicBoolean( true ), data, new int[]{LABEL},
                        alwaysTrue(), failingWriter, null, new NodeCursorBehaviour( data ), id -> null, true, PageCacheTracer.NULL,
                        EmptyMemoryTracker.INSTANCE );

        // when/then
        assertThatThrownBy( () -> runScan( scan ) ).isInstanceOf( IllegalStateException.class ).hasMessageContaining( "Failed to write" );
    }

    @Test
    void shouldApplyExternalUpdatesIfThereAreSuch()
    {
        // given
        StubStorageCursors data = someData();
        EntityIdIterator entityIdIterator = new CursorEntityIdIterator<>( data.allocateNodeCursor( NULL ) );
        AtomicInteger numBatchesProcessed = new AtomicInteger();
        ControlledExternalUpdatesCheck externalUpdatesCheck = new ControlledExternalUpdatesCheck( config.batchSize(), 2, numBatchesProcessed );
        Visitor<List<EntityUpdates>,RuntimeException> writer = updates ->
        {
            numBatchesProcessed.incrementAndGet();
            return false;
        };
        StoreScanStage<RuntimeException,StorageNodeCursor> scan =
                new StoreScanStage<>( dbConfig, config, ct -> entityIdIterator, externalUpdatesCheck, new AtomicBoolean( true ), data, new int[]{LABEL},
                        alwaysTrue(), writer, null, new NodeCursorBehaviour( data ), id -> null, true, PageCacheTracer.NULL, EmptyMemoryTracker.INSTANCE );

        // when
        runScan( scan );

        // then
        assertThat( externalUpdatesCheck.applyCallCount ).isEqualTo( 1 );
    }

    @Test
    void shouldAbortScanOnStopped()
    {
        // given
        StubStorageCursors data = someData();
        EntityIdIterator entityIdIterator = new CursorEntityIdIterator<>( data.allocateNodeCursor( NULL ) );
        AtomicInteger numBatchesProcessed = new AtomicInteger();
        AtomicBoolean continueScanning = new AtomicBoolean( true );
        AbortingExternalUpdatesCheck externalUpdatesCheck = new AbortingExternalUpdatesCheck( 1, continueScanning );
        Visitor<List<EntityUpdates>,RuntimeException> writer = updates ->
        {
            numBatchesProcessed.incrementAndGet();
            return false;
        };
        StoreScanStage<RuntimeException,StorageNodeCursor> scan =
                new StoreScanStage<>( dbConfig, config, ct -> entityIdIterator, externalUpdatesCheck, continueScanning, data, new int[]{LABEL}, alwaysTrue(),
                        writer, null, new NodeCursorBehaviour( data ), id -> null, true, PageCacheTracer.NULL, EmptyMemoryTracker.INSTANCE );

        // when
        runScan( scan );

        // then
        assertThat( numBatchesProcessed.get() ).isEqualTo( 2 );
    }

    @Test
    void shouldReportCorrectNumberOfEntitiesProcessed()
    {
        // given
        StubStorageCursors data = someData();
        AtomicReference<StoreScanStage<RuntimeException,StorageNodeCursor>> stage = new AtomicReference<>();
        EntityIdIterator entityIdIterator = new CursorEntityIdIterator<>( data.allocateNodeCursor( NULL ) )
        {
            private long manualCounter;

            @Override
            protected boolean fetchNext()
            {
                assertThat( stage.get().numberOfIteratedEntities() ).isEqualTo( (manualCounter / config.batchSize()) * config.batchSize() );
                manualCounter++;
                return super.fetchNext();
            }
        };
        StoreScanStage<RuntimeException,StorageNodeCursor> scan =
                new StoreScanStage<>( dbConfig, config, ct -> entityIdIterator, NO_EXTERNAL_UPDATES, new AtomicBoolean( true ), data, new int[]{LABEL},
                        alwaysTrue(), new ThreadCapturingWriter<>(), new ThreadCapturingWriter<>(), new NodeCursorBehaviour( data ), l -> LockService.NO_LOCK,
                        true, PageCacheTracer.NULL, EmptyMemoryTracker.INSTANCE );
        stage.set( scan );

        // when
        runScan( scan );

        // then
        assertThat( scan.numberOfIteratedEntities() ).isEqualTo( (long) config.batchSize() * NUMBER_OF_BATCHES );
    }

    private void runScan( StoreScanStage<RuntimeException,StorageNodeCursor> scan )
    {
        superviseDynamicExecution( eagerRandomSaturation( config.maxNumberOfProcessors() ), scan );
    }

    private StubStorageCursors someData()
    {
        StubStorageCursors data = new StubStorageCursors();
        for ( int i = 0; i < config.batchSize() * NUMBER_OF_BATCHES; i++ )
        {
            data.withNode( i ).labels( LABEL ).properties( KEY, stringValue( "name_" + i ) );
        }
        return data;
    }

    private static class ControlledLockFunction implements LongFunction<Lock>
    {
        private final Set<Thread> seenThreads = ConcurrentHashMap.newKeySet();
        private final CountDownLatch latch = new CountDownLatch( 2 );

        @Override
        public Lock apply( long id )
        {
            // We know that there'll be > 1 updates generator thread, therefore block the first batch that comes in
            // and let another one trigger us to continue. This proves that there are at least 2 threads generating updates
            seenThreads.add( Thread.currentThread() );
            latch.countDown();
            awaitLatch( latch );
            return null;
        }
    }

    private static class ControlledExternalUpdatesCheck implements ExternalUpdatesCheck
    {
        private final int expectedNodeId;
        private final int applyOnBatchIndex;
        private final AtomicInteger numBatchesProcessed;
        private int checkCallCount;
        private volatile int applyCallCount;

        ControlledExternalUpdatesCheck( int batchSize, int applyOnBatchIndex, AtomicInteger numBatchesProcessed )
        {
            this.applyOnBatchIndex = applyOnBatchIndex;
            this.numBatchesProcessed = numBatchesProcessed;
            this.expectedNodeId = batchSize * applyOnBatchIndex - 1;
        }

        @Override
        public boolean needToApplyExternalUpdates()
        {
            return checkCallCount++ == applyOnBatchIndex;
        }

        @Override
        public void applyExternalUpdates( long currentlyIndexedNodeId )
        {
            assertThat( currentlyIndexedNodeId ).isEqualTo( expectedNodeId );
            assertThat( numBatchesProcessed.get() ).isEqualTo( applyOnBatchIndex );
            applyCallCount++;
        }
    }

    private static class ThreadCapturingWriter<T> implements Visitor<T,RuntimeException>
    {
        private final Set<Thread> seenThreads = ConcurrentHashMap.newKeySet();

        @Override
        public boolean visit( T element ) throws RuntimeException
        {
            seenThreads.add( Thread.currentThread() );
            return false;
        }
    }

    // Used to hook into the ReadEntityIdsStep to know when to trigger the stop, it's not actually doing external updates
    // The way this is called in the step is that disabling scanning won't trigger until the next batch
    private static class AbortingExternalUpdatesCheck implements ExternalUpdatesCheck
    {
        private final int abortAfterBatch;
        private final AtomicBoolean continueScanning;
        private int callCount;

        AbortingExternalUpdatesCheck( int abortAfterBatch, AtomicBoolean continueScanning )
        {
            this.abortAfterBatch = abortAfterBatch;
            this.continueScanning = continueScanning;
        }

        @Override
        public boolean needToApplyExternalUpdates()
        {
            if ( callCount++ == abortAfterBatch )
            {
                continueScanning.set( false );
            }
            return false;
        }

        @Override
        public void applyExternalUpdates( long currentlyIndexedNodeId )
        {
            throw new IllegalStateException( "Should not be called" );
        }
    }
}
