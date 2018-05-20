/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.document.Document;
import org.eclipse.collections.api.map.primitive.LongObjectMap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.function.ThrowingAction;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.helpers.collection.CollectorsUtil;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.api.impl.schema.writer.PartitionedIndexWriter;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.util.concurrent.BinaryLatch;

import static org.neo4j.kernel.api.impl.fulltext.LuceneFulltextDocumentStructure.documentRepresentingProperties;
import static org.neo4j.kernel.api.impl.fulltext.LuceneFulltextDocumentStructure.newTermForChangeOrRemove;

class FulltextUpdateApplier extends LifecycleAdapter
{
    private static final FulltextIndexUpdate STOP_SIGNAL = new FulltextIndexUpdate( null, null );
    private static final int POPULATING_BATCH_SIZE = 10_000;
    private static final JobScheduler.Group UPDATE_APPLIER = new JobScheduler.Group( "FulltextIndexUpdateApplier" );
    private static final String APPLIER_THREAD_NAME = "Fulltext Index Add-On Applier Thread";

    private final LinkedBlockingQueue<FulltextIndexUpdate> workQueue;
    private final Log log;
    private final AvailabilityGuard availabilityGuard;
    private final JobScheduler scheduler;
    private JobScheduler.JobHandle workerThread;

    FulltextUpdateApplier( Log log, AvailabilityGuard availabilityGuard, JobScheduler scheduler )
    {
        this.log = log;
        this.availabilityGuard = availabilityGuard;
        this.scheduler = scheduler;
        workQueue = new LinkedBlockingQueue<>();
    }

    <E extends Entity> AsyncFulltextIndexOperation updatePropertyData(
            LongObjectMap<Map<String, Object>> state, WritableFulltext index ) throws IOException
    {
        FulltextIndexUpdate update = new FulltextIndexUpdate( index, () ->
        {
            PartitionedIndexWriter indexWriter = index.getIndexWriter();
            state.forEachKeyValue( ( entityId, value ) ->
            {
                Set<String> indexedProperties = index.getProperties();
                if ( !Collections.disjoint( indexedProperties, value.keySet() ) )
                {
                    Stream<Map.Entry<String,Object>> entryStream = value.entrySet().stream();
                    Predicate<Map.Entry<String,Object>> relevantForIndex =
                            entry -> indexedProperties.contains( entry.getKey() );
                    Map<String,Object> allProperties = entryStream.filter( relevantForIndex )
                            .collect( CollectorsUtil.entriesToMap() );

                    if ( !allProperties.isEmpty() )
                    {
                        updateDocument( indexWriter, entityId, allProperties );
                    }
                }
            } );
        } );

        enqueueUpdate( update );
        return update;
    }

    private static void updateDocument( PartitionedIndexWriter indexWriter, long entityId, Map<String, Object> properties )
    {
        Document document = documentRepresentingProperties( entityId, properties );
        try
        {
            indexWriter.updateDocument( newTermForChangeOrRemove( entityId ), document );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    <E extends Entity> AsyncFulltextIndexOperation removePropertyData(
            Iterable<PropertyEntry<E>> propertyEntries, LongObjectMap<Map<String, Object>> state, WritableFulltext index )
            throws IOException
    {
        FulltextIndexUpdate update = new FulltextIndexUpdate( index, () ->
        {
            for ( PropertyEntry<E> propertyEntry : propertyEntries )
            {
                if ( index.getProperties().contains( propertyEntry.key() ) )
                {
                    long entityId = propertyEntry.entity().getId();
                    Map<String,Object> allProperties = state.get( entityId );
                    if ( allProperties == null || allProperties.isEmpty() )
                    {
                        index.getIndexWriter().deleteDocuments( newTermForChangeOrRemove( entityId ) );
                    }
                }
            }
        } );

        enqueueUpdate( update );
        return update;
    }

    AsyncFulltextIndexOperation writeBarrier() throws IOException
    {
        FulltextIndexUpdate barrier = new FulltextIndexUpdate( null, ThrowingAction.noop() );
        enqueueUpdate( barrier );
        return barrier;
    }

    AsyncFulltextIndexOperation populateNodes( WritableFulltext index, GraphDatabaseService db ) throws IOException
    {
        return enqueuePopulateIndex( index, db, db::getAllNodes );
    }

    AsyncFulltextIndexOperation populateRelationships( WritableFulltext index, GraphDatabaseService db )
            throws IOException
    {
        return enqueuePopulateIndex( index, db, db::getAllRelationships );
    }

    private AsyncFulltextIndexOperation enqueuePopulateIndex(
            WritableFulltext index, GraphDatabaseService db,
            Supplier<ResourceIterable<? extends Entity>> entitySupplier ) throws IOException
    {
        FulltextIndexUpdate population = new FulltextIndexUpdate( index, () ->
        {
            try
            {
                PartitionedIndexWriter indexWriter = index.getIndexWriter();
                String[] indexedPropertyKeys = index.getProperties().toArray( new String[0] );
                ArrayList<Supplier<Document>> documents = new ArrayList<>();
                try ( Transaction ignore = db.beginTx( 1, TimeUnit.DAYS ) )
                {
                    ResourceIterable<? extends Entity> entities = entitySupplier.get();
                    for ( Entity entity : entities )
                    {
                        long entityId = entity.getId();
                        Map<String,Object> properties = entity.getProperties( indexedPropertyKeys );
                        if ( !properties.isEmpty() )
                        {
                            documents.add( documentBuilder( entityId, properties ) );
                        }

                        if ( documents.size() > POPULATING_BATCH_SIZE )
                        {
                            indexWriter.addDocuments( documents.size(), reifyDocuments( documents ) );
                            documents.clear();
                        }
                    }
                }
                indexWriter.addDocuments( documents.size(), reifyDocuments( documents ) );
                index.setPopulated();
            }
            catch ( Throwable th )
            {
                if ( index != null )
                {
                    index.setFailed();
                }
                throw th;
            }
        } );

        enqueueUpdate( population );
        return population;
    }

    private Supplier<Document> documentBuilder( long entityId, Map<String,Object> properties )
    {
        return () -> documentRepresentingProperties( entityId, properties );
    }

    private Iterable<Document> reifyDocuments( ArrayList<Supplier<Document>> documents )
    {
        return () -> documents.stream().map( Supplier::get ).iterator();
    }

    private void enqueueUpdate( FulltextIndexUpdate update ) throws IOException
    {
        try
        {
            workQueue.put( update );
        }
        catch ( InterruptedException e )
        {
            throw new IOException( "Fulltext index update failed.", e );
        }
    }

    @Override
    public void start()
    {
        if ( workerThread != null )
        {
            throw new IllegalStateException( APPLIER_THREAD_NAME + " already started." );
        }
        workerThread = scheduler.schedule( UPDATE_APPLIER, new ApplierWorker( workQueue, log, availabilityGuard ) );
    }

    @Override
    public void stop()
    {
        boolean enqueued;
        do
        {
            enqueued = workQueue.offer( STOP_SIGNAL );
        }
        while ( !enqueued );

        try
        {
            workerThread.waitTermination();
            workerThread = null;
        }
        catch ( InterruptedException e )
        {
            log.error( "Interrupted before " + APPLIER_THREAD_NAME + " could shut down.", e );
        }
        catch ( ExecutionException e )
        {
            log.error( "Exception while waiting for " + APPLIER_THREAD_NAME + " to shut down.", e );
        }
    }

    private static class FulltextIndexUpdate extends BinaryLatch implements AsyncFulltextIndexOperation
    {
        private final WritableFulltext index;
        private final ThrowingAction<IOException> action;
        private volatile Throwable throwable;

        private FulltextIndexUpdate( WritableFulltext index, ThrowingAction<IOException> action )
        {
            this.index = index;
            this.action = action;
        }

        @Override
        public void awaitCompletion() throws ExecutionException
        {
            super.await();
            Throwable th = this.throwable;
            if ( th != null )
            {
                throw new ExecutionException( th );
            }
        }

        void applyUpdate()
        {
            try
            {
                action.apply();
            }
            catch ( Throwable e )
            {
                throwable = e;
            }
        }
    }

    private static class ApplierWorker implements Runnable
    {
        private LinkedBlockingQueue<FulltextIndexUpdate> workQueue;
        private final Log log;
        private final AvailabilityGuard availabilityGuard;

        ApplierWorker( LinkedBlockingQueue<FulltextIndexUpdate> workQueue, Log log,
                       AvailabilityGuard availabilityGuard )
        {
            this.workQueue = workQueue;
            this.log = log;
            this.availabilityGuard = availabilityGuard;
        }

        @Override
        public void run()
        {
            Thread.currentThread().setName( APPLIER_THREAD_NAME );
            waitForDatabaseToBeAvailable();
            Set<WritableFulltext> refreshableSet = new HashSet<>();
            List<BinaryLatch> latches = new ArrayList<>();

            FulltextIndexUpdate update;
            while ( (update = getNextUpdate()) != STOP_SIGNAL )
            {
                update = drainQueueAndApplyUpdates( update, refreshableSet, latches );
                refreshAndClearIndexes( refreshableSet );
                releaseAndClearLatches( latches );

                if ( update == STOP_SIGNAL )
                {
                    return;
                }
            }
        }

        private void waitForDatabaseToBeAvailable()
        {
            boolean isAvailable;
            do
            {
                isAvailable = availabilityGuard.isAvailable( 100 );
            }
            while ( !isAvailable && !availabilityGuard.isShutdown() );
        }

        private FulltextIndexUpdate drainQueueAndApplyUpdates(
                FulltextIndexUpdate update,
                Set<WritableFulltext> refreshableSet,
                List<BinaryLatch> latches )
        {
            do
            {
                applyUpdate( update, refreshableSet, latches );
                update = workQueue.poll();
            }
            while ( update != null && update != STOP_SIGNAL );
            return update;
        }

        private void refreshAndClearIndexes( Set<WritableFulltext> refreshableSet )
        {
            for ( WritableFulltext index : refreshableSet )
            {
                refreshIndex( index );
            }
            refreshableSet.clear();
        }

        private void releaseAndClearLatches( List<BinaryLatch> latches )
        {
            for ( BinaryLatch latch : latches )
            {
                latch.release();
            }
            latches.clear();
        }

        private FulltextIndexUpdate getNextUpdate()
        {
            FulltextIndexUpdate update = null;
            do
            {
                try
                {
                    update = workQueue.take();
                }
                catch ( InterruptedException e )
                {
                    log.debug( APPLIER_THREAD_NAME + " decided to ignore an interrupt.", e );
                }
            }
            while ( update == null );
            return update;
        }

        private void applyUpdate( FulltextIndexUpdate update,
                                  Set<WritableFulltext> refreshableSet,
                                  List<BinaryLatch> latches )
        {
            latches.add( update );
            update.applyUpdate();
            refreshableSet.add( update.index );
        }

        private void refreshIndex( WritableFulltext index )
        {
            try
            {
                if ( index != null )
                {
                    index.maybeRefreshBlocking();
                }
            }
            catch ( Throwable e )
            {
                log.error( "Failed to refresh fulltext after updates.", e );
            }
        }
    }
}
