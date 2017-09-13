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
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.document.Document;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.api.impl.schema.writer.PartitionedIndexWriter;
import org.neo4j.logging.Log;

import static org.neo4j.kernel.api.impl.fulltext.LuceneFulltextDocumentStructure.documentRepresentingProperties;
import static org.neo4j.kernel.api.impl.fulltext.LuceneFulltextDocumentStructure.newTermForChangeOrRemove;

class FulltextUpdateApplier
{
    private static final FulltextIndexUpdate STOP_SIGNAL = () -> null;
    private final LinkedBlockingQueue<FulltextIndexUpdate> workQueue;
    private final ApplierThread workerThread;
    private final Log log;

    public FulltextUpdateApplier( Log log )
    {
        this.log = log;
        workQueue = new LinkedBlockingQueue<>();
        workerThread = new ApplierThread( workQueue, log );
    }

    <E extends Entity> BinaryLatch updatePropertyData( Map<Long,Map<String,Object>> state, WritableFulltext index ) throws
            IOException
    {
        BinaryLatch completedLatch = new BinaryLatch();
        FulltextIndexUpdate update = () ->
        {
            for ( Map.Entry<Long,Map<String,Object>> stateEntry : state.entrySet() )
            {
                Set<String> indexedProperties = index.properties();
                if ( !Collections.disjoint( indexedProperties, stateEntry.getValue().keySet() ) )
                {
                    long entityId = stateEntry.getKey();
                    Stream<Map.Entry<String,Object>> entryStream = stateEntry.getValue().entrySet().stream();
                    Predicate<Map.Entry<String,Object>> relevantForIndex =
                            entry -> indexedProperties.contains( entry.getKey() );
                    Map<String,Object> allProperties = entryStream.filter( relevantForIndex ).collect(
                            Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );

                    if ( !allProperties.isEmpty() )
                    {
                        Document document = documentRepresentingProperties( entityId, allProperties );
                        PartitionedIndexWriter indexWriter = index.getIndexWriter();
                        indexWriter.updateDocument( newTermForChangeOrRemove( entityId ), document );
                    }
                }
            }
            return Pair.of( index, completedLatch );
        };

        enqueueUpdate( update );
        return completedLatch;
    }

    <E extends Entity> BinaryLatch removePropertyData( Iterable<PropertyEntry<E>> propertyEntries,
                                                Map<Long,Map<String,Object>> state,
                                                WritableFulltext index ) throws IOException
    {
        BinaryLatch completedLatch = new BinaryLatch();
        FulltextIndexUpdate update = () ->
        {
            for ( PropertyEntry<E> propertyEntry : propertyEntries )
            {
                if ( index.properties().contains( propertyEntry.key() ) )
                {
                    long entityId = propertyEntry.entity().getId();
                    Map<String,Object> allProperties = state.get( entityId );
                    if ( allProperties == null || allProperties.isEmpty() )
                    {
                        index.getIndexWriter().deleteDocuments( newTermForChangeOrRemove( entityId ) );
                    }
                }
            }
            return Pair.of( index, completedLatch );
        };

        enqueueUpdate( update );
        return completedLatch;
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

    public void start()
    {
        workerThread.start();
    }

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
            workerThread.join();
        }
        catch ( InterruptedException e )
        {
            log.error( "Interrupted before " + workerThread.getName() + " could shut down.", e );
        }
    }

    private interface FulltextIndexUpdate
    {
        Pair<WritableFulltext, BinaryLatch> applyUpdateAndReturnIndex() throws IOException;
    }

    private static class ApplierThread extends Thread
    {
        private LinkedBlockingQueue<FulltextIndexUpdate> workQueue;
        private final Log log;

        public ApplierThread( LinkedBlockingQueue<FulltextIndexUpdate> workQueue, Log log )
        {
            super( "Fulltext Index Add-On Applier Thread" );
            this.workQueue = workQueue;
            this.log = log;
            setDaemon( true );
        }

        @Override
        public void run()
        {
            FulltextIndexUpdate update;
            while ( (update = getNextUpdate()) != STOP_SIGNAL )
            {
                Pair<WritableFulltext,BinaryLatch> updateProgress = applyUpdate( update );
                refreshIndex( updateProgress.first() );
                updateProgress.other().release();
            }
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
                    log.debug( getName() + " decided to ignore an interrupt.", e );
                }
            }
            while ( update == null );
            return update;
        }

        private Pair<WritableFulltext,BinaryLatch> applyUpdate( FulltextIndexUpdate update )
        {
            try
            {
                return update.applyUpdateAndReturnIndex();
            }
            catch ( IOException e )
            {
                log.error( "Failed to apply fulltext index update.", e );
            }
            return null;
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
            catch ( IOException e )
            {
                log.error( "Failed to refresh fulltext after updates", e );
            }
        }
    }
}
