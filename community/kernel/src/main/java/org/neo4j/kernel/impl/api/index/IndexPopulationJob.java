/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import static java.lang.String.format;
import static org.neo4j.helpers.FutureAdapter.latchGuardedValue;
import static org.neo4j.helpers.ValueGetter.NO_VALUE;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.kernel.impl.api.index.IndexingService.singleProxy;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.impl.api.index.IndexingService.StoreScan;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

/**
 * Represents one job of initially populating an index over existing data in the database.
 * Scans the store directly.
 * 
 * @author Mattias Persson
 */
public class IndexPopulationJob implements Runnable
{
    private final IndexingService.IndexStoreView storeView;

    // NOTE: unbounded queue expected here
    private final Queue<NodePropertyUpdate> queue = new ConcurrentLinkedQueue<NodePropertyUpdate>();

    private final IndexDescriptor descriptor;
    private final IndexPopulator populator;
    private final FlippableIndexProxy flipper;
    private final UpdateableSchemaState updateableSchemaState;
    private final StringLogger log;
    private final CountDownLatch doneSignal = new CountDownLatch( 1 );

    private volatile StoreScan storeScan;
    private volatile boolean cancelled;

    public IndexPopulationJob( IndexDescriptor descriptor, IndexPopulator populator, FlippableIndexProxy flipper,
                               IndexingService.IndexStoreView storeView, UpdateableSchemaState updateableSchemaState,
                               Logging logging )
    {
        this.descriptor = descriptor;
        this.populator = populator;
        this.flipper = flipper;
        this.storeView = storeView;
        this.updateableSchemaState = updateableSchemaState;
        this.log = logging.getLogger( getClass() );
    }
    
    @Override
    public void run()
    {
        boolean success = false;
        try
        {
            log.info( format( "Index population started for label id %d on property id %d",
                    descriptor.getLabelId(), descriptor.getPropertyKeyId() ) );
            populator.create();

            indexAllNodes();
            if ( cancelled )
                // We remain in POPULATING state
                return;

            Callable<Void> duringFlip = new Callable<Void>()
            {
                @Override
                public Void call() throws IOException
                {
                    populateFromQueueIfAvailable( Long.MAX_VALUE );
                    populator.close( true );
                    updateableSchemaState.flush();
                    return null;
                }
            };

            FailedIndexProxy failureTarget = new FailedIndexProxy( descriptor, populator );

            flipper.flip( duringFlip, failureTarget );
            success = true;
            log.info( format( "Index population completed for label id %d on property id %d, index is now online.",
                    descriptor.getLabelId(), descriptor.getPropertyKeyId() ) );
        }
        catch ( Throwable t )
        {
            log.error( "Failed to populate index.", t );
            // The flipper will have already flipped to a failed index context here, but
            // it will not include the cause of failure, so we do another flip to a failed
            // context that does.
    
            // The reason for having the flipper transition to the failed index context in the first
            // place is that we would otherwise introduce a race condition where updates could come
            // in to the old context, if something failed in the job we send to the flipper.
            flipper.setFlipTarget( singleProxy( new FailedIndexProxy( descriptor, populator, t ) ) );
            flipper.flip();
        }
        finally
        {
            try
            {
                if ( !success )
                    populator.close( false );
            }
            catch ( Throwable e )
            {
                log.warn( "Unable to close failed populator", e );
            }
            finally
            {
                doneSignal.countDown();
            }
        }
    }

    private void indexAllNodes()
    {
        storeScan = storeView.visitNodesWithPropertyAndLabel( descriptor, new Visitor<Pair<Long, Object>>()
        {
            @Override
            public boolean visit( Pair<Long, Object> element )
            {
                populator.add( element.first(), element.other() );
                populateFromQueueIfAvailable( element.first() );

                return false;
            }
        });
        storeScan.run();
    }

    private void populateFromQueueIfAvailable( final long highestIndexedNodeId )
    {
        if ( !queue.isEmpty() )
        {
            Predicate<NodePropertyUpdate> hasBeenIndexed = new Predicate<NodePropertyUpdate>()
            {
                @Override
                public boolean accept( NodePropertyUpdate item )
                {
                    return item.getNodeId() <= highestIndexedNodeId;
                }
            };

            populator.update( filter( hasBeenIndexed, queue ) );
        }
    }

    public Future<Void> cancel()
    {
        // Stop the population
        if ( storeScan != null )
        {
            cancelled = true;
            storeScan.stop();
        }
        
        return latchGuardedValue( NO_VALUE, doneSignal );
    }

    /**
     * A transaction happened that produced the given updates. Let this job incorporate its data
     * into, feeding it to the {@link IndexPopulator}.
     */
    public void update( Iterable<NodePropertyUpdate> updates )
    {
        for ( NodePropertyUpdate update : updates )
            queue.add( update );
    }
    
    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[populator:" + populator + ",descriptor:" + descriptor + "]";
    }
}
