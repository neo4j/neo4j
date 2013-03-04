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

import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.kernel.impl.api.index.IndexingService.singleContext;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
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
    private final long labelId;
    private final long propertyKeyId;
    private final IndexingService.IndexStoreView storeView;

    // NOTE: unbounded queue expected here
    private final Queue<NodePropertyUpdate> queue = new ConcurrentLinkedQueue<NodePropertyUpdate>();
    private final IndexPopulator writer;
    private final FlippableIndexContext flipper;
    private final StringLogger log;

    public IndexPopulationJob( IndexRule indexRule, IndexPopulator writer, FlippableIndexContext flipper,
                               IndexingService.IndexStoreView storeView, Logging logging )
    {
        this.writer = writer;
        this.flipper = flipper;
        this.labelId = indexRule.getLabel();
        this.propertyKeyId = indexRule.getPropertyKey();
        this.storeView = storeView;
        this.log = logging.getLogger( getClass() );
    }
    
    @Override
    public void run()
    {
        try
        {
            writer.createIndex();

            indexAllNodes();

            flipper.flip( new Runnable()
            {
                @Override
                public void run()
                {
                    populateFromQueueIfAvailable( Long.MAX_VALUE );
                    writer.populationCompleted();
                }
            }, new FailedIndexContext( writer ));
        }
        catch ( RuntimeException e )
        {
            log.error( "Failed to create index.", e );
            flipper.setFlipTarget( singleContext( new FailedIndexContext( writer, e ) ) );
            flipper.flip();
        }
        catch ( FlippableIndexContext.FlipFailedKernelException e )
        {
            log.error( "Failed to create index.", e );
            // The flipper will have already flipped to a failed index context here, but
            // it will not include the cause of failure, so we do another flip to a failed
            // context that does.

            // The reason for having the flipper transition to the failed index context in the first
            // place is that we would otherwise introduce a race condition where updates could come
            // in to the old context, if something failed in the job we send to the flipper.
            flipper.setFlipTarget( singleContext( new FailedIndexContext( writer, e ) ) );
            flipper.flip();
        }
    }

    @SuppressWarnings("unchecked")
    private void indexAllNodes()
    {
        storeView.visitNodesWithPropertyAndLabel( labelId, propertyKeyId, new Visitor<Pair<Long, Object>>(){
            @Override
            public boolean visit( Pair<Long, Object> element )
            {
                writer.add( element.first(), element.other() );
                populateFromQueueIfAvailable( element.first() );

                return false;
            }
        });
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

            writer.update( filter( hasBeenIndexed, queue ) );
        }
    }

    public void cancel()
    {
        // TODO
    }

    /**
     * A transaction happened that produced the given updates. Let this job incorporate its data
     * into, feeding it to the {@link IndexPopulator}.
     * @param updates
     */
    public void update( Iterable<NodePropertyUpdate> updates )
    {
        for ( NodePropertyUpdate update : updates )
            queue.add( update );
    }
}
