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

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.api.index.StateFlipOver.StateFlip;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RecordStore.Processor;

/**
 * Represents one job of initially populating an index over existing data in the database.
 * Scans the store directly.
 * 
 * @author Mattias Persson
 */
class IndexPopulationJob implements Runnable
{
    private final long labelId;
    private final long propertyKeyId;
    private final IndexWriter populator;
    private final NeoStore neoStore;

    // NOTE: unbounded queue expected here
    private final Queue<NodePropertyUpdate> queue = new ConcurrentLinkedQueue<NodePropertyUpdate>();
    private final ThreadToStatementContextBridge ctxProvider;
    private final StateFlipOver flip;
    private final IndexRule indexRule;
    private final Collection<IndexPopulationJob> indexJobs;

    public IndexPopulationJob( IndexRule indexRule, IndexWriter populator, NeoStore neoStore,
                               ThreadToStatementContextBridge ctxProvider, StateFlipOver flip,
                               Collection<IndexPopulationJob> indexJobs )
    {
        this.indexRule = indexRule;
        this.flip = flip;
        this.indexJobs = indexJobs;
        this.labelId = indexRule.getLabel();
        this.propertyKeyId = indexRule.getPropertyKey();
        this.populator = populator;
        this.neoStore = neoStore;
        this.ctxProvider = ctxProvider;
    }
    
    @Override
    public void run()
    {
        populator.clear();
        
        indexAllNodes();
        
        StateFlip stateFlip = flip.flip( indexRule );
        try
        {
            populateFromQueueIfAvailable( Long.MAX_VALUE );
        }
        finally
        {
            indexJobs.remove( this );
            stateFlip.flop();
        }
    }

    @SuppressWarnings("unchecked")
    private void indexAllNodes()
    {
        // Create a processor that for each accepted node (containing the desired label) looks through its properties,
        // getting the desired one (if any) and feeds to the index manipulator.
        final PropertyStore propertyStore = neoStore.getPropertyStore();
        Processor processor = new NodeIndexingProcessor( propertyStore );

        // Run the processor for the nodes containing the given label.
        // TODO When we've got a decent way of getting nodes with a label, use that instead.
        final NodeStore nodeStore = neoStore.getNodeStore();
        final Predicate<NodeRecord> predicate = new NodeLabelFilterPredicate( nodeStore );
        processor.applyFiltered( nodeStore, predicate );
    }

    private void populateFromQueueIfAvailable( long highestIndexedNodeId )
    {
        for ( NodePropertyUpdate update = queue.poll(); update != null; update = queue.poll() )
        {
            boolean hasBeenIndexed = update.getNodeId() <= highestIndexedNodeId;
            if ( hasBeenIndexed )
                update.apply( populator );
        }
    }

    public void cancel()
    {
        // TODO
    }

    /**
     * A transaction happened that produced the given updates. Let this job incorporate its data
     * into, feeding it to the {@link IndexWriter}.
     */
    public void indexUpdates( Iterable<NodePropertyUpdate> updates )
    {
        // TODO synchronization with the index job thread
        for ( NodePropertyUpdate update : updates )
            if ( (update.getPropertyKeyId() == propertyKeyId) && (update.hasLabel( labelId )) )
                queue.add( update );
    }

    private class NodeIndexingProcessor extends Processor
    {
        private final PropertyStore propertyStore;

        public NodeIndexingProcessor( PropertyStore propertyStore )
        {
            this.propertyStore = propertyStore;
        }

        @Override
        public void processNode( RecordStore<NodeRecord> nodeStore, NodeRecord node )
        {
            // Actually index node record
            indexNodeRecord( node );

            // Process queued updates
            // TODO synchronization
            populateFromQueueIfAvailable( node.getId() );
        }

        private void indexNodeRecord( NodeRecord node )
        {
            // TODO check cache if that property is in cache and use it, instead of loading it from the store.

            long firstPropertyId = node.getCommittedNextProp();
            if ( firstPropertyId == Record.NO_NEXT_PROPERTY.intValue() )
                return;

            // TODO optimize so that we don't have to load all property records, but instead stop
            // when we first find the property we're looking for.
            for ( PropertyRecord propertyRecord : propertyStore.getPropertyRecordChain( firstPropertyId ) )
            {
                for ( PropertyBlock property : propertyRecord.getPropertyBlocks() )
                {
                    if ( property.getKeyIndexId() == propertyKeyId )
                    {
                        if ( property.getKeyIndexId() == propertyKeyId )
                        {
                            // Make sure the value is loaded, even if it's of a "heavy" kind.
                            propertyStore.makeHeavy( property );
                            Object propertyValue = property.getType().getValue( property, propertyStore );
                            populator.add( node.getId(), propertyValue );
                        }
                    }
                }
            }
        }
    }

    private class NodeLabelFilterPredicate implements Predicate<NodeRecord>
    {
        private final NodeStore nodeStore;

        public NodeLabelFilterPredicate( NodeStore nodeStore )
        {
            this.nodeStore = nodeStore;
        }

        @Override
        public boolean accept( NodeRecord node )
        {
            for ( long nodeLabelId : nodeStore.getLabelsForNode( node ) )
                if ( nodeLabelId == labelId )
                    return true;
            return false;
        }
    }
}
