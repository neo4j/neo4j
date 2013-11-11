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
package org.neo4j.kernel.impl.nioneo.xa;

import java.util.ArrayList;

import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.Predicates;
import org.neo4j.helpers.PrimitiveIntPredicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RecordStore.Processor;

import static org.neo4j.kernel.api.index.NodePropertyUpdate.EMPTY_LONG_ARRAY;
import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.labelChanges;
import static org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField.parseLabelsField;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.safeCastLongToInt;

public class NeoStoreIndexStoreView implements IndexStoreView
{
    private final PropertyStore propertyStore;
    private final NodeStore nodeStore;

    public NeoStoreIndexStoreView( NeoStore neoStore )
    {
        this.propertyStore = neoStore.getPropertyStore();
        this.nodeStore = neoStore.getNodeStore();
    }

    @Override
    public <FAILURE extends Exception> StoreScan<FAILURE> visitNodesWithPropertyAndLabel(
            IndexDescriptor descriptor, Visitor<NodePropertyUpdate, FAILURE> visitor )
    {
        // Create a processor that for each accepted node (containing the desired label) looks through its properties,
        // getting the desired one (if any) and feeds to the index manipulator.
        LabelsReference labelsReference = new LabelsReference();
        RecordStore.Processor<FAILURE> processor = new NodePropertyUpdateProcessor<>( propertyStore,
                singleIntPredicate( descriptor.getPropertyKeyId() ),
                labelsReference, visitor );

        // Run the processor for the nodes containing the given label.
        // TODO When we've got a decent way of getting nodes with a label, use that instead.
        Predicate<NodeRecord> predicate = new NodeLabelFilterPredicate( nodeStore,
                singleIntPredicate( descriptor.getLabelId() ), labelsReference );

        // Run the processor, be sure that the predicate filters out removed nodes by checking in-use
        return new ProcessStoreScan<>( processor, predicate );
    }

    @Override
    public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes(
            int[] labelIds, int[] propertyKeyIds,
            Visitor<NodePropertyUpdate, FAILURE> propertyUpdateVisitor,
            Visitor<NodeLabelUpdate, FAILURE> labelUpdateVisitor )
    {
        // Create a processor that for each accepted node (containing the desired label) looks through its properties,
        // getting the desired one (if any) and feeds to the index manipulator.
        LabelsReference labelsReference = new LabelsReference();
        NodePropertyUpdateProcessor<FAILURE> propertyUpdateProcessor = new NodePropertyUpdateProcessor<>( propertyStore,
                multipleIntPredicate( propertyKeyIds ),
                labelsReference, propertyUpdateVisitor );
        Predicate<NodeRecord> predicate = new NodeLabelFilterPredicate( nodeStore,
                multipleIntPredicate( labelIds ), labelsReference );

        // Wrap the property processor inside another processor that processes everything, produces
        // label updates and delegates to the property processor.
        RecordStore.Processor<FAILURE> processor =
                new NodeProcessor<>( propertyUpdateProcessor, predicate, labelsReference, labelUpdateVisitor );

        // Processor (no filtering)
        //     --> NodeProcessor (processes label updates for all nodes)
        //           --> NodePropertyUpdateProcessor (processes property updates for relevant nodes)
        return new ProcessStoreScan<>( processor, Predicates.<NodeRecord>TRUE() );
    }

    @Override
    public Iterable<NodePropertyUpdate> nodeAsUpdates( long nodeId )
    {
        NodeRecord node = nodeStore.forceGetRecord( nodeId );
        if ( !node.inUse() )
        {
            return Iterables.empty(); // node not in use => no updates
        }
        long firstPropertyId = node.getCommittedNextProp();
        if ( firstPropertyId == Record.NO_NEXT_PROPERTY.intValue() )
        {
            return Iterables.empty(); // no properties => no updates (it's not going to be in any index)
        }
        long[] labels = parseLabelsField( node ).get( nodeStore );
        if ( labels.length == 0 )
        {
            return Iterables.empty(); // no labels => no updates (it's not going to be in any index)
        }
        ArrayList<NodePropertyUpdate> updates = new ArrayList<>();
        for ( PropertyRecord propertyRecord : propertyStore.getPropertyRecordChain( firstPropertyId ) )
        {
            for ( PropertyBlock property : propertyRecord.getPropertyBlocks() )
            {
                Object value = property.getType().getValue( property, propertyStore );
                updates.add( NodePropertyUpdate.add( node.getId(), property.getKeyIndexId(), value, labels ) );
            }
        }
        return updates;
    }

    /**
     * Used for sharing the extracted labels from the last processed node between the label and property key filter.
     * First the label predicate will be run (which will set the labels).
     * Then the property key filtering in the processor will be run (which will get the labels).
     * <p/>
     * All this to prevent extracting the label set two times per processed node.
     */
    private static class LabelsReference
    {
        private long[] labels;

        long[] get()
        {
            return this.labels;
        }

        void set( long[] labels )
        {
            this.labels = labels;
        }
    }

    private static PrimitiveIntPredicate singleIntPredicate( final int acceptedValue )
    {
        return new PrimitiveIntPredicate()
        {
            @Override
            public boolean accept( int value )
            {
                return value == acceptedValue;
            }
        };
    }

    private static PrimitiveIntPredicate multipleIntPredicate( final int... acceptedValues )
    {
        return new PrimitiveIntPredicate()
        {
            @Override
            public boolean accept( int value )
            {
                for ( int acceptedValue : acceptedValues )
                {
                    if ( value == acceptedValue )
                    {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    private class NodeProcessor<FAILURE extends Exception> extends RecordStore.Processor<FAILURE>
    {
        private final NodePropertyUpdateProcessor<FAILURE> propertyUpdateProcessor;
        private final Predicate<NodeRecord> propertyUpdateFilter;
        private final LabelsReference labelsReference;
        private final Visitor<NodeLabelUpdate, FAILURE> labelUpdateVisitor;

        NodeProcessor( NodePropertyUpdateProcessor<FAILURE> propertyUpdateProcessor,
                Predicate<NodeRecord> propertyUpdateFilter, LabelsReference labelsReference,
                Visitor<NodeLabelUpdate, FAILURE> labelUpdateVisitor )
        {
            this.propertyUpdateProcessor = propertyUpdateProcessor;
            this.propertyUpdateFilter = propertyUpdateFilter;
            this.labelsReference = labelsReference;
            this.labelUpdateVisitor = labelUpdateVisitor;
        }

        @Override
        public void processNode( RecordStore<NodeRecord> nodeStore, NodeRecord node ) throws FAILURE
        {
            if ( !node.inUse() )
            {
                return;
            }

            // We do this first since we know that calling the predicate will update the labels reference
            // with labels for the current node.
            boolean processPropertyUpdates = propertyUpdateFilter.accept( node );

            // label updates
            labelUpdateVisitor.visit( labelChanges( node.getId(), EMPTY_LONG_ARRAY, labelsReference.get() ) );

            // delegate to property updates
            if ( processPropertyUpdates )
            {
                propertyUpdateProcessor.processNode( nodeStore, node );
            }
        }
    }

    private class NodePropertyUpdateProcessor<FAILURE extends Exception> extends RecordStore.Processor<FAILURE>
    {
        private final PropertyStore propertyStore;
        private final Visitor<NodePropertyUpdate, FAILURE> visitor;
        private final PrimitiveIntPredicate propertyKeyPredicate;
        private final LabelsReference labelsReference;

        public NodePropertyUpdateProcessor( PropertyStore propertyStore,
                                      PrimitiveIntPredicate propertyKeyPredicate, LabelsReference labelsReference,
                                      Visitor<NodePropertyUpdate, FAILURE> visitor )
        {
            this.propertyStore = propertyStore;
            this.propertyKeyPredicate = propertyKeyPredicate;
            this.labelsReference = labelsReference;
            this.visitor = visitor;
        }

        @Override
        public void processNode( RecordStore<NodeRecord> nodeStore, NodeRecord node ) throws FAILURE
        {
            // TODO check cache if that property is in cache and use it, instead of loading it from the store.
            long firstPropertyId = node.getCommittedNextProp();
            if ( firstPropertyId == Record.NO_NEXT_PROPERTY.intValue() )
            {
                return;
            }

            // TODO optimize so that we don't have to load all property records, but instead stop
            // when we first find the property we're looking for.
            for ( PropertyRecord propertyRecord : propertyStore.getPropertyRecordChain( firstPropertyId ) )
            {
                for ( PropertyBlock property : propertyRecord.getPropertyBlocks() )
                {
                    int propertyKeyId = property.getKeyIndexId();
                    if ( propertyKeyPredicate.accept( propertyKeyId ) )
                    {
                        // Make sure the value is loaded, even if it's of a "heavy" kind.
                        propertyStore.ensureHeavy( property );
                        Object propertyValue = property.getType().getValue( property, propertyStore );

                        visitor.visit( NodePropertyUpdate.add( node.getId(), propertyKeyId, propertyValue,
                                labelsReference.get() ) );
                    }
                }
            }
        }
    }

    private class NodeLabelFilterPredicate implements Predicate<NodeRecord>
    {
        private final NodeStore nodeStore;
        private final PrimitiveIntPredicate labelPredicate;
        private final LabelsReference labelsReference;

        public NodeLabelFilterPredicate( NodeStore nodeStore, PrimitiveIntPredicate labelPredicate,
                                         LabelsReference labelsReference )
        {
            this.nodeStore = nodeStore;
            this.labelPredicate = labelPredicate;
            this.labelsReference = labelsReference;
        }

        @Override
        public boolean accept( NodeRecord node )
        {
            if ( node.inUse() )
            {
                long[] labelsForNode = parseLabelsField( node ).get( nodeStore );
                labelsReference.set( labelsForNode ); // Make these available for the processor for this node
                for ( long nodeLabelId : labelsForNode )
                {
                    if ( labelPredicate.accept( safeCastLongToInt( nodeLabelId ) ) )
                    {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private class ProcessStoreScan<FAILURE extends Exception> implements StoreScan<FAILURE>
    {
        private final Processor<FAILURE> processor;
        private final Predicate<NodeRecord> predicate;

        public ProcessStoreScan( Processor<FAILURE> processor, Predicate<NodeRecord> predicate )
        {
            this.processor = processor;
            this.predicate = predicate;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void run() throws FAILURE
        {
            processor.applyFiltered( nodeStore, predicate );
        }

        @Override
        public void stop()
        {
            processor.stopScanning();
        }
    }
}
