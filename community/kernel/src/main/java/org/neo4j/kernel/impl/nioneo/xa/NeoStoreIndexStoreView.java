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

import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.flatMap;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.asIterable;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;

import java.util.Iterator;
import java.util.Set;

import org.neo4j.helpers.Function;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
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
    public Iterator<Pair<Integer, Object>> getNodeProperties( final long nodeId, final Iterator<Long>
            propertyKeysIterator )
    {
        long firstPropertyId = nodeStore.forceGetRecord( nodeId ).getNextProp();

        if ( firstPropertyId == Record.NO_NEXT_PROPERTY.intValue() )
            return emptyIterator();

        final Set<Long> propertyKeys = asSet( asIterable( propertyKeysIterator ) );

        return flatMap( new Function<PropertyRecord, Iterator<Pair<Integer, Object>>>()
        {
            @Override
            public Iterator<Pair<Integer, Object>> apply( PropertyRecord propertyRecord )
            {
                return filter( notNull(),
                          map( propertiesThatAreIn( propertyKeys ), propertyRecord.getPropertyBlocks().iterator() ) );
            }
        }, propertyStore.getPropertyRecordChain( firstPropertyId ).iterator());
    }

    @Override
    public <FAILURE extends Exception> StoreScan<FAILURE> visitNodesWithPropertyAndLabel( IndexDescriptor descriptor,
            Visitor<NodePropertyUpdate, FAILURE> visitor )
    {
        return visitNodes( singleLongPredicate( descriptor.getPropertyKeyId() ),
                singleLongPredicate( descriptor.getLabelId() ), visitor );
    }
    
    @Override
    public <FAILURE extends Exception> StoreScan<FAILURE>  visitNodes( long[] labelIds, long[] propertyKeyIds,
                                                                       Visitor<NodePropertyUpdate, FAILURE> visitor )
    {
        return visitNodes( multipleLongPredicate( propertyKeyIds ), multipleLongPredicate( labelIds ), visitor );
    }

    private <FAILURE extends Exception> StoreScan<FAILURE>  visitNodes( PrimitiveLongPredicate propertyKeyPredicate,
                                                                        PrimitiveLongPredicate labelPredicate,
            Visitor<NodePropertyUpdate, FAILURE> visitor )
    {
        // Create a processor that for each accepted node (containing the desired label) looks through its properties,
        // getting the desired one (if any) and feeds to the index manipulator.
        LabelsReference labelsReference = new LabelsReference();
        final RecordStore.Processor<FAILURE> processor = new NodeIndexingProcessor<FAILURE>( propertyStore, propertyKeyPredicate,
                labelsReference, visitor );

        // Run the processor for the nodes containing the given label.
        // TODO When we've got a decent way of getting nodes with a label, use that instead.
        final Predicate<NodeRecord> predicate = new NodeLabelFilterPredicate( nodeStore, labelPredicate,
                labelsReference );

        // Run the processor
        return new ProcessStoreScan<FAILURE>( processor, predicate );
    }
    
    /**
     * Used for sharing the extracted labels from the last processed node between the label and property key filter.
     * First the label predicate will be run (which will set the labels).
     * Then the property key filtering in the processor will be run (which will get the labels).
     * 
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
    
    private interface PrimitiveLongPredicate
    {
        boolean accept( long value );
    }
    
    private static PrimitiveLongPredicate singleLongPredicate( final long acceptedValue )
    {
        return new PrimitiveLongPredicate()
        {
            @Override
            public boolean accept( long value )
            {
                return value == acceptedValue;
            }
        };
    }

    private static PrimitiveLongPredicate multipleLongPredicate( final long... acceptedValues )
    {
        return new PrimitiveLongPredicate()
        {
            @Override
            public boolean accept( long value )
            {
                for ( int i = 0; i < acceptedValues.length; i++ )
                    if ( value == acceptedValues[i] )
                        return true;
                return false;
            }
        };
    }
    
    private class NodeIndexingProcessor<FAILURE extends Exception> extends RecordStore.Processor<FAILURE>
    {
        private final PropertyStore propertyStore;
        private final Visitor<NodePropertyUpdate, FAILURE> visitor;
        private final PrimitiveLongPredicate propertyKeyPredicate;
        private final LabelsReference labelsReference;

        public NodeIndexingProcessor( PropertyStore propertyStore,
                PrimitiveLongPredicate propertyKeyPredicate, LabelsReference labelsReference,
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
                return;

            // TODO optimize so that we don't have to load all property records, but instead stop
            // when we first find the property we're looking for.
            for ( PropertyRecord propertyRecord : propertyStore.getPropertyRecordChain( firstPropertyId ) )
            {
                for ( PropertyBlock property : propertyRecord.getPropertyBlocks() )
                {
                    long propertyKeyId = property.getKeyIndexId();
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

    private Predicate<Pair<Integer, Object>> notNull()
    {
        return new Predicate<Pair<Integer, Object>>(){
            @Override
            public boolean accept( Pair<Integer, Object> item )
            {
                return item != null;
            }
        };
    }

    private Function<PropertyBlock, Pair<Integer, Object>> propertiesThatAreIn( final Set<Long> propertyKeys )
    {
        return new Function<PropertyBlock, Pair<Integer, Object>>()
        {

            @Override
            public Pair<Integer, Object> apply( PropertyBlock property )
            {
                int keyId = property.getKeyIndexId();
                if ( propertyKeys.contains( (long)keyId ) )
                {
                    propertyStore.ensureHeavy( property );
                    Object propertyValue = property.getType().getValue( property, propertyStore );
                    return Pair.of( property.getKeyIndexId(), propertyValue );
                }

                return null;
            }
        };
    }

    private class NodeLabelFilterPredicate implements Predicate<NodeRecord>
    {
        private final NodeStore nodeStore;
        private final PrimitiveLongPredicate labelPredicate;
        private final LabelsReference labelsReference;

        public NodeLabelFilterPredicate( NodeStore nodeStore, PrimitiveLongPredicate labelPredicate,
                LabelsReference labelsReference )
        {
            this.nodeStore = nodeStore;
            this.labelPredicate = labelPredicate;
            this.labelsReference = labelsReference;
        }

        @Override
        public boolean accept( NodeRecord node )
        {
            long[] labelsForNode = nodeStore.getLabelsForNode( node );
            labelsReference.set( labelsForNode ); // Make these available for the processor for this node
            for ( long nodeLabelId : labelsForNode )
                if ( labelPredicate.accept( nodeLabelId ) )
                    return true;
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

        @SuppressWarnings( "unchecked" )
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
