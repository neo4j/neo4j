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
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;

public class NeoStoreIndexStoreView implements IndexingService.IndexStoreView
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

//    @Override
//    public boolean nodeHasLabel( long nodeId, long label )
//    {
//        for ( long nodeLabelId : nodeStore.getLabelsForNode( nodeId ) )
//            if ( nodeLabelId == label )
//                return true;
//        return false;
//    }

    @Override
    public void visitNodesWithPropertyAndLabel( long labelId, long propertyKeyId, Visitor<Pair<Long, Object>> visitor )
    {
        // Create a processor that for each accepted node (containing the desired label) looks through its properties,
        // getting the desired one (if any) and feeds to the index manipulator.
        RecordStore.Processor processor = new NodeIndexingProcessor( propertyStore, propertyKeyId, visitor );

        // Run the processor for the nodes containing the given label.
        // TODO When we've got a decent way of getting nodes with a label, use that instead.
        final Predicate<NodeRecord> predicate = new NodeLabelFilterPredicate( nodeStore, labelId );

        // Run the processor
        processor.applyFiltered( nodeStore, predicate );
    }

    private class NodeIndexingProcessor extends RecordStore.Processor
    {
        private final PropertyStore propertyStore;
        private final long propertyKeyId;
        private final Visitor<Pair<Long, Object>> visitor;

        public NodeIndexingProcessor( PropertyStore propertyStore, long propertyKeyId, Visitor<Pair<Long, Object>>
                visitor )
        {
            this.propertyStore = propertyStore;
            this.propertyKeyId = propertyKeyId;
            this.visitor = visitor;
        }

        @Override
        public void processNode( RecordStore<NodeRecord> nodeStore, NodeRecord node )
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
                        // Make sure the value is loaded, even if it's of a "heavy" kind.
                        propertyStore.makeHeavy( property );
                        Object propertyValue = property.getType().getValue( property, propertyStore );

                        visitor.visit( Pair.of( node.getId(), propertyValue ) );
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
                    propertyStore.makeHeavy( property );
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
        private final long labelId;

        public NodeLabelFilterPredicate( NodeStore nodeStore, long labelId )
        {
            this.nodeStore = nodeStore;
            this.labelId = labelId;
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
