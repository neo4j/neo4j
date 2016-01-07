/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.state;

import java.util.Collection;
import java.util.Iterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.StoreIdIterator;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.schema.IndexPopulationProgress;
import org.neo4j.register.Registers;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.kernel.api.CountsRead.ANY_LABEL;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.add;
import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.labelChanges;
import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;

public class NeoStoreIndexStoreView implements IndexStoreView
{
    private final PropertyStore propertyStore;
    private final NodeStore nodeStore;
    private final LockService locks;
    private final CountsTracker counts;

    public NeoStoreIndexStoreView( LockService locks, NeoStores neoStores )
    {
        this.locks = locks;
        this.propertyStore = neoStores.getPropertyStore();
        this.nodeStore = neoStores.getNodeStore();
        this.counts = neoStores.getCounts();
    }

    @Override
    public DoubleLongRegister indexUpdatesAndSize( IndexDescriptor descriptor, DoubleLongRegister output )
    {
        return counts.indexUpdatesAndSize( descriptor.getLabelId(), descriptor.getPropertyKeyId(), output );
    }

    @Override
    public void replaceIndexCounts( IndexDescriptor descriptor,
                                    long uniqueElements, long maxUniqueElements, long indexSize )
    {
        int labelId = descriptor.getLabelId();
        int propertyKeyId = descriptor.getPropertyKeyId();
        try ( CountsAccessor.IndexStatsUpdater updater = counts.updateIndexCounts() )
        {
            updater.replaceIndexSample( labelId, propertyKeyId, uniqueElements, maxUniqueElements );
            updater.replaceIndexUpdateAndSize( labelId, propertyKeyId, 0l, indexSize );
        }
    }

    @Override
    public void incrementIndexUpdates( IndexDescriptor descriptor, long updatesDelta )
    {
        try ( CountsAccessor.IndexStatsUpdater updater = counts.updateIndexCounts() )
        {
            updater.incrementIndexUpdates( descriptor.getLabelId(), descriptor.getPropertyKeyId(), updatesDelta );
        }
    }

    @Override
    public DoubleLongRegister indexSample( IndexDescriptor descriptor, DoubleLongRegister output )
    {
        return counts.indexSample( descriptor.getLabelId(), descriptor.getPropertyKeyId(), output );
    }

    @Override
    public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes(
            final int[] labelIds, final int[] propertyKeyIds,
            final Visitor<NodePropertyUpdate, FAILURE> propertyUpdateVisitor )
    {
        return visitNodes( labelIds, propertyKeyIds, propertyUpdateVisitor,
                // null here is fine
                null );
    }

    @Override
    public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes(
            final int[] labelIds, final int[] propertyKeyIds,
            final Visitor<NodePropertyUpdate, FAILURE> propertyUpdateVisitor,
            final Visitor<NodeLabelUpdate, FAILURE> labelUpdateVisitor )
    {
        final long allNodes =
                counts.nodeCount( ANY_LABEL, Registers.newDoubleLongRegister() ).readSecond();

        return new NodeStoreScan<FAILURE>( nodeStore, locks, allNodes )
        {
            @Override
            protected void process( NodeRecord node ) throws FAILURE
            {
                long[] labels = parseLabelsField( node ).get( this.nodeStore );
                if ( labels.length == 0 )
                {
                    // This node has no labels at all
                    return;
                }

                if ( labelUpdateVisitor != null )
                {
                    // Notify the label update visitor
                    labelUpdateVisitor.visit( labelChanges( node.getId(), EMPTY_LONG_ARRAY, labels ) );
                }

                if ( !containsAnyLabel( labelIds, labels ) )
                {
                    // This node has no labels of interest to us
                    return;

                }
                properties: for ( PropertyBlock property : properties( node ) )
                {
                    int propertyKeyId = property.getKeyIndexId();
                    for ( int sought : propertyKeyIds )
                    {
                        if ( propertyKeyId == sought )
                        {
                            // This node has a property of interest to us
                            NodePropertyUpdate update = add( node.getId(), propertyKeyId, valueOf( property ), labels );
                            propertyUpdateVisitor.visit( update );
                            continue properties;
                        }
                    }
                }
            }
        };
    }

    @Override
    public void nodeAsUpdates( long nodeId, NodeRecord record, Collection<NodePropertyUpdate> target )
    {
        NodeRecord node = nodeStore.loadRecord( nodeId, record );
        if ( node == null || !node.inUse() )
        {
            return;
        }
        long firstPropertyId = node.getNextProp();
        if ( firstPropertyId == Record.NO_NEXT_PROPERTY.intValue() )
        {
            return; // no properties => no updates (it's not going to be in any index)
        }
        long[] labels = parseLabelsField( node ).get( nodeStore );
        if ( labels.length == 0 )
        {
            return; // no labels => no updates (it's not going to be in any index)
        }
        for ( PropertyRecord propertyRecord : propertyStore.getPropertyRecordChain( firstPropertyId ) )
        {
            for ( PropertyBlock property : propertyRecord )
            {
                Object value = property.getType().getValue( property, propertyStore );
                target.add( add( node.getId(), property.getKeyIndexId(), value, labels ) );
            }
        }
    }

    @Override
    public Property getProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException, PropertyNotFoundException
    {
        NodeRecord node = nodeStore.forceGetRecord( nodeId );
        if ( !node.inUse() )
        {
            throw new EntityNotFoundException( EntityType.NODE, nodeId );
        }
        long firstPropertyId = node.getNextProp();
        if ( firstPropertyId == Record.NO_NEXT_PROPERTY.intValue() )
        {
            throw new PropertyNotFoundException( propertyKeyId, EntityType.NODE, nodeId );
        }
        for ( PropertyRecord propertyRecord : propertyStore.getPropertyRecordChain( firstPropertyId ) )
        {
            PropertyBlock propertyBlock = propertyRecord.getPropertyBlock( propertyKeyId );
            if ( propertyBlock != null )
            {
                return propertyBlock.newPropertyData( propertyStore );
            }
        }
        throw new PropertyNotFoundException( propertyKeyId, EntityType.NODE, nodeId );
    }

    private Object valueOf( PropertyBlock property )
    {
        // Make sure the value is loaded, even if it's of a "heavy" kind.
        propertyStore.ensureHeavy( property );
        return property.getType().getValue( property, propertyStore );
    }

    private Iterable<PropertyBlock> properties( final NodeRecord node )
    {
        return new Iterable<PropertyBlock>()
        {
            @Override
            public Iterator<PropertyBlock> iterator()
            {
                return new PropertyBlockIterator( node );
            }
        };
    }

    private static boolean containsLabel( int sought, long[] labels )
    {
        for ( long label : labels )
        {
            if ( label == sought )
            {
                return true;
            }
        }
        return false;
    }

    private static boolean containsAnyLabel( int[] soughtIds, long[] labels )
    {
        for ( int soughtId : soughtIds )
        {
            if ( containsLabel( soughtId, labels ) )
            {
                return true;
            }
        }
        return false;
    }

    private class PropertyBlockIterator extends PrefetchingIterator<PropertyBlock>
    {
        private final Iterator<PropertyRecord> records;
        private Iterator<PropertyBlock> blocks = IteratorUtil.emptyIterator();

        PropertyBlockIterator( NodeRecord node )
        {
            long firstPropertyId = node.getNextProp();
            if ( firstPropertyId == Record.NO_NEXT_PROPERTY.intValue() )
            {
                records = IteratorUtil.emptyIterator();
            }
            else
            {
                records = propertyStore.getPropertyRecordChain( firstPropertyId ).iterator();
            }
        }

        @Override
        protected PropertyBlock fetchNextOrNull()
        {
            for (; ; )
            {
                if ( blocks.hasNext() )
                {
                    return blocks.next();
                }
                if ( !records.hasNext() )
                {
                    return null;
                }
                blocks = records.next().iterator();
            }
        }
    }

    abstract static class NodeStoreScan<FAILURE extends Exception> implements StoreScan<FAILURE>
    {
        private volatile boolean continueScanning;

        protected final NodeStore nodeStore;
        protected final LockService locks;
        private final long totalCount;

        private long count = 0;

        protected abstract void process( NodeRecord loaded ) throws FAILURE;

        public NodeStoreScan( NodeStore nodeStore, LockService locks, long totalCount )
        {
            this.nodeStore = nodeStore;
            this.locks = locks;
            this.totalCount = totalCount;
        }

        @Override
        public void run() throws FAILURE
        {
            PrimitiveLongIterator nodeIds = new StoreIdIterator( nodeStore );
            continueScanning = true;
            NodeRecord record = new NodeRecord( -1 );
            while ( continueScanning && nodeIds.hasNext() )
            {
                long id = nodeIds.next();
                try ( Lock ignored = locks.acquireNodeLock( id, LockService.LockType.READ_LOCK ) )
                {
                    NodeRecord loaded = nodeStore.loadRecord( id, record );
                    if ( loaded != null )
                    {
                        process( loaded );
                    }
                    count++;
                }
            }
        }

        @Override
        public void stop()
        {
            continueScanning = false;
        }

        @Override
        public IndexPopulationProgress getProgress()
        {
            if ( totalCount > 0 )
            {
                return new IndexPopulationProgress( count, totalCount );
            }

            // nothing to do 100% completed
            return IndexPopulationProgress.DONE;
        }
    }
}
