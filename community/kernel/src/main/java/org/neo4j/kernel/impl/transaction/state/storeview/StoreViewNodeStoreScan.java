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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.apache.commons.lang3.ArrayUtils;

import java.util.Iterator;
import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.NodePropertyUpdates;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.labelChanges;
import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;

public class StoreViewNodeStoreScan<FAILURE extends Exception> extends NodeStoreScan<FAILURE>
{
    private final PropertyStore propertyStore;

    private final Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor;
    private final Visitor<NodePropertyUpdates,FAILURE> propertyUpdatesVisitor;
    private final IntPredicate propertyKeyIdFilter;
    private NodePropertyUpdates updates;
    protected final int[] labelIds;

    public StoreViewNodeStoreScan( NodeStore nodeStore, LockService locks, PropertyStore propertyStore,
            Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor,
            Visitor<NodePropertyUpdates,FAILURE> propertyUpdatesVisitor,
            int[] labelIds, IntPredicate propertyKeyIdFilter )
    {
        super( nodeStore, locks, nodeStore.getHighId() );
        this.propertyStore = propertyStore;
        this.labelUpdateVisitor = labelUpdateVisitor;
        this.propertyUpdatesVisitor = propertyUpdatesVisitor;
        this.labelIds = labelIds;

        this.propertyKeyIdFilter = propertyKeyIdFilter;
        updates = new NodePropertyUpdates();
    }

    @Override
    protected PrimitiveLongResourceIterator getNodeIdIterator()
    {
        return super.getNodeIdIterator();
    }

    @Override
    public void process( NodeRecord node ) throws FAILURE
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

        if ( propertyUpdatesVisitor != null && containsAnyLabel( labelIds, labels ) )
        {

            updates.initForNodeId( node.getId() );
            // Notify the property update visitor
            for ( PropertyBlock property : properties( node ) )
            {
                int propertyKeyId = property.getKeyIndexId();
                if ( propertyKeyIdFilter.test( propertyKeyId ) )
                {
                    // This node has a property of interest to us
                    updates.add( propertyKeyId, valueOf( property ), labels );
                }
            }
            if ( updates.containsUpdates() )
            {
                propertyUpdatesVisitor.visit( updates );
                updates.reset();
            }
        }
    }

    private Iterable<PropertyBlock> properties( final NodeRecord node )
    {
        return () -> new PropertyBlockIterator( node );
    }

    private Object valueOf( PropertyBlock property )
    {
        // Make sure the value is loaded, even if it's of a "heavy" kind.
        propertyStore.ensureHeavy( property );
        return property.getType().getValue( property, propertyStore );
    }

    private static boolean containsAnyLabel( int[] labelIdFilter, long[] labels )
    {
        for ( long candidate : labels )
        {
            if ( ArrayUtils.contains( labelIdFilter, Math.toIntExact( candidate ) ) )
            {
                return true;
            }
        }
        return false;
    }

    private class PropertyBlockIterator extends PrefetchingIterator<PropertyBlock>
    {
        private final Iterator<PropertyRecord> records;
        private Iterator<PropertyBlock> blocks = Iterators.emptyIterator();

        PropertyBlockIterator( NodeRecord node )
        {
            long firstPropertyId = node.getNextProp();
            if ( firstPropertyId == Record.NO_NEXT_PROPERTY.intValue() )
            {
                records = Iterators.emptyIterator();
            }
            else
            {
                records = propertyStore.getPropertyRecordChain( firstPropertyId ).iterator();
            }
        }

        @Override
        protected PropertyBlock fetchNextOrNull()
        {
            for ( ; ; )
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
}
