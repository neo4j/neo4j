/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.function.IntPredicate;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.EntityUpdates;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.values.storable.Value;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.labelChanges;
import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;

public class StoreViewNodeStoreScan<FAILURE extends Exception> extends PropertyAwareEntityStoreScan<NodeRecord,FAILURE>
{
    private final NodeStore nodeStore;

    private final Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor;
    private final Visitor<EntityUpdates,FAILURE> propertyUpdatesVisitor;
    private final IntPredicate propertyKeyIdFilter;
    protected final int[] labelIds;

    public StoreViewNodeStoreScan( NodeStore nodeStore, LockService locks, PropertyStore propertyStore,
            Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor,
            Visitor<EntityUpdates,FAILURE> propertyUpdatesVisitor,
            int[] labelIds, IntPredicate propertyKeyIdFilter )
    {
        super( nodeStore, locks, propertyStore );
        this.nodeStore = nodeStore;
        this.labelUpdateVisitor = labelUpdateVisitor;
        this.propertyUpdatesVisitor = propertyUpdatesVisitor;
        this.labelIds = labelIds;

        this.propertyKeyIdFilter = propertyKeyIdFilter;
    }

    @Override
    public void process( NodeRecord node ) throws FAILURE
    {
        long[] labels = parseLabelsField( node ).get( this.nodeStore );
        if ( labels.length == 0 && labelIds.length != 0 )
        {
            // This node has no labels at all
            return;
        }

        if ( labelUpdateVisitor != null )
        {
            // Notify the label update visitor
            labelUpdateVisitor.visit( labelChanges( node.getId(), EMPTY_LONG_ARRAY, labels ) );
        }

        if ( propertyUpdatesVisitor != null && containsAnyEntityToken( labelIds, labels ) )
        {
            // Notify the property update visitor
            // TODO: reuse object instead? Better in terms of speed and GC?
            EntityUpdates.Builder updates = EntityUpdates.forEntity( node.getId(), labels );
            boolean hasRelevantProperty = false;

            for ( PropertyBlock property : properties( node ) )
            {
                int propertyKeyId = property.getKeyIndexId();
                if ( propertyKeyIdFilter.test( propertyKeyId ) )
                {
                    // This node has a property of interest to us
                    Value value = valueOf( property );
                    Validators.INDEX_VALUE_VALIDATOR.validate( value );
                    updates.added( propertyKeyId, value );
                    hasRelevantProperty = true;
                }
            }

            if ( hasRelevantProperty )
            {
                propertyUpdatesVisitor.visit( updates.build() );
            }
        }
    }
}
