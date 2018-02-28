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

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.impl.api.index.EntityUpdates;
import org.neo4j.kernel.impl.api.index.MultipleIndexPopulator;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreIdIterator;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.values.storable.Value;

import static java.util.Collections.emptyIterator;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

public class RelationshipStoreScan<FAILURE extends Exception> implements StoreScan<FAILURE>
{
    private final RelationshipStore relationshipStore;
    private final PropertyStore propertyStore;
    private final int[] relationshipTypeIds;
    private final IntPredicate propertyKeyIdFilter;
    private final Visitor<EntityUpdates,FAILURE> propertyUpdatesVisitor;
    private final LockService locks;
    private final RelationshipRecord record;
    private boolean continueScanning;
    private long count;
    private long totalCount;

    public RelationshipStoreScan( RelationshipStore relationshipStore, LockService locks, PropertyStore propertyStore,
            Visitor<EntityUpdates,FAILURE> propertyUpdatesVisitor, int[] relationshipTypeIds, IntPredicate propertyKeyIdFilter )
    {
        this.relationshipStore = relationshipStore;
        this.propertyStore = propertyStore;
        this.relationshipTypeIds = relationshipTypeIds;
        this.propertyKeyIdFilter = propertyKeyIdFilter;
        this.propertyUpdatesVisitor = propertyUpdatesVisitor;
        this.locks = locks;
        this.record = relationshipStore.newRecord();
        this.totalCount = relationshipStore.getHighId();
    }

    @Override
    public void run() throws FAILURE
    {
        try ( PrimitiveLongResourceIterator nodeIds = getRelationshipIdIterator() )
        {
            continueScanning = true;
            while ( continueScanning && nodeIds.hasNext() )
            {
                long id = nodeIds.next();
                try ( Lock ignored = locks.acquireRelationshipLock( id, LockService.LockType.READ_LOCK ) )
                {
                    count++;
                    if ( relationshipStore.getRecord( id, record, FORCE ).inUse() )
                    {
                        process( record );
                    }
                }
            }
        }
    }

    private void process( RelationshipRecord record ) throws FAILURE
    {
        int reltype = record.getType();

        if ( propertyUpdatesVisitor != null && Arrays.stream( relationshipTypeIds ).anyMatch( type -> type == reltype ) )
        {
            // Notify the property update visitor
            // TODO: reuse object instead? Better in terms of speed and GC?
            EntityUpdates.Builder updates = EntityUpdates.forEntity( record.getId(), new long[]{reltype} );
            boolean hasRelevantProperty = false;

            for ( PropertyBlock property : properties( record ) )
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

    private Value valueOf( PropertyBlock property )
    {
        // Make sure the value is loaded, even if it's of a "heavy" kind.
        propertyStore.ensureHeavy( property );
        return property.getType().value( property, propertyStore );
    }

    private Iterable<PropertyBlock> properties( final RelationshipRecord relationship )
    {
        return () -> new PropertyBlockIterator( relationship );
    }

    @Override
    public void stop()
    {
        continueScanning = false;
    }

    @Override
    public void acceptUpdate( MultipleIndexPopulator.MultipleIndexUpdater updater, IndexEntryUpdate<?> update, long currentlyIndexedNodeId )
    {
        if ( update.getEntityId() <= currentlyIndexedNodeId )
        {
            updater.process( update );
        }
    }

    @Override
    public PopulationProgress getProgress()
    {
        if ( totalCount > 0 )
        {
            return new PopulationProgress( count, totalCount );
        }

        // nothing to do 100% completed
        return PopulationProgress.DONE;
    }

    private PrimitiveLongResourceIterator getRelationshipIdIterator()
    {
        return PrimitiveLongCollections.resourceIterator( new StoreIdIterator( relationshipStore ), null );
    }

    private class PropertyBlockIterator extends PrefetchingIterator<PropertyBlock>
    {
        private final Iterator<PropertyRecord> records;
        private Iterator<PropertyBlock> blocks = emptyIterator();

        PropertyBlockIterator( RelationshipRecord relationship )
        {
            long firstPropertyId = relationship.getNextProp();
            if ( firstPropertyId == Record.NO_NEXT_PROPERTY.intValue() )
            {
                records = emptyIterator();
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
