/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.neo4j.kernel.impl.api.index.EntityUpdates;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

public class RelationshipStoreScan<FAILURE extends Exception> extends PropertyAwareEntityStoreScan<RelationshipRecord,FAILURE>
{
    private final int[] relationshipTypeIds;
    private final Visitor<EntityUpdates,FAILURE> propertyUpdatesVisitor;

    public RelationshipStoreScan( RelationshipStore relationshipStore, LockService locks, PropertyStore propertyStore,
            Visitor<EntityUpdates,FAILURE> propertyUpdatesVisitor, int[] relationshipTypeIds, IntPredicate propertyKeyIdFilter )
    {
        super( relationshipStore, propertyStore, propertyKeyIdFilter, id -> locks.acquireRelationshipLock( id, LockService.LockType.READ_LOCK ) );
        this.relationshipTypeIds = relationshipTypeIds;
        this.propertyUpdatesVisitor = propertyUpdatesVisitor;
    }

    @Override
    protected void process( RelationshipRecord record ) throws FAILURE
    {
        int reltype = record.getType();

        if ( propertyUpdatesVisitor != null && containsAnyEntityToken( relationshipTypeIds, reltype ) )
        {
            // Notify the property update visitor
            EntityUpdates.Builder updates = EntityUpdates.forEntity( record.getId() ).withTokens( reltype );

            if ( hasRelevantProperty( record, updates ) )
            {
                propertyUpdatesVisitor.visit( updates.build() );
            }
        }
    }
}
