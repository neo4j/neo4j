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
package org.neo4j.kernel.impl.api.store;

import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.util.InstanceCache;

/**
 * Cursor for a single relationship.
 */
public class StoreSingleRelationshipCursor extends StoreAbstractRelationshipCursor
{
    private long relationshipId = -1;
    private final InstanceCache<StoreSingleRelationshipCursor> instanceCache;

    public StoreSingleRelationshipCursor( RelationshipRecord relationshipRecord, NeoStores neoStores,
            StoreStatement storeStatement, InstanceCache<StoreSingleRelationshipCursor> instanceCache,
            LockService lockService )
    {
        super( relationshipRecord, neoStores, storeStatement, lockService );
        this.instanceCache = instanceCache;
    }

    public StoreSingleRelationshipCursor init( long relId )
    {
        this.relationshipId = relId;
        return this;
    }

    @Override
    public boolean next()
    {
        if ( relationshipId != StatementConstants.NO_SUCH_RELATIONSHIP )
        {
            try
            {
                relationshipRecord.setId( relationshipId );
                return relationshipStore.fillRecord( relationshipId, relationshipRecord, RecordLoad.CHECK );
            }
            finally
            {
                relationshipId = StatementConstants.NO_SUCH_RELATIONSHIP;
            }
        }

        return false;
    }


    @Override
    public void close()
    {
        instanceCache.accept( this );
    }
}
