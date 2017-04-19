/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.function.Consumer;

import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;

/**
 * Cursor for a single relationship.
 */
public class StoreSingleRelationshipCursor extends StoreAbstractRelationshipCursor
{
    private final Consumer<StoreSingleRelationshipCursor> consumer;
    private long relationshipId = NO_SUCH_RELATIONSHIP;
    private ReadableTransactionState state;

    StoreSingleRelationshipCursor( RelationshipStore relationshipStore,
            Consumer<StoreSingleRelationshipCursor> consumer, LockService lockService )
    {
        super( relationshipStore, lockService );
        this.consumer = consumer;
    }

    public StoreSingleRelationshipCursor init( long relId, ReadableTransactionState state )
    {
        this.relationshipId = relId;
        this.state = state;
        return this;
    }

    @Override
    protected boolean fetchNext()
    {
        if ( fetched || isDeletedInTx() || (!loadNextRecord() && !fetchFromTxState()) )
        {
            visit( NO_SUCH_RELATIONSHIP, NO_SUCH_RELATIONSHIP_TYPE, NO_SUCH_NODE, NO_SUCH_NODE );
            relationshipId = NO_SUCH_RELATIONSHIP;
            return false;
        }

        return true;
    }

    private boolean isDeletedInTx()
    {
        return state.relationshipIsDeletedInThisTx( relationshipId );
    }

    private boolean loadNextRecord()
    {
        return relationshipId != NO_SUCH_RELATIONSHIP &&
                relationshipStore.readRecord( relationshipId, relationshipRecord, CHECK, cursor ).inUse();
    }

    private boolean fetchFromTxState()
    {
        boolean found = state.relationshipIsAddedInThisTx( relationshipId );
        if ( found )
        {
            state.relationshipVisit( relationshipId, this );
        }
        return found;
    }

    @Override
    public void close()
    {
        super.close();
        state = null;
        relationshipId = NO_SUCH_RELATIONSHIP;
        consumer.accept( this );
    }
}
