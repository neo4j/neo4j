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

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;

public abstract class StoreAbstractIteratorRelationshipCursor extends StoreAbstractRelationshipCursor
{
    private ReadableTransactionState state;
    private PrimitiveLongIterator addedRelationshipIterator;
    private boolean fromStore;

    StoreAbstractIteratorRelationshipCursor( RelationshipRecord relationshipRecord, RecordCursors cursors,
            LockService lockService )
    {
        super( relationshipRecord, cursors, lockService );
    }

    void internalInitTxState( ReadableTransactionState state, PrimitiveLongIterator addedRelationshipIterator )
    {
        this.state = state;
        this.addedRelationshipIterator = addedRelationshipIterator;
        this.fromStore = true;
    }

    @Override
    protected final boolean fetchNext()
    {
        if ( fromStore )
        {
            while ( doFetchNext() )
            {
                if ( state != null && state.relationshipIsDeletedInThisTx( id() ) )
                {
                    continue;
                }

                return true;
            }

            fromStore = false;
        }

        if ( state != null && addedRelationshipIterator != null && addedRelationshipIterator.hasNext() )
        {
            state.getRelationshipState( addedRelationshipIterator.next() ).accept( this );
            return true;
        }

        visit( NO_SUCH_RELATIONSHIP, NO_SUCH_RELATIONSHIP_TYPE, NO_SUCH_NODE, NO_SUCH_NODE );
        return false;
    }

    protected abstract boolean doFetchNext();

    @Override
    public void close()
    {
        super.close();
        state = null;
        addedRelationshipIterator = null;
    }
}
