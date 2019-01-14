/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.api.cursor;

import java.util.function.Consumer;

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.storageengine.api.RelationshipItem;

/**
 * Overlays transaction state on a {@link RelationshipItem} item.
 */
public class TxSingleRelationshipCursor extends TxAbstractRelationshipCursor
{
    private long nextId = StatementConstants.NO_SUCH_RELATIONSHIP;

    public TxSingleRelationshipCursor( TransactionState state, Consumer<TxSingleRelationshipCursor> instanceCache )
    {
        super( state, (Consumer) instanceCache );
    }

    public TxSingleRelationshipCursor init( Cursor<RelationshipItem> cursor, long id )
    {
        this.nextId = id;
        super.init( cursor );
        return this;
    }

    @Override
    public boolean next()
    {
        if ( state.relationshipIsDeletedInThisTx( nextId ) )
        {
            visit( StatementConstants.NO_SUCH_RELATIONSHIP, StatementConstants.NO_SUCH_RELATIONSHIP_TYPE,
                    StatementConstants.NO_SUCH_NODE, StatementConstants.NO_SUCH_NODE );
            nextId = StatementConstants.NO_SUCH_RELATIONSHIP;
            return false;
        }

        boolean exists = cursor.next();

        this.relationshipIsAddedInThisTx = state.relationshipIsAddedInThisTx( nextId );
        if ( exists || relationshipIsAddedInThisTx )
        {
            if ( relationshipIsAddedInThisTx )
            {
                state.relationshipVisit( nextId, this );
            }
            else
            {
                RelationshipItem relationshipItem = cursor.get();
                visit( nextId, relationshipItem.type(), relationshipItem.startNode(), relationshipItem.endNode() );
            }
            relationshipState = state.getRelationshipState( nextId );
            return true;
        }
        else
        {
            visit( StatementConstants.NO_SUCH_RELATIONSHIP, StatementConstants.NO_SUCH_RELATIONSHIP_TYPE,
                    StatementConstants.NO_SUCH_NODE, StatementConstants.NO_SUCH_NODE );
            relationshipState = null;
            nextId = StatementConstants.NO_SUCH_RELATIONSHIP;
            return false;
        }
    }
}
