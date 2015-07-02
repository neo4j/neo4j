/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.cursor;

import org.neo4j.function.Consumer;
import org.neo4j.kernel.api.cursor.RelationshipCursor;
import org.neo4j.kernel.api.txstate.TransactionState;

/**
 * Overlays transaction state on a {@link RelationshipCursor}.
 */
public class TxSingleRelationshipCursor
        extends TxAbstractRelationshipCursor
{
    public TxSingleRelationshipCursor( TransactionState state, Consumer<TxSingleRelationshipCursor> instanceCache )
    {
        super( state, (Consumer) instanceCache );
    }

    public TxSingleRelationshipCursor init( RelationshipCursor cursor )
    {
        super.init( cursor );
        return this;
    }

    @Override
    public boolean next()
    {
        boolean exists = cursor.next();

        long id = cursor.getId();

        if ( state.relationshipIsDeletedInThisTx( id ) )
        {
            visit( -1, -1, -1, -1 );
            return false;
        }

        this.relationshipIsAddedInThisTx = state.relationshipIsAddedInThisTx( id );
        if ( exists || relationshipIsAddedInThisTx )
        {
            if ( relationshipIsAddedInThisTx )
            {
                state.relationshipVisit( id, this );
            }
            else
            {
                visit( id, cursor.getType(), cursor.getStartNode(), cursor.getEndNode() );
            }
            relationshipState = state.getRelationshipState( cursor.getId() );
            return true;
        }
        else
        {
            visit( -1, -1, -1, -1 );
            this.relationshipState = null;
            return false;
        }
    }
}
