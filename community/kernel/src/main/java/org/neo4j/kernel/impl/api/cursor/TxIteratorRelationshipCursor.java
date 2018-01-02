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
package org.neo4j.kernel.impl.api.cursor;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.function.Consumer;
import org.neo4j.kernel.api.cursor.RelationshipItem;
import org.neo4j.kernel.api.txstate.TransactionState;

/**
 * Overlays transaction state on a {@link RelationshipItem} cursor. This additionally knows how to traverse added
 * relationships in this transaction and skip deleted ones.
 */
public class TxIteratorRelationshipCursor extends TxAbstractRelationshipCursor
{
    private PrimitiveLongIterator added;
    private PrimitiveLongIterator addedRelationshipIterator;

    public TxIteratorRelationshipCursor( TransactionState state, Consumer<TxIteratorRelationshipCursor> instanceCache )
    {
        super( state, (Consumer) instanceCache );
    }

    public TxIteratorRelationshipCursor init( Cursor<RelationshipItem> cursor,
            PrimitiveLongIterator addedRelationshipIterator )
    {
        super.init( cursor );

        this.relationshipIsAddedInThisTx = false;
        this.addedRelationshipIterator = addedRelationshipIterator;

        added = null;

        return this;
    }

    @Override
    public boolean next()
    {
        if ( added == null )
        {
            while ( cursor.next() )
            {
                RelationshipItem relationshipItem = cursor.get();
                long id = relationshipItem.id();

                if ( state.relationshipIsDeletedInThisTx( id ) )
                {
                    continue;
                }

                visit( id, relationshipItem.type(), relationshipItem.startNode(), relationshipItem.endNode() );
                relationshipState = state.getRelationshipState( relationshipItem.id() );
                return true;
            }

            added = addedRelationshipIterator;
            relationshipIsAddedInThisTx = true;
        }

        if ( added.hasNext() )
        {
            relationshipState = state.getRelationshipState( added.next() );
            relationshipState.accept( this );
            return true;
        }
        else
        {
            visit( -1, -1, -1, -1 );
            relationshipState = null;
            return false;
        }
    }

    @Override
    public void close()
    {
        super.close();
        addedRelationshipIterator = null;
        added = null;
    }
}
