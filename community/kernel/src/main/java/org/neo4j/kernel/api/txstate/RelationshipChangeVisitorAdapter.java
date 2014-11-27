/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.api.txstate;

import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.util.diffsets.DiffSetsVisitor;

import static java.util.Objects.requireNonNull;

public abstract class RelationshipChangeVisitorAdapter implements DiffSetsVisitor<Long>
{
    private final DetailVisitor added, removed;

    public RelationshipChangeVisitorAdapter( ReadableTxState txState )
    {
        this.added = added( requireNonNull( txState, "ReadableTxState" ) );
        this.removed = null;
    }

    public RelationshipChangeVisitorAdapter( StoreReadLayer store )
    {
        this.added = null;
        this.removed = removed( requireNonNull( store, "StoreReadLayer" ) );
    }

    public RelationshipChangeVisitorAdapter( StoreReadLayer store, ReadableTxState txState )
    {
        this.added = added( requireNonNull( txState, "ReadableTxState" ) );
        this.removed = removed( requireNonNull( store, "StoreReadLayer" ) );
    }

    protected void visitAddedRelationship( long relationshipId )
    {
        if ( added != null )
        {
            added.visit( relationshipId );
        }
    }

    protected void visitRemovedRelationship( long relationshipId )
    {
        if ( removed != null )
        {
            removed.visit( relationshipId );
        }
    }

    protected void visitAddedRelationship( long relationshipId, int type, long startNode, long endNode )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    protected void visitRemovedRelationship( long relationshipId, int type, long startNode, long endNode )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public final void visitAdded( Long relationshipId )
    {
        visitAddedRelationship( relationshipId );
    }

    @Override
    public final void visitRemoved( Long relationshipId )
    {
        visitRemovedRelationship( relationshipId );
    }

    private static abstract class DetailVisitor implements RelationshipVisitor<RuntimeException>
    {
        abstract void visit( long relationshipId );

        @Override
        public abstract void visit( long relId, int type, long startNode, long endNode );
    }

    DetailVisitor added( final ReadableTxState txState )
    {
        return new DetailVisitor()
        {
            @Override
            void visit( long relationshipId )
            {
                if ( !txState.relationshipVisit( relationshipId, this ) )
                {
                    throw new IllegalStateException( "No RelationshipState for added relationship!" );
                }
            }

            @Override
            public void visit( long relId, int type, long startNode, long endNode )
            {
                visitAddedRelationship( relId, type, startNode, endNode );
            }
        };
    }

    DetailVisitor removed( final StoreReadLayer store )
    {
        return new DetailVisitor()
        {
            @Override
            void visit( long relationshipId )
            {
                try
                {
                    store.relationshipVisit( relationshipId, this );
                }
                catch ( EntityNotFoundException e )
                {
                    throw new IllegalStateException( "No RelationshipState for removed relationship!", e );
                }
            }

            @Override
            public void visit( long relId, int type, long startNode, long endNode )
            {
                visitRemovedRelationship( relId, type, startNode, endNode );
            }
        };
    }
}
