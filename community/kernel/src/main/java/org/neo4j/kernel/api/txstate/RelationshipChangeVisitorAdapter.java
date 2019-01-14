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
package org.neo4j.kernel.api.txstate;

import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.txstate.DiffSetsVisitor;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

import static java.util.Objects.requireNonNull;

/**
 * A visitor implementation that makes visiting changed relationships of a transaction easier by facilitating accessing
 * the details of the visited relationships.
 *
 * By invoking a constructor that takes a {@linkplain ReadableTransactionState transaction state} parameter, the default
 * implementation of {@link #visitAddedRelationship(long)} will retrieve the details of the visited relationship and
 * supply that information to the {@link #visitAddedRelationship(long, int, long, long)}-method. If no details can be
 * found, the transaction state is inconsistent with itself, and an exception will be thrown. If no such details are
 * required it is recommended to use one of the other constructors, and override {@link #visitAddedRelationship(long)}
 * if any action is to be taken for added relationships.
 *
 * By invoking a constructor that takes a {@linkplain StoreReadLayer store layer} parameter, the default
 * implementation of {@link #visitRemovedRelationship(long)} will retrieve the details of the visited relationship and
 * supply that information to the {@link #visitRemovedRelationship(long, int, long, long)}-method. If no details can be
 * found, the transaction state is inconsistent with the store, and an exception will be thrown. If no such details are
 * required it is recommended to use one of the other constructors, and override
 * {@link #visitRemovedRelationship(long)} if any action is to be taken for removed relationships.
 */
public abstract class RelationshipChangeVisitorAdapter implements DiffSetsVisitor<Long>
{
    private final DetailVisitor added;
    private final DetailVisitor removed;

    /**
     * Causes {@link #visitAddedRelationship(long, int, long, long)} to be invoked for added relationships.
     */
    public RelationshipChangeVisitorAdapter( ReadableTransactionState txState )
    {
        this.added = added( requireNonNull( txState, "ReadableTxState" ) );
        this.removed = null;
    }

    protected void visitAddedRelationship( long relationshipId ) throws ConstraintValidationException
    {
        if ( added != null )
        {
            added.visit( relationshipId );
        }
    }

    protected void visitRemovedRelationship( long relationshipId ) throws ConstraintValidationException
    {
        if ( removed != null )
        {
            removed.visit( relationshipId );
        }
    }

    protected void visitAddedRelationship( long relationshipId, int type, long startNode, long endNode )
            throws ConstraintValidationException
    {
    }

    protected void visitRemovedRelationship( long relationshipId, int type, long startNode, long endNode )
    {
    }

    @Override
    public final void visitAdded( Long relationshipId )
            throws ConstraintValidationException
    {
        visitAddedRelationship( relationshipId );
    }

    @Override
    public final void visitRemoved( Long relationshipId ) throws ConstraintValidationException
    {
        visitRemovedRelationship( relationshipId );
    }

    private abstract static class DetailVisitor implements RelationshipVisitor<ConstraintValidationException>
    {
        abstract void visit( long relationshipId ) throws ConstraintValidationException;

        @Override
        public abstract void visit( long relId, int type, long startNode, long endNode )
                throws ConstraintValidationException;
    }

    private DetailVisitor added( final ReadableTransactionState txState )
    {
        return new DetailVisitor()
        {
            @Override
            void visit( long relationshipId ) throws ConstraintValidationException
            {
                if ( !txState.relationshipVisit( relationshipId, this ) )
                {
                    throw new IllegalStateException( "No RelationshipState for added relationship!" );
                }
            }

            @Override
            public void visit( long relId, int type, long startNode, long endNode )
                    throws ConstraintValidationException
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
            void visit( long relationshipId ) throws ConstraintValidationException
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
