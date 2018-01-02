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
package org.neo4j.kernel.api.txstate;

import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.util.diffsets.DiffSetsVisitor;

import static java.util.Objects.requireNonNull;

/**
 * A visitor implementation that makes visiting changed relationships of a transaction easier by facilitating accessing
 * the details of the visited relationships.
 *
 * By invoking a constructor that takes a {@linkplain ReadableTxState transaction state} parameter, the default
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
    private final DetailVisitor added, removed;

    /**
     * Causes {@link #visitAddedRelationship(long, int, long, long)} to be invoked for added relationships.
     */
    public RelationshipChangeVisitorAdapter( ReadableTxState txState )
    {
        this.added = added( requireNonNull( txState, "ReadableTxState" ) );
        this.removed = null;
    }

    protected void visitAddedRelationship( long relationshipId ) throws ConstraintValidationKernelException
    {
        if ( added != null )
        {
            added.visit( relationshipId );
        }
    }

    protected void visitRemovedRelationship( long relationshipId ) throws ConstraintValidationKernelException
    {
        if ( removed != null )
        {
            removed.visit( relationshipId );
        }
    }

    protected void visitAddedRelationship( long relationshipId, int type, long startNode, long endNode )
            throws ConstraintValidationKernelException
    {
    }

    protected void visitRemovedRelationship( long relationshipId, int type, long startNode, long endNode )
    {
    }

    @Override
    public final void visitAdded( Long relationshipId ) throws ConstraintValidationKernelException
    {
        visitAddedRelationship( relationshipId );
    }

    @Override
    public final void visitRemoved( Long relationshipId ) throws ConstraintValidationKernelException
    {
        visitRemovedRelationship( relationshipId );
    }

    private static abstract class DetailVisitor implements RelationshipVisitor<ConstraintValidationKernelException>
    {
        abstract void visit( long relationshipId ) throws ConstraintValidationKernelException;

        @Override
        public abstract void visit( long relId, int type, long startNode, long endNode )
                throws ConstraintValidationKernelException;
    }

    DetailVisitor added( final ReadableTxState txState )
    {
        return new DetailVisitor()
        {
            @Override
            void visit( long relationshipId ) throws ConstraintValidationKernelException
            {
                if ( !txState.relationshipVisit( relationshipId, this ) )
                {
                    throw new IllegalStateException( "No RelationshipState for added relationship!" );
                }
            }

            @Override
            public void visit( long relId, int type, long startNode, long endNode )
                    throws ConstraintValidationKernelException
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
            void visit( long relationshipId ) throws ConstraintValidationKernelException
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
