/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.storageengine.api.txstate;

import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.set.primitive.LongSet;

import java.util.Iterator;
import java.util.function.Function;

import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.schema.IndexDescriptor;

/**
 * A visitor for visiting the changes that have been made in a transaction.
 */
public interface TxStateVisitor extends AutoCloseable
{
    void visitCreatedNode( long id );

    void visitDeletedNode( long id );

    void visitCreatedRelationship( long id, int type, long startNode, long endNode ) throws
            ConstraintValidationException;

    void visitDeletedRelationship( long id );

    void visitNodePropertyChanges( long id, Iterator<StorageProperty> added, Iterator<StorageProperty> changed,
            IntIterable removed ) throws ConstraintValidationException;

    void visitRelPropertyChanges( long id, Iterator<StorageProperty> added, Iterator<StorageProperty> changed,
            IntIterable removed ) throws ConstraintValidationException;

    void visitGraphPropertyChanges( Iterator<StorageProperty> added, Iterator<StorageProperty> changed,
            IntIterable removed );

    void visitNodeLabelChanges( long id, LongSet added, LongSet removed ) throws ConstraintValidationException;

    void visitAddedIndex( IndexDescriptor element );

    void visitRemovedIndex( IndexDescriptor element );

    void visitAddedConstraint( ConstraintDescriptor element ) throws CreateConstraintFailureException;

    void visitRemovedConstraint( ConstraintDescriptor element );

    void visitCreatedLabelToken( long id, String name );

    void visitCreatedPropertyKeyToken( long id, String name );

    void visitCreatedRelationshipTypeToken( long id, String name );

    @Override
    void close();

    class Adapter implements TxStateVisitor
    {
        @Override
        public void visitCreatedNode( long id )
        {
        }

        @Override
        public void visitDeletedNode( long id )
        {
        }

        @Override
        public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
        {
        }

        @Override
        public void visitDeletedRelationship( long id )
        {
        }

        @Override
        public void visitNodePropertyChanges( long id, Iterator<StorageProperty> added,
                Iterator<StorageProperty> changed, IntIterable removed )
        {
        }

        @Override
        public void visitRelPropertyChanges( long id, Iterator<StorageProperty> added,
                Iterator<StorageProperty> changed, IntIterable removed )
        {
        }

        @Override
        public void visitGraphPropertyChanges( Iterator<StorageProperty> added, Iterator<StorageProperty> changed,
                IntIterable removed )
        {
        }

        @Override
        public void visitNodeLabelChanges( long id, LongSet added, LongSet removed )
        {
        }

        @Override
        public void visitAddedIndex( IndexDescriptor index )
        {
        }

        @Override
        public void visitRemovedIndex( IndexDescriptor index )
        {
        }

        @Override
        public void visitAddedConstraint( ConstraintDescriptor element ) throws CreateConstraintFailureException
        {
        }

        @Override
        public void visitRemovedConstraint( ConstraintDescriptor element )
        {
        }

        @Override
        public void visitCreatedLabelToken( long id, String name )
        {
        }

        @Override
        public void visitCreatedPropertyKeyToken( long id, String name )
        {
        }

        @Override
        public void visitCreatedRelationshipTypeToken( long id, String name )
        {
        }

        @Override
        public void close()
        {
        }
    }

    TxStateVisitor EMPTY = new Adapter();

    class Delegator implements TxStateVisitor
    {
        private final TxStateVisitor actual;

        public Delegator( TxStateVisitor actual )
        {
            assert actual != null;
            this.actual = actual;
        }

        @Override
        public void visitCreatedNode( long id )
        {
            actual.visitCreatedNode( id );
        }

        @Override
        public void visitDeletedNode( long id )
        {
            actual.visitDeletedNode( id );
        }

        @Override
        public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
                throws ConstraintValidationException
        {
            actual.visitCreatedRelationship( id, type, startNode, endNode );
        }

        @Override
        public void visitDeletedRelationship( long id )
        {
            actual.visitDeletedRelationship( id );
        }

        @Override
        public void visitNodePropertyChanges( long id, Iterator<StorageProperty> added,
                Iterator<StorageProperty> changed, IntIterable removed ) throws ConstraintValidationException
        {
            actual.visitNodePropertyChanges( id, added, changed, removed );
        }

        @Override
        public void visitRelPropertyChanges( long id, Iterator<StorageProperty> added,
                Iterator<StorageProperty> changed, IntIterable removed )
                        throws ConstraintValidationException
        {
            actual.visitRelPropertyChanges( id, added, changed, removed );
        }

        @Override
        public void visitGraphPropertyChanges( Iterator<StorageProperty> added, Iterator<StorageProperty> changed,
                IntIterable removed )
        {
            actual.visitGraphPropertyChanges( added, changed, removed );
        }

        @Override
        public void visitNodeLabelChanges( long id, LongSet added, LongSet removed )
                throws ConstraintValidationException
        {
            actual.visitNodeLabelChanges( id, added, removed );
        }

        @Override
        public void visitAddedIndex( IndexDescriptor index )
        {
            actual.visitAddedIndex( index );
        }

        @Override
        public void visitRemovedIndex( IndexDescriptor index )
        {
            actual.visitRemovedIndex( index );
        }

        @Override
        public void visitAddedConstraint( ConstraintDescriptor constraint ) throws CreateConstraintFailureException
        {
            actual.visitAddedConstraint( constraint );
        }

        @Override
        public void visitRemovedConstraint( ConstraintDescriptor constraint )
        {
            actual.visitRemovedConstraint( constraint );
        }

        @Override
        public void visitCreatedLabelToken( long id, String name )
        {
            actual.visitCreatedLabelToken( id, name );
        }

        @Override
        public void visitCreatedPropertyKeyToken( long id, String name )
        {
            actual.visitCreatedPropertyKeyToken( id, name );
        }

        @Override
        public void visitCreatedRelationshipTypeToken( long id, String name )
        {
            actual.visitCreatedRelationshipTypeToken( id, name );
        }

        @Override
        public void close()
        {
            actual.close();
        }
    }

    /**
     * Interface for allowing decoration of a TxStateVisitor with one or more other visitor(s).
     */
    interface Decorator extends Function<TxStateVisitor,TxStateVisitor>
    {
    }

    Decorator NO_DECORATION = txStateVisitor -> txStateVisitor;
}
