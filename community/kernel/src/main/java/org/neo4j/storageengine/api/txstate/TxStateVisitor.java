/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.storageengine.api.txstate;

import java.util.Iterator;
import java.util.Set;

import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.storageengine.api.StorageProperty;

/**
 * A visitor for visiting the changes that have been made in a transaction.
 */
public interface TxStateVisitor extends AutoCloseable
{
    void visitCreatedNode( long id );

    void visitDeletedNode( long id );

    void visitCreatedRelationship( long id, int type, long startNode, long endNode )
            throws ConstraintValidationKernelException;

    void visitDeletedRelationship( long id );

    void visitNodePropertyChanges( long id, Iterator<StorageProperty> added, Iterator<StorageProperty> changed,
                                   Iterator<Integer> removed ) throws ConstraintValidationKernelException;

    void visitRelPropertyChanges( long id, Iterator<StorageProperty> added, Iterator<StorageProperty> changed,
                                  Iterator<Integer> removed ) throws ConstraintValidationKernelException;

    void visitGraphPropertyChanges( Iterator<StorageProperty> added, Iterator<StorageProperty> changed,
                                    Iterator<Integer> removed );

    void visitNodeLabelChanges( long id, Set<Integer> added, Set<Integer> removed )
            throws ConstraintValidationKernelException;

    void visitAddedIndex( IndexDescriptor element, boolean isConstraintIndex );

    void visitRemovedIndex( IndexDescriptor element, boolean isConstraintIndex );

    void visitAddedUniquePropertyConstraint( UniquenessConstraint element );

    void visitRemovedUniquePropertyConstraint( UniquenessConstraint element );

    void visitAddedNodePropertyExistenceConstraint( NodePropertyExistenceConstraint element )
            throws CreateConstraintFailureException;

    void visitRemovedNodePropertyExistenceConstraint( NodePropertyExistenceConstraint element );

    void visitAddedRelationshipPropertyExistenceConstraint( RelationshipPropertyExistenceConstraint element )
            throws CreateConstraintFailureException;

    void visitRemovedRelationshipPropertyExistenceConstraint( RelationshipPropertyExistenceConstraint element );

    void visitCreatedLabelToken( String name, int id );

    void visitCreatedPropertyKeyToken( String name, int id );

    void visitCreatedRelationshipTypeToken( String name, int id );

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
                throws ConstraintValidationKernelException
        {
        }

        @Override
        public void visitDeletedRelationship( long id )
        {
        }

        @Override
        public void visitNodePropertyChanges( long id, Iterator<StorageProperty> added,
                Iterator<StorageProperty> changed, Iterator<Integer> removed )
                        throws ConstraintValidationKernelException
        {
        }

        @Override
        public void visitRelPropertyChanges( long id, Iterator<StorageProperty> added,
                Iterator<StorageProperty> changed, Iterator<Integer> removed )
                        throws ConstraintValidationKernelException
        {
        }

        @Override
        public void visitGraphPropertyChanges( Iterator<StorageProperty> added, Iterator<StorageProperty> changed,
                Iterator<Integer> removed )
        {
        }

        @Override
        public void visitNodeLabelChanges( long id, Set<Integer> added, Set<Integer> removed )
                throws ConstraintValidationKernelException
        {
        }

        @Override
        public void visitAddedIndex( IndexDescriptor element, boolean isConstraintIndex )
        {
        }

        @Override
        public void visitRemovedIndex( IndexDescriptor element, boolean isConstraintIndex )
        {
        }

        @Override
        public void visitAddedUniquePropertyConstraint( UniquenessConstraint element )
        {
        }

        @Override
        public void visitRemovedUniquePropertyConstraint( UniquenessConstraint element )
        {
        }

        @Override
        public void visitAddedNodePropertyExistenceConstraint( NodePropertyExistenceConstraint element )
                throws CreateConstraintFailureException
        {
        }

        @Override
        public void visitRemovedNodePropertyExistenceConstraint( NodePropertyExistenceConstraint element )
        {
        }

        @Override
        public void visitAddedRelationshipPropertyExistenceConstraint( RelationshipPropertyExistenceConstraint element )
                throws CreateConstraintFailureException
        {
        }

        @Override
        public void
                visitRemovedRelationshipPropertyExistenceConstraint( RelationshipPropertyExistenceConstraint element )
        {
        }

        @Override
        public void visitCreatedLabelToken( String name, int id )
        {
        }

        @Override
        public void visitCreatedPropertyKeyToken( String name, int id )
        {
        }

        @Override
        public void visitCreatedRelationshipTypeToken( String name, int id )
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
                throws ConstraintValidationKernelException
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
                Iterator<StorageProperty> changed, Iterator<Integer> removed )
                        throws ConstraintValidationKernelException
        {
            actual.visitNodePropertyChanges( id, added, changed, removed );
        }

        @Override
        public void visitRelPropertyChanges( long id, Iterator<StorageProperty> added,
                Iterator<StorageProperty> changed, Iterator<Integer> removed )
                        throws ConstraintValidationKernelException
        {
            actual.visitRelPropertyChanges( id, added, changed, removed );
        }

        @Override
        public void visitGraphPropertyChanges( Iterator<StorageProperty> added, Iterator<StorageProperty> changed,
                Iterator<Integer> removed )
        {
            actual.visitGraphPropertyChanges( added, changed, removed );
        }

        @Override
        public void visitNodeLabelChanges( long id, Set<Integer> added, Set<Integer> removed )
                throws ConstraintValidationKernelException
        {
            actual.visitNodeLabelChanges( id, added, removed );
        }

        @Override
        public void visitAddedIndex( IndexDescriptor element, boolean isConstraintIndex )
        {
            actual.visitAddedIndex( element, isConstraintIndex );
        }

        @Override
        public void visitRemovedIndex( IndexDescriptor element, boolean isConstraintIndex )
        {
            actual.visitRemovedIndex( element, isConstraintIndex );
        }

        @Override
        public void visitAddedUniquePropertyConstraint( UniquenessConstraint element )
        {
            actual.visitAddedUniquePropertyConstraint( element );
        }

        @Override
        public void visitRemovedUniquePropertyConstraint( UniquenessConstraint element )
        {
            actual.visitRemovedUniquePropertyConstraint( element );
        }

        @Override
        public void visitAddedNodePropertyExistenceConstraint( NodePropertyExistenceConstraint element )
                throws CreateConstraintFailureException
        {
            actual.visitAddedNodePropertyExistenceConstraint( element );
        }

        @Override
        public void visitRemovedNodePropertyExistenceConstraint( NodePropertyExistenceConstraint element )
        {
            actual.visitRemovedNodePropertyExistenceConstraint( element );
        }

        @Override
        public void visitAddedRelationshipPropertyExistenceConstraint( RelationshipPropertyExistenceConstraint element )
                throws CreateConstraintFailureException
        {
            actual.visitAddedRelationshipPropertyExistenceConstraint( element );
        }

        @Override
        public void
                visitRemovedRelationshipPropertyExistenceConstraint( RelationshipPropertyExistenceConstraint element )
        {
            actual.visitRemovedRelationshipPropertyExistenceConstraint( element );
        }

        @Override
        public void visitCreatedLabelToken( String name, int id )
        {
            actual.visitCreatedLabelToken( name, id );
        }

        @Override
        public void visitCreatedPropertyKeyToken( String name, int id )
        {
            actual.visitCreatedPropertyKeyToken( name, id );
        }

        @Override
        public void visitCreatedRelationshipTypeToken( String name, int id )
        {
            actual.visitCreatedRelationshipTypeToken( name, id );
        }

        @Override
        public void close()
        {
            actual.close();
        }
    }
}
