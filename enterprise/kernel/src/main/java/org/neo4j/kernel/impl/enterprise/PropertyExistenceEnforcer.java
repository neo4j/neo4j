/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.enterprise;

import java.util.Iterator;
import java.util.Set;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.NodePropertyExistenceConstraintViolationKernelException;
import org.neo4j.kernel.api.exceptions.schema.RelationshipPropertyExistenceConstraintViolationKernelException;
import org.neo4j.storageengine.api.LabelItem;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

import static java.lang.String.format;

class PropertyExistenceEnforcer extends TxStateVisitor.Delegator
{
    private final StoreReadLayer storeLayer;
    private final ReadableTransactionState txState;
    private final PrimitiveIntSet labelIds = Primitive.intSet();
    private final PrimitiveIntSet propertyKeyIds = Primitive.intSet();
    private StorageStatement storageStatement;

    public PropertyExistenceEnforcer(
            TxStateVisitor next,
            ReadableTransactionState txState,
            StoreReadLayer storeLayer )
    {
        super( next );
        this.txState = txState;
        this.storeLayer = storeLayer;
    }

    @Override
    public void visitNodePropertyChanges( long id, Iterator<StorageProperty> added, Iterator<StorageProperty> changed,
            Iterator<Integer> removed ) throws ConstraintValidationKernelException
    {
        validateNode( id );
        super.visitNodePropertyChanges( id, added, changed, removed );
    }

    @Override
    public void visitNodeLabelChanges( long id, Set<Integer> added, Set<Integer> removed )
            throws ConstraintValidationKernelException
    {
        validateNode( id );
        super.visitNodeLabelChanges( id, added, removed );
    }

    @Override
    public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
            throws ConstraintValidationKernelException
    {
        validateRelationship( id );
        super.visitCreatedRelationship( id, type, startNode, endNode );
    }

    @Override
    public void visitRelPropertyChanges( long id, Iterator<StorageProperty> added, Iterator<StorageProperty> changed,
            Iterator<Integer> removed ) throws ConstraintValidationKernelException
    {
        validateRelationship( id );
        super.visitRelPropertyChanges( id, added, changed, removed );
    }

    private void validateNode( long nodeId ) throws ConstraintValidationKernelException
    {
        try ( Cursor<NodeItem> node = nodeCursor( nodeId ) )
        {
            if ( node.next() )
            {
                // Get all labels into a set for quick lookup
                labelIds.clear();
                try ( Cursor<LabelItem> labels = node.get().labels() )
                {
                    while ( labels.next() )
                    {
                        labelIds.add( labels.get().getAsInt() );
                    }
                }

                // Iterate all constraints and find property existence constraints that matches labels
                propertyKeyIds.clear();
                Iterator<PropertyConstraint> constraints = storeLayer.constraintsGetAll();
                while ( constraints.hasNext() )
                {
                    PropertyConstraint constraint = constraints.next();
                    if ( constraint instanceof NodePropertyExistenceConstraint && labelIds.contains(
                            ((NodePropertyExistenceConstraint) constraint).label() ) )
                    {
                        if ( propertyKeyIds.isEmpty() )
                        {
                            // Get all key ids into a set for quick lookup
                            try ( Cursor<PropertyItem> properties = node.get().properties() )
                            {
                                while ( properties.next() )
                                {
                                    propertyKeyIds.add( properties.get().propertyKeyId() );
                                }
                            }
                        }

                        // Check if this node has the mandatory property set
                        if ( !propertyKeyIds.contains( constraint.propertyKey() ) )
                        {
                            throw new NodePropertyExistenceConstraintViolationKernelException(
                                    ((NodePropertyExistenceConstraint) constraint).label(),
                                    constraint.propertyKey(), nodeId );
                        }
                    }
                }
            }
            else
            {
                throw new IllegalStateException( format( "Node %d with changes should exist.", nodeId ) );
            }
        }
    }

    private Cursor<NodeItem> nodeCursor( long id )
    {
        Cursor<NodeItem> cursor = storeStatement().acquireSingleNodeCursor( id );
        return txState.augmentSingleNodeCursor( cursor, id );
    }

    private StorageStatement storeStatement()
    {
        return storageStatement == null ? storageStatement = storeLayer.newStatement() : storageStatement;
    }

    @Override
    public void close()
    {
        super.close();
        if ( storageStatement != null )
        {
            storageStatement.close();
        }
    }

    private void validateRelationship( long id ) throws ConstraintValidationKernelException
    {
        try ( Cursor<RelationshipItem> relationship = relationshipCursor( id ) )
        {
            if ( relationship.next() )
            {
                // Iterate all constraints and find property existence constraints that matche relationship type
                propertyKeyIds.clear();
                Iterator<RelationshipPropertyConstraint> constraints = storeLayer.constraintsGetForRelationshipType(
                        relationship.get().type() );
                while ( constraints.hasNext() )
                {
                    RelationshipPropertyConstraint constraint = constraints.next();

                    if ( propertyKeyIds.isEmpty() )
                    {
                        // Get all key ids into a set for quick lookup
                        try ( Cursor<PropertyItem> properties = relationship.get().properties() )
                        {
                            while ( properties.next() )
                            {
                                propertyKeyIds.add( properties.get().propertyKeyId() );
                            }
                        }
                    }

                    // Check if this relationship has the mandatory property set
                    if ( !propertyKeyIds.contains( constraint.propertyKey() ) )
                    {
                        throw new RelationshipPropertyExistenceConstraintViolationKernelException(
                                constraint.relationshipType(),
                                constraint.propertyKey(), id );
                    }
                }

            }
            else
            {
                throw new IllegalStateException( format( "Relationship %d with changes should exist.", id ) );
            }
        }
    }

    private Cursor<RelationshipItem> relationshipCursor( long id )
    {
        Cursor<RelationshipItem> cursor = storeStatement().acquireSingleRelationshipCursor( id );
        return txState.augmentSingleRelationshipCursor( cursor, id );
    }
}
