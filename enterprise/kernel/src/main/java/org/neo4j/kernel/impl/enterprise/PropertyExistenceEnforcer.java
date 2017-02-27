/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.List;
import java.util.Set;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.api.exceptions.schema.NodePropertyExistenceException;
import org.neo4j.kernel.api.exceptions.schema.RelationshipPropertyExistenceException;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.RelationTypeSchemaDescriptor;
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
    private final List<LabelSchemaDescriptor> labelExistenceConstraints;
    private final List<RelationTypeSchemaDescriptor> relTypeExistenceConstraints;

    private final PrimitiveIntSet propertyKeyIds = Primitive.intSet();

    private StorageStatement storageStatement;

    public PropertyExistenceEnforcer( TxStateVisitor next, ReadableTransactionState txState, StoreReadLayer storeLayer,
            List<LabelSchemaDescriptor> labelExistenceConstraints,
            List<RelationTypeSchemaDescriptor> relTypeExistenceConstraints )
    {
        super( next );
        this.txState = txState;
        this.storeLayer = storeLayer;
        this.labelExistenceConstraints = labelExistenceConstraints;
        this.relTypeExistenceConstraints = relTypeExistenceConstraints;
    }

    @Override
    public void visitNodePropertyChanges( long id, Iterator<StorageProperty> added, Iterator<StorageProperty> changed,
            Iterator<Integer> removed ) throws ConstraintValidationException
    {
        validateNode( id );
        super.visitNodePropertyChanges( id, added, changed, removed );
    }

    @Override
    public void visitNodeLabelChanges( long id, Set<Integer> added, Set<Integer> removed )
            throws ConstraintValidationException
    {
        validateNode( id );
        super.visitNodeLabelChanges( id, added, removed );
    }

    @Override
    public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
            throws ConstraintValidationException
    {
        validateRelationship( id );
        super.visitCreatedRelationship( id, type, startNode, endNode );
    }

    @Override
    public void visitRelPropertyChanges( long id, Iterator<StorageProperty> added, Iterator<StorageProperty> changed,
            Iterator<Integer> removed ) throws ConstraintValidationException
    {
        validateRelationship( id );
        super.visitRelPropertyChanges( id, added, changed, removed );
    }

    private void validateNode( long nodeId ) throws NodePropertyExistenceException
    {
        if ( labelExistenceConstraints.isEmpty() )
        {
            return;
        }

        try ( Cursor<NodeItem> node = nodeCursor( nodeId ) )
        {
            if ( node.next() )
            {
                PrimitiveIntSet labelIds = node.get().labels();

                propertyKeyIds.clear();
                try ( Cursor<PropertyItem> properties = node.get().properties() )
                {
                    while ( properties.next() )
                    {
                        propertyKeyIds.add( properties.get().propertyKeyId() );
                    }
                }

                for ( LabelSchemaDescriptor descriptor : labelExistenceConstraints )
                {
                    if ( labelIds.contains( descriptor.getLabelId() ) )
                    {
                        for ( int propertyId : descriptor.getPropertyIds() )
                        {
                            validateNodeProperty( nodeId, propertyId, descriptor );
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

    private void validateNodeProperty( long nodeId, int propertyKey, LabelSchemaDescriptor descriptor )
            throws NodePropertyExistenceException
    {
        if ( !propertyKeyIds.contains( propertyKey ) )
        {
            throw new NodePropertyExistenceException( descriptor, ConstraintValidationException.Phase.VALIDATION, nodeId );
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

    private void validateRelationship( long id ) throws RelationshipPropertyExistenceException
    {
        if ( relTypeExistenceConstraints.isEmpty() )
        {
            return;
        }

        try ( Cursor<RelationshipItem> relationship = relationshipCursor( id ) )
        {
            if ( relationship.next() )
            {
                // Iterate all constraints and find property existence constraints that match relationship type
                propertyKeyIds.clear();
                try ( Cursor<PropertyItem> properties = relationship.get().properties() )
                {
                    while ( properties.next() )
                    {
                        propertyKeyIds.add( properties.get().propertyKeyId() );
                    }
                }

                for ( RelationTypeSchemaDescriptor descriptor : relTypeExistenceConstraints )
                {
                    if ( relationship.get().type() == descriptor.getRelTypeId() )
                    {
                        for ( int propertyId : descriptor.getPropertyIds() )
                        {
                            if ( !propertyKeyIds.contains( propertyId ) )
                            {
                                throw new RelationshipPropertyExistenceException( descriptor,
                                        ConstraintValidationException.Phase.VALIDATION, id );
                            }
                        }
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
