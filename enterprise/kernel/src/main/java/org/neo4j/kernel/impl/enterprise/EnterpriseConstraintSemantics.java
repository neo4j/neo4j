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

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.schema.NodePropertyDescriptor;
import org.neo4j.kernel.api.schema.RelationshipPropertyDescriptor;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.NodePropertyExistenceConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.RelationshipPropertyExistenceConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintBoundary;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

import static org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptor.Type.EXISTS;
import static org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptorFactory.existsForLabel;
import static org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptorFactory.existsForRelType;

public class EnterpriseConstraintSemantics extends StandardConstraintSemantics
{
    @Override
    protected PropertyConstraint readNonStandardConstraint( ConstraintRule rule )
    {
        if ( rule.getConstraintDescriptor().type() != EXISTS )
        {
            throw new IllegalStateException( "Unsupported constraint type: " + rule );
        }
        return ConstraintBoundary.map( rule.getConstraintDescriptor() );
    }

    @Override
    public ConstraintRule writeNodePropertyExistenceConstraint( long ruleId,
            NodePropertyDescriptor descriptor )
    {
        return ConstraintRule.constraintRule( ruleId,
                existsForLabel( descriptor.getLabelId(), descriptor.getPropertyKeyId() ) );
    }

    @Override
    public ConstraintRule writeRelationshipPropertyExistenceConstraint( long ruleId,
            RelationshipPropertyDescriptor descriptor )
    {
        return ConstraintRule.constraintRule( ruleId,
                existsForRelType( descriptor.getRelationshipTypeId(), descriptor.getPropertyKeyId() ) );
    }

    @Override
    public void validateNodePropertyExistenceConstraint( Iterator<Cursor<NodeItem>> allNodes,
            NodePropertyDescriptor descriptor ) throws CreateConstraintFailureException
    {
        while ( allNodes.hasNext() )
        {
            try ( Cursor<NodeItem> cursor = allNodes.next() )
            {
                NodeItem node = cursor.get();
                if ( descriptor.isComposite() )
                {
                    for ( int propertyKey : descriptor.getPropertyKeyIds() )
                    {
                        validateNodePropertyExistenceConstraint( node, propertyKey, descriptor );
                    }
                }
                else
                {
                    validateNodePropertyExistenceConstraint( node, descriptor.getPropertyKeyId(), descriptor );
                }
            }
        }
    }

    private void validateNodePropertyExistenceConstraint( NodeItem node, int propertyKey,
            NodePropertyDescriptor descriptor ) throws CreateConstraintFailureException
    {
        if ( !node.hasProperty( propertyKey ) )
        {
            throw createConstraintFailure(
                new NodePropertyExistenceConstraintVerificationFailedKernelException(
                    new NodePropertyExistenceConstraint( descriptor ), node.id()
                ) );
        }
    }

    @Override
    public void validateRelationshipPropertyExistenceConstraint(
            Cursor<RelationshipItem> allRels, RelationshipPropertyDescriptor descriptor )
            throws CreateConstraintFailureException
    {
        while ( allRels.next() )
        {
            RelationshipItem relationship = allRels.get();
            if ( relationship.type() == descriptor.getRelationshipTypeId() &&
                 !relationship.hasProperty( descriptor.getPropertyKeyId() ) )
            {
                throw createConstraintFailure(
                        new RelationshipPropertyExistenceConstraintVerificationFailedKernelException(
                                new RelationshipPropertyExistenceConstraint( descriptor ), relationship.id() ) );
            }

        }
    }

    private CreateConstraintFailureException createConstraintFailure( ConstraintVerificationFailedKernelException it )
    {
        return new CreateConstraintFailureException( it.constraint(), it );
    }

    @Override
    public TxStateVisitor decorateTxStateVisitor( StoreReadLayer storeLayer, ReadableTransactionState txState,
            TxStateVisitor visitor )
    {
        Iterator<PropertyConstraint> constraints = storeLayer.constraintsGetAll();
        while ( constraints.hasNext() )
        {
            PropertyConstraint constraint = constraints.next();
            // TODO checking instanceof isn't nice, we should instead introduce an internal type in addition
            // to the public ConstraintType which we cannot have on this level.
            if ( constraint instanceof NodePropertyExistenceConstraint ||
                 constraint instanceof RelationshipPropertyExistenceConstraint )
            {
                return new PropertyExistenceEnforcer( visitor, txState, storeLayer );
            }
        }
        return visitor;
    }
}
