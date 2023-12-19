/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.impl.enterprise;

import java.util.Iterator;
import java.util.function.BiPredicate;

import org.neo4j.cursor.Cursor;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.NodePropertyExistenceException;
import org.neo4j.kernel.api.exceptions.schema.RelationshipPropertyExistenceException;
import org.neo4j.kernel.api.schema.constaints.NodeKeyConstraintDescriptor;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

import static org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException.Phase.VERIFICATION;
import static org.neo4j.kernel.impl.enterprise.PropertyExistenceEnforcer.getOrCreatePropertyExistenceEnforcerFrom;

public class EnterpriseConstraintSemantics extends StandardConstraintSemantics
{
    @Override
    protected ConstraintDescriptor readNonStandardConstraint( ConstraintRule rule, String errorMessage )
    {
        if ( !rule.getConstraintDescriptor().enforcesPropertyExistence() )
        {
            throw new IllegalStateException( "Unsupported constraint type: " + rule );
        }
        return rule.getConstraintDescriptor();
    }

    @Override
    public ConstraintRule createNodeKeyConstraintRule(
            long ruleId, NodeKeyConstraintDescriptor descriptor, long indexId )
    {
        return ConstraintRule.constraintRule( ruleId, descriptor, indexId );
    }

    @Override
    public ConstraintRule createExistenceConstraint( long ruleId, ConstraintDescriptor descriptor )
    {
        return ConstraintRule.constraintRule( ruleId, descriptor );
    }

    @Override
    public void validateNodePropertyExistenceConstraint( Iterator<Cursor<NodeItem>> allNodes,
            LabelSchemaDescriptor descriptor, BiPredicate<NodeItem,Integer> hasPropertyCheck )
            throws CreateConstraintFailureException
    {
        while ( allNodes.hasNext() )
        {
            try ( Cursor<NodeItem> cursor = allNodes.next() )
            {
                NodeItem node = cursor.get();
                for ( int propertyKey : descriptor.getPropertyIds() )
                {
                    validateNodePropertyExistenceConstraint( node, propertyKey, descriptor, hasPropertyCheck );
                }
            }
        }
    }

    @Override
    public void validateNodePropertyExistenceConstraint( NodeLabelIndexCursor allNodes, NodeCursor nodeCursor,
            PropertyCursor propertyCursor, LabelSchemaDescriptor descriptor )
            throws CreateConstraintFailureException
    {
        while ( allNodes.next() )
        {
            allNodes.node( nodeCursor );
            while ( nodeCursor.next() )
            {
                for ( int propertyKey : descriptor.getPropertyIds() )
                {
                    nodeCursor.properties( propertyCursor );
                    if ( !hasProperty( propertyCursor, propertyKey ) )
                    {
                        throw createConstraintFailure(
                                new NodePropertyExistenceException( descriptor, VERIFICATION,
                                        nodeCursor.nodeReference() ) );
                    }
                }
            }
        }
    }

    @Override
    public void validateNodeKeyConstraint( Iterator<Cursor<NodeItem>> allNodes,
            LabelSchemaDescriptor descriptor, BiPredicate<NodeItem,Integer> hasPropertyCheck )
            throws CreateConstraintFailureException
    {
        validateNodePropertyExistenceConstraint( allNodes, descriptor, hasPropertyCheck );
    }

    @Override
    public void validateNodeKeyConstraint( NodeLabelIndexCursor allNodes, NodeCursor nodeCursor,
            PropertyCursor propertyCursor, LabelSchemaDescriptor descriptor ) throws CreateConstraintFailureException
    {
        validateNodePropertyExistenceConstraint( allNodes, nodeCursor, propertyCursor, descriptor );
    }

    private void validateNodePropertyExistenceConstraint( NodeItem node, int propertyKey,
            LabelSchemaDescriptor descriptor, BiPredicate<NodeItem,Integer> hasPropertyCheck ) throws
            CreateConstraintFailureException
    {
        if ( !hasPropertyCheck.test( node, propertyKey ) )
        {
            throw createConstraintFailure(
                    new NodePropertyExistenceException( descriptor, VERIFICATION, node.id() ) );
        }
    }

    private boolean hasProperty( PropertyCursor propertyCursor, int property )
    {
        while ( propertyCursor.next() )
        {
            if ( propertyCursor.propertyKey() == property )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public void validateRelationshipPropertyExistenceConstraint( Cursor<RelationshipItem> allRelationships,
            RelationTypeSchemaDescriptor descriptor, BiPredicate<RelationshipItem,Integer> hasPropertyCheck )
            throws CreateConstraintFailureException
    {
        while ( allRelationships.next() )
        {
            RelationshipItem relationship = allRelationships.get();
            for ( int propertyId : descriptor.getPropertyIds() )
            {
                if ( relationship.type() == descriptor.getRelTypeId() &&
                     !hasPropertyCheck.test( relationship, propertyId ) )
                {
                    throw createConstraintFailure(
                            new RelationshipPropertyExistenceException( descriptor, VERIFICATION, relationship.id() ) );
                }
            }
        }
    }

    @Override
    public void validateRelationshipPropertyExistenceConstraint( RelationshipScanCursor relationshipCursor,
            PropertyCursor propertyCursor, RelationTypeSchemaDescriptor descriptor )
            throws CreateConstraintFailureException
    {
        while ( relationshipCursor.next() )
        {
            relationshipCursor.properties( propertyCursor );

            for ( int propertyKey : descriptor.getPropertyIds() )
            {
                if ( relationshipCursor.type() == descriptor.getRelTypeId() &&
                     !hasProperty( propertyCursor, propertyKey ) )
                {
                    throw createConstraintFailure(
                            new RelationshipPropertyExistenceException( descriptor, VERIFICATION,
                                    relationshipCursor.relationshipReference() ) );
                }
            }
        }
    }

    private CreateConstraintFailureException createConstraintFailure( ConstraintValidationException it )
    {
        return new CreateConstraintFailureException( it.constraint(), it );
    }

    @Override
    public TxStateVisitor decorateTxStateVisitor( StoreReadLayer storeLayer, ReadableTransactionState txState,
            TxStateVisitor visitor )
    {
        if ( !txState.hasDataChanges() )
        {
            // If there are no data changes, there is no need to enforce constraints. Since there is no need to
            // enforce constraints, there is no need to build up the state required to be able to enforce constraints.
            // In fact, it might even be counter productive to build up that state, since if there are no data changes
            // there would be schema changes instead, and in that case we would throw away the schema-dependant state
            // we just built when the schema changing transaction commits.
            return visitor;
        }
        return getOrCreatePropertyExistenceEnforcerFrom( storeLayer )
                .decorate( visitor, txState, storeLayer );
    }
}
