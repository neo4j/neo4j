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

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.NodePropertyExistenceConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.RelationshipPropertyExistenceConstraintVerificationFailedKernelException;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.store.record.NodePropertyExistenceConstraintRule;
import org.neo4j.kernel.impl.store.record.PropertyConstraintRule;
import org.neo4j.kernel.impl.store.record.RelationshipPropertyExistenceConstraintRule;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

import static org.neo4j.kernel.impl.store.record.NodePropertyExistenceConstraintRule.nodePropertyExistenceConstraintRule;
import static org.neo4j.kernel.impl.store.record.RelationshipPropertyExistenceConstraintRule.relPropertyExistenceConstraintRule;

public class EnterpriseConstraintSemantics extends StandardConstraintSemantics
{
    @Override
    protected PropertyConstraint readNonStandardConstraint( PropertyConstraintRule rule )
    {
        if ( rule instanceof NodePropertyExistenceConstraintRule )
        {
            return ((NodePropertyExistenceConstraintRule) rule).toConstraint();
        }
        if ( rule instanceof RelationshipPropertyExistenceConstraintRule )
        {
            return ((RelationshipPropertyExistenceConstraintRule) rule).toConstraint();
        }
        throw new IllegalStateException( "Unsupported constraint type: " + rule );
    }

    @Override
    public PropertyConstraintRule writeNodePropertyExistenceConstraint( long ruleId, int type, int propertyKey )
    {
        return nodePropertyExistenceConstraintRule( ruleId, type, propertyKey );
    }

    @Override
    public PropertyConstraintRule writeRelationshipPropertyExistenceConstraint( long ruleId, int type, int propertyKey )
    {
        return relPropertyExistenceConstraintRule( ruleId, type, propertyKey );
    }

    @Override
    public void validateNodePropertyExistenceConstraint( Cursor<NodeItem> allNodes, int label, int propertyKey )
            throws CreateConstraintFailureException
    {
        while ( allNodes.next() )
        {
            NodeItem node = allNodes.get();
            if ( !node.hasProperty( propertyKey ) )
            {
                throw createConstraintFailure(
                        new NodePropertyExistenceConstraintVerificationFailedKernelException(
                                new NodePropertyExistenceConstraint( label,propertyKey ), node.id() ) );
            }
        }
    }

    @Override
    public void validateRelationshipPropertyExistenceConstraint(
            Cursor<RelationshipItem> allRels, int type, int propertyKey )
            throws CreateConstraintFailureException
    {
        while ( allRels.next() )
        {
            RelationshipItem relationship = allRels.get();
            if ( relationship.type() == type && !relationship.hasProperty( propertyKey ) )
            {
                throw createConstraintFailure(
                        new RelationshipPropertyExistenceConstraintVerificationFailedKernelException(
                                new RelationshipPropertyExistenceConstraint( type, propertyKey ), relationship.id() ) );
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
