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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiPredicate;

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.NodePropertyExistenceException;
import org.neo4j.kernel.api.exceptions.schema.RelationshipPropertyExistenceException;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaProcessor;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptor;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

import static org.neo4j.kernel.api.exceptions.schema.ConstraintValidationException.Phase.VERIFICATION;
import static org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptor.Type.EXISTS;

public class EnterpriseConstraintSemantics extends StandardConstraintSemantics
{
    @Override
    protected ConstraintDescriptor readNonStandardConstraint( ConstraintRule rule )
    {
        if ( rule.getConstraintDescriptor().type() != EXISTS )
        {
            throw new IllegalStateException( "Unsupported constraint type: " + rule );
        }
        return rule.getConstraintDescriptor();
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

    private void validateNodePropertyExistenceConstraint( NodeItem node, int propertyKey,
        LabelSchemaDescriptor descriptor, BiPredicate<NodeItem, Integer> hasPropertyCheck ) throws
            CreateConstraintFailureException
    {
        if ( !hasPropertyCheck.test( node, propertyKey ) )
        {
            throw createConstraintFailure(
                new NodePropertyExistenceException( descriptor, VERIFICATION, node.id() ) );
        }
    }

    @Override
    public void validateExistenceConstraint( Cursor<RelationshipItem> allRels, RelationTypeSchemaDescriptor descriptor )
            throws CreateConstraintFailureException
    {
        while ( allRels.next() )
        {
            RelationshipItem relationship = allRels.get();
            if ( relationship.type() == descriptor.getRelTypeId() )
            {
                for ( int propertyId : descriptor.getPropertyIds() )
                {
                    if ( !relationship.hasProperty( propertyId ) )
                    {
                        throw createConstraintFailure(
                                new RelationshipPropertyExistenceException( descriptor, VERIFICATION, relationship.id() ) );
                    }
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
        ExistenceConstraintCollector collector = new ExistenceConstraintCollector();
        Iterator<ConstraintDescriptor> constraints = storeLayer.constraintsGetAll();
        while ( constraints.hasNext() )
        {
            collector.addIfRelevant( constraints.next() );
        }

        if ( collector.hasExistenceConstraints() )
        {
            return new PropertyExistenceEnforcer( visitor, txState, storeLayer, collector.labelExists, collector.relTypeExists );
        }
        return visitor;
    }

    private static class ExistenceConstraintCollector implements SchemaProcessor
    {
        final List<LabelSchemaDescriptor> labelExists = new ArrayList<>();
        final List<RelationTypeSchemaDescriptor> relTypeExists = new ArrayList<>();

        void addIfRelevant( ConstraintDescriptor constraint )
        {
            if ( constraint.type() == EXISTS )
            {
                constraint.schema().processWith( this );
            }
        }

        @Override
        public void processSpecific( LabelSchemaDescriptor schema )
        {
            labelExists.add( schema );
        }

        @Override
        public void processSpecific( RelationTypeSchemaDescriptor schema )
        {
            relTypeExists.add( schema );
        }

        boolean hasExistenceConstraints()
        {
            return labelExists.size() > 0 || relTypeExists.size() > 0;
        }
    }
}
