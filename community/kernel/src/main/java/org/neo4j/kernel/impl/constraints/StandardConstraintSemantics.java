/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.constraints;

import java.util.Iterator;

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.schema.NodePropertyDescriptor;
import org.neo4j.kernel.api.schema.RelationshipPropertyDescriptor;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintBoundary;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

import static org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptor.Type.UNIQUE;
import static org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptorFactory.uniqueForLabel;

public class StandardConstraintSemantics implements ConstraintSemantics
{
    public static final String ERROR_MESSAGE = "Property existence constraint requires Neo4j Enterprise Edition";

    @Override
    public void validateNodePropertyExistenceConstraint( Iterator<Cursor<NodeItem>> allNodes,
            NodePropertyDescriptor descriptor ) throws CreateConstraintFailureException
    {
        throw propertyExistenceConstraintsNotAllowed( new NodePropertyExistenceConstraint( descriptor ) );
    }

    @Override
    public void validateRelationshipPropertyExistenceConstraint( Cursor<RelationshipItem> allRels,
            RelationshipPropertyDescriptor descriptor ) throws CreateConstraintFailureException
    {
        throw propertyExistenceConstraintsNotAllowed( new RelationshipPropertyExistenceConstraint( descriptor ) );
    }

    @Override
    public PropertyConstraint readConstraint( ConstraintRule rule )
    {
        ConstraintDescriptor desc = rule.getConstraintDescriptor();
        if ( desc.type() == UNIQUE )
        {
            return ConstraintBoundary.map( desc );
        }
        return readNonStandardConstraint( rule );
    }

    protected PropertyConstraint readNonStandardConstraint( ConstraintRule rule )
    {
        // When opening a store in Community Edition that contains a Property Existence Constraint
        throw new IllegalStateException( ERROR_MESSAGE );
    }

    private CreateConstraintFailureException propertyExistenceConstraintsNotAllowed( PropertyConstraint constraint )
    {
        // When creating a Property Existence Constraint in Community Edition
        return new CreateConstraintFailureException( constraint, new IllegalStateException( ERROR_MESSAGE ) );
    }

    @Override
    public ConstraintRule writeUniquePropertyConstraint( long ruleId, NodePropertyDescriptor descriptor,
            long indexId )
    {
        return ConstraintRule.constraintRule(
                ruleId, uniqueForLabel( descriptor.getLabelId(), descriptor.getPropertyKeyId() ), indexId );
    }

    @Override
    public ConstraintRule writeNodePropertyExistenceConstraint( long ruleId, NodePropertyDescriptor descriptor )
            throws CreateConstraintFailureException
    {
        throw propertyExistenceConstraintsNotAllowed( new NodePropertyExistenceConstraint( descriptor ) );
    }

    @Override
    public ConstraintRule writeRelationshipPropertyExistenceConstraint( long ruleId,
            RelationshipPropertyDescriptor descriptor ) throws CreateConstraintFailureException
    {
        throw propertyExistenceConstraintsNotAllowed( new RelationshipPropertyExistenceConstraint( descriptor ) );
    }

    @Override
    public TxStateVisitor decorateTxStateVisitor( StoreReadLayer storeLayer, ReadableTransactionState txState,
            TxStateVisitor visitor )
    {
        return visitor;
    }
}
