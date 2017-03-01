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
import java.util.function.BiPredicate;

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaDescriptor;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema_new.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

import static org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptor.Type.UNIQUE;

public class StandardConstraintSemantics implements ConstraintSemantics
{
    public static final String ERROR_MESSAGE = "Property existence constraint requires Neo4j Enterprise Edition";

    @Override
    public void validateNodePropertyExistenceConstraint( Iterator<Cursor<NodeItem>> allNodes,
            LabelSchemaDescriptor descriptor, BiPredicate<NodeItem,Integer> hasProperty )
            throws CreateConstraintFailureException
    {
        throw propertyExistenceConstraintsNotAllowed( descriptor );
    }

    @Override
    public void validateRelationshipPropertyExistenceConstraint( Cursor<RelationshipItem> allRelationships,
            RelationTypeSchemaDescriptor descriptor, BiPredicate<RelationshipItem,Integer> hasPropertyCheck )
            throws CreateConstraintFailureException
    {
        throw propertyExistenceConstraintsNotAllowed( descriptor );
    }

    @Override
    public ConstraintDescriptor readConstraint( ConstraintRule rule )
    {
        ConstraintDescriptor desc = rule.getConstraintDescriptor();
        if ( desc.type() == UNIQUE )
        {
            return desc;
        }
        return readNonStandardConstraint( rule );
    }

    protected ConstraintDescriptor readNonStandardConstraint( ConstraintRule rule )
    {
        // When opening a store in Community Edition that contains a Property Existence Constraint
        throw new IllegalStateException( ERROR_MESSAGE );
    }

    private CreateConstraintFailureException propertyExistenceConstraintsNotAllowed( SchemaDescriptor descriptor )
    {
        // When creating a Property Existence Constraint in Community Edition
        return new CreateConstraintFailureException(
                    ConstraintDescriptorFactory.existsForSchema( descriptor ),
                    new IllegalStateException( ERROR_MESSAGE ) );
    }

    @Override
    public ConstraintRule createUniquenessConstraintRule(
            long ruleId, UniquenessConstraintDescriptor descriptor, long indexId )
    {
        return ConstraintRule.constraintRule( ruleId, descriptor, indexId );
    }

    @Override
    public ConstraintRule createExistenceConstraint( long ruleId, ConstraintDescriptor descriptor )
            throws CreateConstraintFailureException
    {
        throw propertyExistenceConstraintsNotAllowed( descriptor.schema() );
    }

    @Override
    public TxStateVisitor decorateTxStateVisitor( StoreReadLayer storeLayer, ReadableTransactionState txState,
            TxStateVisitor visitor )
    {
        return visitor;
    }
}
