/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.cursor.RelationshipItem;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.api.txstate.TxStateVisitor;
import org.neo4j.kernel.impl.api.StatementOperationParts;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.store.record.PropertyConstraintRule;
import org.neo4j.kernel.impl.store.record.UniquePropertyConstraintRule;

public class StandardConstraintSemantics implements ConstraintSemantics
{
    public static final String ERROR_MESSAGE = "Property existence constraint requires Neo4j Enterprise Edition";

    @Override
    public void validateNodePropertyExistenceConstraint( Cursor<NodeItem> allNodes, int label, int propertyKey )
            throws CreateConstraintFailureException
    {
        throw propertyExistenceConstraintsNotAllowed( new NodePropertyExistenceConstraint( label, propertyKey ) );
    }

    @Override
    public void validateRelationshipPropertyExistenceConstraint( Cursor<RelationshipItem> allRels, int type,
            int key ) throws CreateConstraintFailureException
    {
        throw propertyExistenceConstraintsNotAllowed( new RelationshipPropertyExistenceConstraint( type, key ) );
    }

    @Override
    public PropertyConstraint readConstraint( PropertyConstraintRule rule )
    {
        if ( rule instanceof UniquePropertyConstraintRule )
        {
            return ((UniquePropertyConstraintRule) rule).toConstraint();
        }
        return readNonStandardConstraint( rule );
    }

    protected PropertyConstraint readNonStandardConstraint( PropertyConstraintRule rule )
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
    public PropertyConstraintRule writeUniquePropertyConstraint( long ruleId, int label, int propertyKey, long indexId )
    {
        return UniquePropertyConstraintRule.uniquenessConstraintRule( ruleId, label, propertyKey, indexId );
    }

    @Override
    public PropertyConstraintRule writeNodePropertyExistenceConstraint( long ruleId, int label, int propertyKey )
            throws CreateConstraintFailureException
    {
        throw propertyExistenceConstraintsNotAllowed( new NodePropertyExistenceConstraint( label, propertyKey ) );
    }

    @Override
    public PropertyConstraintRule writeRelationshipPropertyExistenceConstraint( long ruleId, int type, int key )
            throws CreateConstraintFailureException
    {
        throw propertyExistenceConstraintsNotAllowed( new RelationshipPropertyExistenceConstraint( type, key ) );
    }

    @Override
    public TxStateVisitor decorateTxStateVisitor( StatementOperationParts operations, StoreStatement storeStatement,
            StoreReadLayer storeLayer, TxStateHolder holder, TxStateVisitor visitor )
    {
        return visitor;
    }
}
