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
package org.neo4j.kernel.impl.constraints;

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.impl.store.record.PropertyConstraintRule;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

/**
 * Implements semantics of constraint creation and enforcement.
 */
public interface ConstraintSemantics
{
    void validateNodePropertyExistenceConstraint( Cursor<NodeItem> allNodes, int label, int propertyKey )
            throws CreateConstraintFailureException;

    void validateRelationshipPropertyExistenceConstraint( Cursor<RelationshipItem> allRels, int type, int propertyKey )
            throws CreateConstraintFailureException;

    PropertyConstraint readConstraint( PropertyConstraintRule rule );

    PropertyConstraintRule writeUniquePropertyConstraint( long ruleId, int label, int propertyKey, long indexId );

    PropertyConstraintRule writeNodePropertyExistenceConstraint( long ruleId, int label, int propertyKey )
            throws CreateConstraintFailureException;

    PropertyConstraintRule writeRelationshipPropertyExistenceConstraint( long ruleId, int type, int propertyKey )
            throws CreateConstraintFailureException;

    TxStateVisitor decorateTxStateVisitor( StoreReadLayer storeLayer, ReadableTransactionState state,
            TxStateVisitor visitor );
}
