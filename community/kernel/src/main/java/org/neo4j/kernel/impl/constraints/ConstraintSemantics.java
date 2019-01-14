/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.schema.constaints.NodeKeyConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
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
    @Deprecated
    void validateNodeKeyConstraint( Iterator<Cursor<NodeItem>> allNodes, LabelSchemaDescriptor descriptor,
            BiPredicate<NodeItem,Integer> hasProperty ) throws CreateConstraintFailureException;

    @Deprecated
    void validateNodePropertyExistenceConstraint( Iterator<Cursor<NodeItem>> allNodes, LabelSchemaDescriptor descriptor,
            BiPredicate<NodeItem,Integer> hasProperty ) throws CreateConstraintFailureException;

    @Deprecated
    void validateRelationshipPropertyExistenceConstraint( Cursor<RelationshipItem> allRelationships,
            RelationTypeSchemaDescriptor descriptor, BiPredicate<RelationshipItem,Integer> hasPropertyCheck )
            throws CreateConstraintFailureException;

    void validateNodeKeyConstraint( NodeLabelIndexCursor allNodes, NodeCursor nodeCursor,
            PropertyCursor propertyCursor, LabelSchemaDescriptor descriptor ) throws CreateConstraintFailureException;

    void validateNodePropertyExistenceConstraint( NodeLabelIndexCursor allNodes, NodeCursor nodeCursor,
            PropertyCursor propertyCursor, LabelSchemaDescriptor descriptor ) throws CreateConstraintFailureException;

    void validateRelationshipPropertyExistenceConstraint( RelationshipScanCursor relationshipCursor,
            PropertyCursor propertyCursor, RelationTypeSchemaDescriptor descriptor )
            throws CreateConstraintFailureException;

    ConstraintDescriptor readConstraint( ConstraintRule rule );

    ConstraintRule createUniquenessConstraintRule( long ruleId, UniquenessConstraintDescriptor descriptor,
            long indexId );

    ConstraintRule createNodeKeyConstraintRule( long ruleId, NodeKeyConstraintDescriptor descriptor, long indexId )
            throws CreateConstraintFailureException;

    ConstraintRule createExistenceConstraint( long ruleId, ConstraintDescriptor descriptor )
            throws CreateConstraintFailureException;

    TxStateVisitor decorateTxStateVisitor( StoreReadLayer storeLayer, ReadableTransactionState state,
            TxStateVisitor visitor );
}
