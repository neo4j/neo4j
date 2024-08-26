/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.constraints;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.LabelCoexistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.RelationshipEndpointConstraintDescriptor;
import org.neo4j.internal.schema.constraints.TypeConstraintDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

public interface ConstraintValidator {
    void assertKeyConstraintAllowed(SchemaDescriptor descriptor) throws CreateConstraintFailureException;

    void validateNodeKeyConstraint(
            NodeLabelIndexCursor allNodes,
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            LabelSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException;

    void validateRelKeyConstraint(
            RelationshipTypeIndexCursor allRelationships,
            RelationshipScanCursor relCursor,
            PropertyCursor propertyCursor,
            RelationTypeSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException;

    void validateNodePropertyExistenceConstraint(
            NodeLabelIndexCursor allNodes,
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            LabelSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            boolean isDependent)
            throws CreateConstraintFailureException;

    void validateRelationshipPropertyExistenceConstraint(
            RelationshipScanCursor relationshipCursor,
            PropertyCursor propertyCursor,
            RelationTypeSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            boolean isDependent)
            throws CreateConstraintFailureException;

    void validateRelationshipPropertyExistenceConstraint(
            RelationshipTypeIndexCursor allRelationships,
            PropertyCursor propertyCursor,
            RelationTypeSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            boolean isDependent)
            throws CreateConstraintFailureException;

    TxStateVisitor decorateTxStateVisitor(
            StorageReader storageReader,
            Read read,
            CursorFactory cursorFactory,
            ReadableTransactionState state,
            TxStateVisitor visitor,
            CursorContext cursorContext,
            MemoryTracker memoryTracker);

    void validateNodePropertyExistenceConstraint(
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            LabelSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            boolean isDependent)
            throws CreateConstraintFailureException;

    void validateNodeKeyConstraint(
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            LabelSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException;

    void validateRelKeyConstraint(
            RelationshipScanCursor relCursor,
            PropertyCursor propertyCursor,
            RelationTypeSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException;

    void validateNodePropertyTypeConstraint(
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            TypeConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException;

    void validateNodePropertyTypeConstraint(
            NodeLabelIndexCursor allNodes,
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            TypeConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException;

    void validateRelationshipPropertyTypeConstraint(
            RelationshipScanCursor relationshipCursor,
            PropertyCursor propertyCursor,
            TypeConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException;

    void validateRelationshipPropertyTypeConstraint(
            RelationshipTypeIndexCursor allRelationships,
            PropertyCursor propertyCursor,
            TypeConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException;

    /**
     * Verify that none of the relationships from `relCursor` violates the relationship endpoint constraint
     * defined by `descriptor`.
     *
     * @param relCursor initialized scan cursor with all relationships that are going to be checked.
     * @param nodeCursor auxiliary NodeCursor to perform the endpoint checks on each relationship.
     * @param descriptor the descriptor of the constraint being checked.
     * @param tokenNameLookup used to resolve token names for exception messages.
     * @throws CreateConstraintFailureException when one relationship violates the constraint.
     */
    void validateRelationshipEndpointConstraint(
            RelationshipScanCursor relCursor,
            NodeCursor nodeCursor,
            RelationshipEndpointConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException;

    /**
     * Verify that none of the nodes from `nodeCursor` violate the label coexistence constraint
     * defined by `descriptor`.
     *
     * @param allNodes indexed scan cursor with all nodes that are going to be checked.
     * @param nodeCursor cursor for what the indexed scan indicates in the storage.
     * @param descriptor the descriptor of the constraint being checked.
     * @param tokenNameLookup used to resolve token names for exception messages.
     * @throws CreateConstraintFailureException if any of the nodes violate the constraint.
     */
    void validateLabelCoexistenceConstraint(
            NodeLabelIndexCursor allNodes,
            NodeCursor nodeCursor,
            LabelCoexistenceConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException;

    /**
     * Verify that none of the nodes from `nodeCursor` violate the label coexistence constraint
     * defined by `descriptor`.
     *
     * @param nodeCursor initialized scan cursor with all nodes that are going to be checked.
     * @param descriptor the descriptor of the constraint being checked.
     * @param tokenNameLookup used to resolve token names for exception messages.
     * @throws CreateConstraintFailureException if any of the nodes violate the constraint.
     */
    void validateLabelCoexistenceConstraint(
            NodeCursor nodeCursor, LabelCoexistenceConstraintDescriptor descriptor, TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException;
}
