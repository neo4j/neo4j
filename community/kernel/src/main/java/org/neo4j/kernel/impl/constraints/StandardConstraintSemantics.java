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

import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.internal.schema.GraphTypeDependence.DEPENDENT;

import java.util.function.BooleanSupplier;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.KeyConstraintDescriptor;
import org.neo4j.internal.schema.constraints.NodeLabelExistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.RelationshipEndpointLabelConstraintDescriptor;
import org.neo4j.internal.schema.constraints.TypeConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.newapi.FilteringNodeCursorWrapper;
import org.neo4j.kernel.impl.newapi.FilteringRelationshipScanCursorWrapper;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StandardConstraintRuleAccessor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

@ServiceProvider
public class StandardConstraintSemantics extends ConstraintSemantics {
    public static final String ERROR_MESSAGE_EXISTS = "Property existence constraint requires Neo4j Enterprise Edition";
    public static final String ERROR_MESSAGE_KEY_SUFFIX = "Key constraint requires Neo4j Enterprise Edition";
    public static final String ERROR_MESSAGE_TYPE = "Property type constraint requires Neo4j Enterprise Edition";
    public static final String ERROR_MESSAGE_RELATIONSHIP_ENDPOINT_LABEL =
            "Relationship endpoint label constraint requires Neo4j Enterprise Edition";
    public static final String ERROR_MESSAGE_NODE_LABEL_EXISTENCE =
            "Node label existence constraint requires Neo4j Enterprise Edition";

    protected final StandardConstraintRuleAccessor accessor = new StandardConstraintRuleAccessor();

    public StandardConstraintSemantics() {
        this(Integer.MAX_VALUE);
    }

    protected StandardConstraintSemantics(int priority) {
        super(priority);
    }

    @Override
    public void assertKeyConstraintAllowed(SchemaDescriptor descriptor, TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw keyConstraintsNotAllowed(descriptor, tokenNameLookup);
    }

    @Override
    public void validateNodeKeyConstraint(
            NodeLabelIndexCursor allNodes,
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            LabelSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            MemoryTracker memoryTracker)
            throws CreateConstraintFailureException {
        throw keyConstraintsNotAllowed(descriptor, tokenNameLookup);
    }

    @Override
    public void validateRelKeyConstraint(
            RelationshipTypeIndexCursor allRelationships,
            RelationshipScanCursor relCursor,
            PropertyCursor propertyCursor,
            RelationTypeSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            MemoryTracker memoryTracker)
            throws CreateConstraintFailureException {
        throw keyConstraintsNotAllowed(descriptor, tokenNameLookup);
    }

    @Override
    public void validateNodePropertyExistenceConstraint(
            NodeLabelIndexCursor allNodes,
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            LabelSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            boolean isDependent,
            MemoryTracker memoryTracker,
            BooleanSupplier doContinue)
            throws CreateConstraintFailureException {
        throw propertyExistenceConstraintsNotAllowed(descriptor, tokenNameLookup, isDependent);
    }

    @Override
    public void validateRelationshipPropertyExistenceConstraint(
            FilteringRelationshipScanCursorWrapper relationshipCursor,
            PropertyCursor propertyCursor,
            RelationTypeSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            boolean isDependent,
            MemoryTracker memoryTracker)
            throws CreateConstraintFailureException {
        throw propertyExistenceConstraintsNotAllowed(descriptor, tokenNameLookup, isDependent);
    }

    @Override
    public void validateRelationshipPropertyExistenceConstraint(
            RelationshipTypeIndexCursor allRelationships,
            PropertyCursor propertyCursor,
            RelationTypeSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            boolean isDependent,
            MemoryTracker memoryTracker,
            BooleanSupplier doContinue)
            throws CreateConstraintFailureException {
        throw propertyExistenceConstraintsNotAllowed(descriptor, tokenNameLookup, isDependent);
    }

    @Override
    public ConstraintDescriptor readConstraint(ConstraintDescriptor constraint) {
        // Opening a store in Community Edition with Enterprise constraints should not work
        return switch (constraint.type()) {
            case UNIQUE -> constraint;
            case EXISTS -> throw new IllegalStateException(ERROR_MESSAGE_EXISTS);
            case UNIQUE_EXISTS -> throw new IllegalStateException(keyConstraintErrorMessage(constraint.schema()));
            case PROPERTY_TYPE -> throw new IllegalStateException(ERROR_MESSAGE_TYPE);
            case RELATIONSHIP_ENDPOINT_LABEL ->
                throw new IllegalStateException(ERROR_MESSAGE_RELATIONSHIP_ENDPOINT_LABEL);
            case NODE_LABEL_EXISTENCE -> throw new IllegalStateException(ERROR_MESSAGE_NODE_LABEL_EXISTENCE);
        };
    }

    private static CreateConstraintFailureException propertyExistenceConstraintsNotAllowed(
            SchemaDescriptor descriptor, TokenNameLookup tokenNameLookup, boolean isDependent) {
        // When creating a Property Existence Constraint in Community Edition
        return CreateConstraintFailureException.constraintCreationFailedOnCommunity(
                ConstraintDescriptorFactory.existsForSchema(descriptor, isDependent),
                tokenNameLookup,
                ERROR_MESSAGE_EXISTS);
    }

    private static CreateConstraintFailureException propertyTypeConstraintsNotAllowed(
            TypeConstraintDescriptor descriptor, TokenNameLookup tokenNameLookup) {
        // When creating a Property Type Constraint in Community Edition
        return CreateConstraintFailureException.constraintCreationFailedOnCommunity(
                descriptor, tokenNameLookup, ERROR_MESSAGE_TYPE);
    }

    private static CreateConstraintFailureException relationshipEndpointLabelConstraintsNotAllowed(
            RelationshipEndpointLabelConstraintDescriptor descriptor, TokenNameLookup tokenNameLookup) {
        return CreateConstraintFailureException.constraintCreationFailedOnCommunity(
                descriptor, tokenNameLookup, ERROR_MESSAGE_RELATIONSHIP_ENDPOINT_LABEL);
    }

    private static CreateConstraintFailureException nodeLabelExistenceConstraintsNotAllowed(
            NodeLabelExistenceConstraintDescriptor descriptor, TokenNameLookup tokenNameLookup) {
        return CreateConstraintFailureException.constraintCreationFailedOnCommunity(
                descriptor, tokenNameLookup, ERROR_MESSAGE_NODE_LABEL_EXISTENCE);
    }

    private static String keyConstraintErrorMessage(SchemaDescriptor descriptor) {
        return (descriptor.entityType() == NODE ? "Node " : "Relationship ") + ERROR_MESSAGE_KEY_SUFFIX;
    }

    private static CreateConstraintFailureException keyConstraintsNotAllowed(
            SchemaDescriptor descriptor, TokenNameLookup tokenNameLookup) {
        // When creating a Key Constraint in Community Edition
        return CreateConstraintFailureException.constraintCreationFailedOnCommunity(
                ConstraintDescriptorFactory.keyForSchema(descriptor),
                tokenNameLookup,
                keyConstraintErrorMessage(descriptor));
    }

    @Override
    public ConstraintDescriptor createUniquenessConstraintRule(
            UniquenessConstraintDescriptor descriptor, long indexId) {
        return accessor.createUniquenessConstraintRule(descriptor, indexId);
    }

    @Override
    public ConstraintDescriptor createKeyConstraintRule(
            KeyConstraintDescriptor descriptor, long indexId, TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw keyConstraintsNotAllowed(descriptor.schema(), tokenNameLookup);
    }

    @Override
    public ConstraintDescriptor createExistenceConstraint(
            ConstraintDescriptor descriptor, TokenNameLookup tokenNameLookup) throws CreateConstraintFailureException {
        throw propertyExistenceConstraintsNotAllowed(
                descriptor.schema(), tokenNameLookup, descriptor.graphTypeDependence() == DEPENDENT);
    }

    @Override
    public ConstraintDescriptor createPropertyTypeConstraint(
            TypeConstraintDescriptor descriptor, TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw propertyTypeConstraintsNotAllowed(descriptor, tokenNameLookup);
    }

    @Override
    public ConstraintDescriptor createRelationshipEndpointLabelConstraint(
            RelationshipEndpointLabelConstraintDescriptor descriptor, TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw relationshipEndpointLabelConstraintsNotAllowed(descriptor, tokenNameLookup);
    }

    @Override
    public ConstraintDescriptor createNodeLabelExistenceConstraint(
            NodeLabelExistenceConstraintDescriptor descriptor, TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw nodeLabelExistenceConstraintsNotAllowed(descriptor, tokenNameLookup);
    }

    @Override
    public TxStateVisitor decorateTxStateVisitor(
            StorageReader storageReader,
            Read read,
            CursorFactory cursorFactory,
            ReadableTransactionState state,
            TxStateVisitor visitor,
            CursorContext cursorContext,
            MemoryTracker memoryTracker) {
        return visitor;
    }

    @Override
    public void validateNodePropertyExistenceConstraint(
            FilteringNodeCursorWrapper nodeCursor,
            PropertyCursor propertyCursor,
            LabelSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            boolean isDependent,
            MemoryTracker memoryTracker)
            throws CreateConstraintFailureException {
        throw propertyExistenceConstraintsNotAllowed(descriptor, tokenNameLookup, isDependent);
    }

    @Override
    public void validateNodeKeyConstraint(
            FilteringNodeCursorWrapper nodeCursor,
            PropertyCursor propertyCursor,
            LabelSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            MemoryTracker memoryTracker)
            throws CreateConstraintFailureException {
        throw keyConstraintsNotAllowed(descriptor, tokenNameLookup);
    }

    @Override
    public void validateRelKeyConstraint(
            FilteringRelationshipScanCursorWrapper relCursor,
            PropertyCursor propertyCursor,
            RelationTypeSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            MemoryTracker memoryTracker)
            throws CreateConstraintFailureException {
        throw keyConstraintsNotAllowed(descriptor, tokenNameLookup);
    }

    @Override
    public void validateNodePropertyTypeConstraint(
            FilteringNodeCursorWrapper nodeCursor,
            PropertyCursor propertyCursor,
            TypeConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            MemoryTracker memoryTracker)
            throws CreateConstraintFailureException {
        throw propertyTypeConstraintsNotAllowed(descriptor, tokenNameLookup);
    }

    @Override
    public void validateNodePropertyTypeConstraint(
            NodeLabelIndexCursor allNodes,
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            TypeConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            MemoryTracker memoryTracker)
            throws CreateConstraintFailureException {
        throw propertyTypeConstraintsNotAllowed(descriptor, tokenNameLookup);
    }

    @Override
    public void validateRelationshipPropertyTypeConstraint(
            FilteringRelationshipScanCursorWrapper relationshipCursor,
            PropertyCursor propertyCursor,
            TypeConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            MemoryTracker memoryTracker)
            throws CreateConstraintFailureException {
        throw propertyTypeConstraintsNotAllowed(descriptor, tokenNameLookup);
    }

    @Override
    public void validateRelationshipPropertyTypeConstraint(
            RelationshipTypeIndexCursor allRelationships,
            PropertyCursor propertyCursor,
            TypeConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            MemoryTracker memoryTracker)
            throws CreateConstraintFailureException {
        throw propertyTypeConstraintsNotAllowed(descriptor, tokenNameLookup);
    }

    @Override
    public void validateRelationshipEndpointLabelConstraint(
            RelationshipScanCursor relCursor,
            NodeCursor nodeCursor,
            RelationshipEndpointLabelConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw relationshipEndpointLabelConstraintsNotAllowed(descriptor, tokenNameLookup);
    }

    @Override
    public void validateRelationshipEndpointLabelConstraint(
            RelationshipTypeIndexCursor relCursor,
            NodeCursor nodeCursor,
            RelationshipEndpointLabelConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw relationshipEndpointLabelConstraintsNotAllowed(descriptor, tokenNameLookup);
    }

    @Override
    public void validateNodeLabelExistenceConstraint(
            NodeLabelIndexCursor allNodes,
            NodeCursor nodeCursor,
            NodeLabelExistenceConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw nodeLabelExistenceConstraintsNotAllowed(descriptor, tokenNameLookup);
    }

    @Override
    public void validateNodeLabelExistenceConstraint(
            NodeCursor nodeCursor, NodeLabelExistenceConstraintDescriptor descriptor, TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw nodeLabelExistenceConstraintsNotAllowed(descriptor, tokenNameLookup);
    }
}
