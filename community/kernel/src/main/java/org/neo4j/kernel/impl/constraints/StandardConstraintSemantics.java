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
import org.neo4j.internal.schema.constraints.LabelCoexistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.RelationshipEndpointConstraintDescriptor;
import org.neo4j.internal.schema.constraints.TypeConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
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
    public static final String ERROR_MESSAGE_ENDPOINT =
            "Relationship endpoint label constraint requires Neo4j Enterprise Edition";
    public static final String ERROR_MESSAGE_LABEL_COEXISTENCE =
            "Label coexistence constraint requires Neo4j Enterprise Edition";

    protected final StandardConstraintRuleAccessor accessor = new StandardConstraintRuleAccessor();

    public StandardConstraintSemantics() {
        this(1);
    }

    protected StandardConstraintSemantics(int priority) {
        super(priority);
    }

    @Override
    public String getName() {
        return "standardConstraints";
    }

    @Override
    public void assertKeyConstraintAllowed(SchemaDescriptor descriptor) throws CreateConstraintFailureException {
        throw keyConstraintsNotAllowed(descriptor);
    }

    @Override
    public void validateNodeKeyConstraint(
            NodeLabelIndexCursor allNodes,
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            LabelSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw keyConstraintsNotAllowed(descriptor);
    }

    @Override
    public void validateRelKeyConstraint(
            RelationshipTypeIndexCursor allRelationships,
            RelationshipScanCursor relCursor,
            PropertyCursor propertyCursor,
            RelationTypeSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw keyConstraintsNotAllowed(descriptor);
    }

    @Override
    public void validateNodePropertyExistenceConstraint(
            NodeLabelIndexCursor allNodes,
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            LabelSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            boolean isDependent)
            throws CreateConstraintFailureException {
        throw propertyExistenceConstraintsNotAllowed(descriptor, isDependent);
    }

    @Override
    public void validateRelationshipPropertyExistenceConstraint(
            RelationshipScanCursor relationshipCursor,
            PropertyCursor propertyCursor,
            RelationTypeSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            boolean isDependent)
            throws CreateConstraintFailureException {
        throw propertyExistenceConstraintsNotAllowed(descriptor, isDependent);
    }

    @Override
    public void validateRelationshipPropertyExistenceConstraint(
            RelationshipTypeIndexCursor allRelationships,
            PropertyCursor propertyCursor,
            RelationTypeSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            boolean isDependent)
            throws CreateConstraintFailureException {
        throw propertyExistenceConstraintsNotAllowed(descriptor, isDependent);
    }

    @Override
    public ConstraintDescriptor readConstraint(ConstraintDescriptor constraint) {
        return switch (constraint.type()) {
            case EXISTS -> readNonStandardConstraint(constraint, ERROR_MESSAGE_EXISTS);
            case UNIQUE_EXISTS -> readNonStandardConstraint(constraint, keyConstraintErrorMessage(constraint.schema()));
            default -> constraint;
        };
    }

    protected ConstraintDescriptor readNonStandardConstraint(ConstraintDescriptor constraint, String errorMessage) {
        // When opening a store in Community Edition that contains a Property Existence Constraint
        throw new IllegalStateException(errorMessage);
    }

    private static CreateConstraintFailureException propertyExistenceConstraintsNotAllowed(
            SchemaDescriptor descriptor, boolean isDependent) {
        // When creating a Property Existence Constraint in Community Edition
        return new CreateConstraintFailureException(
                ConstraintDescriptorFactory.existsForSchema(descriptor, isDependent), ERROR_MESSAGE_EXISTS);
    }

    private static CreateConstraintFailureException propertyTypeConstraintsNotAllowed(
            TypeConstraintDescriptor descriptor) {
        // When creating a Property Type Constraint in Community Edition
        return new CreateConstraintFailureException(descriptor, ERROR_MESSAGE_TYPE);
    }

    private static CreateConstraintFailureException relationshipEndpointLabelConstraintsNotAllowed(
            RelationshipEndpointConstraintDescriptor descriptor) {
        return new CreateConstraintFailureException(descriptor, ERROR_MESSAGE_ENDPOINT);
    }

    private static CreateConstraintFailureException labelCoexistenceConstraintsNotAllowed(
            LabelCoexistenceConstraintDescriptor descriptor) {
        return new CreateConstraintFailureException(descriptor, ERROR_MESSAGE_LABEL_COEXISTENCE);
    }

    private static String keyConstraintErrorMessage(SchemaDescriptor descriptor) {
        return (descriptor.entityType() == NODE ? "Node " : "Relationship ") + ERROR_MESSAGE_KEY_SUFFIX;
    }

    private static CreateConstraintFailureException keyConstraintsNotAllowed(SchemaDescriptor descriptor) {
        // When creating a Key Constraint in Community Edition
        return new CreateConstraintFailureException(
                ConstraintDescriptorFactory.keyForSchema(descriptor), keyConstraintErrorMessage(descriptor));
    }

    @Override
    public ConstraintDescriptor createUniquenessConstraintRule(
            long ruleId, UniquenessConstraintDescriptor descriptor, long indexId) {
        return accessor.createUniquenessConstraintRule(ruleId, descriptor, indexId);
    }

    @Override
    public ConstraintDescriptor createKeyConstraintRule(long ruleId, KeyConstraintDescriptor descriptor, long indexId)
            throws CreateConstraintFailureException {
        throw keyConstraintsNotAllowed(descriptor.schema());
    }

    @Override
    public ConstraintDescriptor createExistenceConstraint(long ruleId, ConstraintDescriptor descriptor)
            throws CreateConstraintFailureException {
        throw propertyExistenceConstraintsNotAllowed(
                descriptor.schema(), descriptor.graphTypeDependence() == DEPENDENT);
    }

    @Override
    public ConstraintDescriptor createPropertyTypeConstraint(long ruleId, TypeConstraintDescriptor descriptor)
            throws CreateConstraintFailureException {
        throw propertyTypeConstraintsNotAllowed(descriptor);
    }

    @Override
    public ConstraintDescriptor createRelationshipEndpointConstraint(
            long ruleId, RelationshipEndpointConstraintDescriptor descriptor) throws CreateConstraintFailureException {
        throw relationshipEndpointLabelConstraintsNotAllowed(descriptor);
    }

    @Override
    public ConstraintDescriptor createLabelCoexistenceConstraint(
            long ruleId, LabelCoexistenceConstraintDescriptor descriptor) throws CreateConstraintFailureException {
        throw labelCoexistenceConstraintsNotAllowed(descriptor);
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
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            LabelSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            boolean isDependent)
            throws CreateConstraintFailureException {
        throw propertyExistenceConstraintsNotAllowed(descriptor, isDependent);
    }

    @Override
    public void validateNodeKeyConstraint(
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            LabelSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw keyConstraintsNotAllowed(descriptor);
    }

    @Override
    public void validateRelKeyConstraint(
            RelationshipScanCursor relCursor,
            PropertyCursor propertyCursor,
            RelationTypeSchemaDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw keyConstraintsNotAllowed(descriptor);
    }

    @Override
    public void validateNodePropertyTypeConstraint(
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            TypeConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw propertyTypeConstraintsNotAllowed(descriptor);
    }

    @Override
    public void validateNodePropertyTypeConstraint(
            NodeLabelIndexCursor allNodes,
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            TypeConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw propertyTypeConstraintsNotAllowed(descriptor);
    }

    @Override
    public void validateRelationshipPropertyTypeConstraint(
            RelationshipScanCursor relationshipCursor,
            PropertyCursor propertyCursor,
            TypeConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw propertyTypeConstraintsNotAllowed(descriptor);
    }

    @Override
    public void validateRelationshipPropertyTypeConstraint(
            RelationshipTypeIndexCursor allRelationships,
            PropertyCursor propertyCursor,
            TypeConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw propertyTypeConstraintsNotAllowed(descriptor);
    }

    @Override
    public void validateRelationshipEndpointConstraint(
            RelationshipScanCursor relCursor,
            NodeCursor nodeCursor,
            RelationshipEndpointConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw relationshipEndpointLabelConstraintsNotAllowed(descriptor);
    }

    @Override
    public void validateLabelCoexistenceConstraint(
            NodeLabelIndexCursor allNodes,
            NodeCursor nodeCursor,
            LabelCoexistenceConstraintDescriptor descriptor,
            TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw labelCoexistenceConstraintsNotAllowed(descriptor);
    }

    @Override
    public void validateLabelCoexistenceConstraint(
            NodeCursor nodeCursor, LabelCoexistenceConstraintDescriptor descriptor, TokenNameLookup tokenNameLookup)
            throws CreateConstraintFailureException {
        throw labelCoexistenceConstraintsNotAllowed(descriptor);
    }
}
