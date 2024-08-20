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
package org.neo4j.internal.schema.constraints;

import org.neo4j.internal.schema.EndpointType;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.LabelCoexistenceSchemaDescriptor;
import org.neo4j.internal.schema.RelationshipEndpointSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.util.VisibleForTesting;

public class ConstraintDescriptorFactory {
    private ConstraintDescriptorFactory() {}

    public static ExistenceConstraintDescriptor existsForSchema(SchemaDescriptor schema, boolean isDependent) {
        return ConstraintDescriptorImplementation.makeExistsConstraint(schema, isDependent);
    }

    public static UniquenessConstraintDescriptor uniqueForSchema(SchemaDescriptor schema) {
        return ConstraintDescriptorImplementation.makeUniqueConstraint(schema, IndexType.RANGE);
    }

    public static UniquenessConstraintDescriptor uniqueForSchema(SchemaDescriptor schema, IndexType indexType) {
        return ConstraintDescriptorImplementation.makeUniqueConstraint(schema, indexType);
    }

    public static KeyConstraintDescriptor keyForSchema(SchemaDescriptor schema) {
        return ConstraintDescriptorImplementation.makeUniqueExistsConstraint(schema, IndexType.RANGE);
    }

    public static KeyConstraintDescriptor keyForSchema(SchemaDescriptor schema, IndexType indexType) {
        return ConstraintDescriptorImplementation.makeUniqueExistsConstraint(schema, indexType);
    }

    public static TypeConstraintDescriptor typeForSchema(
            SchemaDescriptor schema, PropertyTypeSet allowedTypes, boolean isDependent) {
        return ConstraintDescriptorImplementation.makePropertyTypeConstraint(schema, allowedTypes, isDependent);
    }

    public static RelationshipEndpointConstraintDescriptor relationshipEndpointForSchema(
            RelationshipEndpointSchemaDescriptor schema, int endpointLabelId, EndpointType endpointType) {
        return RelationshipEndpointConstraintDescriptorImplementation.make(schema, endpointLabelId, endpointType);
    }

    public static LabelCoexistenceConstraintDescriptor labelCoexistenceForSchema(
            LabelCoexistenceSchemaDescriptor schema, int requiredLabelId) {
        return LabelCoexistenceConstraintDescriptorImplementation.make(schema, requiredLabelId);
    }

    public static ExistenceConstraintDescriptor existsForLabel(boolean isDependent, int labelId, int... propertyIds) {
        return existsForSchema(SchemaDescriptors.forLabel(labelId, propertyIds), isDependent);
    }

    public static ExistenceConstraintDescriptor existsForRelType(
            boolean isDependent, int relTypeId, int... propertyIds) {
        return existsForSchema(SchemaDescriptors.forRelType(relTypeId, propertyIds), isDependent);
    }

    public static UniquenessConstraintDescriptor uniqueForLabel(int labelId, int... propertyIds) {
        return uniqueForSchema(SchemaDescriptors.forLabel(labelId, propertyIds));
    }

    public static UniquenessConstraintDescriptor uniqueForLabel(IndexType indexType, int labelId, int... propertyIds) {
        return uniqueForSchema(SchemaDescriptors.forLabel(labelId, propertyIds), indexType);
    }

    public static KeyConstraintDescriptor nodeKeyForLabel(int labelId, int... propertyIds) {
        return keyForSchema(SchemaDescriptors.forLabel(labelId, propertyIds));
    }

    public static KeyConstraintDescriptor nodeKeyForLabel(IndexType indexType, int labelId, int... propertyIds) {
        return keyForSchema(SchemaDescriptors.forLabel(labelId, propertyIds), indexType);
    }

    @VisibleForTesting
    public static RelationshipEndpointConstraintDescriptor relationshipEndpointForRelType(
            int relTypeId, int endpointLabelId, EndpointType endpointType) {
        return relationshipEndpointForSchema(
                SchemaDescriptors.forRelationshipEndpoint(relTypeId), endpointLabelId, endpointType);
    }

    @VisibleForTesting
    public static LabelCoexistenceConstraintDescriptor labelCoexistenceForLabel(
            int existingLabelId, int requiredLabelId) {
        return labelCoexistenceForSchema(SchemaDescriptors.forLabelCoexistence(existingLabelId), requiredLabelId);
    }
}
