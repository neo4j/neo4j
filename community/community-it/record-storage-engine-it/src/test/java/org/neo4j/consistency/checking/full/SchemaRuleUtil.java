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
package org.neo4j.consistency.checking.full;

import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;

import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;

public final class SchemaRuleUtil {
    private SchemaRuleUtil() {}

    public static ConstraintDescriptor uniquenessConstraintRule(
            long ruleId, int labelId, int propertyId, long indexId, IndexType indexType) {
        return ConstraintDescriptorFactory.uniqueForLabel(indexType, labelId, propertyId)
                .withId(ruleId)
                .withName("constraint_" + ruleId)
                .withOwnedIndexId(indexId);
    }

    public static ConstraintDescriptor nodePropertyExistenceConstraintRule(
            long ruleId, int labelId, int propertyId, boolean isDependent) {
        return ConstraintDescriptorFactory.existsForLabel(isDependent, labelId, propertyId)
                .withId(ruleId)
                .withName("constraint_" + ruleId);
    }

    public static ConstraintDescriptor relPropertyExistenceConstraintRule(
            long ruleId, int relTypeId, int propertyId, boolean isDependent) {
        return ConstraintDescriptorFactory.existsForRelType(isDependent, relTypeId, propertyId)
                .withId(ruleId)
                .withName("constraint_" + ruleId);
    }

    public static ConstraintDescriptor nodePropertyTypeConstraintRule(
            long ruleId, int labelId, int propertyId, PropertyTypeSet propertyTypeSet, boolean isDependent) {
        return ConstraintDescriptorFactory.typeForSchema(
                        SchemaDescriptors.forLabel(labelId, propertyId), propertyTypeSet, isDependent)
                .withId(ruleId)
                .withName("constraint_" + ruleId);
    }

    public static ConstraintDescriptor relPropertyTypeConstraintRule(
            long ruleId, int relTypeId, int propertyId, PropertyTypeSet propertyTypeSet, boolean isDependent) {
        return ConstraintDescriptorFactory.typeForSchema(
                        SchemaDescriptors.forRelType(relTypeId, propertyId), propertyTypeSet, isDependent)
                .withId(ruleId)
                .withName("constraint_" + ruleId);
    }

    public static IndexDescriptor indexRule(
            long ruleId, int labelId, int propertyId, IndexProviderDescriptor descriptor) {
        return IndexPrototype.forSchema(forLabel(labelId, propertyId), descriptor)
                .withName("index_" + ruleId)
                .materialise(ruleId);
    }

    public static IndexDescriptor constraintIndexRule(
            long ruleId,
            int labelId,
            int propertyId,
            IndexProviderDescriptor descriptor,
            long constraintId,
            IndexType indexType) {
        return IndexPrototype.uniqueForSchema(forLabel(labelId, propertyId), descriptor)
                .withIndexType(indexType)
                .withName("constraint_" + constraintId)
                .materialise(ruleId)
                .withOwningConstraintId(constraintId);
    }

    public static IndexDescriptor constraintIndexRule(
            long ruleId, int labelId, int propertyId, IndexProviderDescriptor descriptor) {
        return IndexPrototype.uniqueForSchema(forLabel(labelId, propertyId), descriptor)
                .withName("constraint_" + ruleId)
                .materialise(ruleId);
    }
}
