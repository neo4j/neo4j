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
package org.neo4j.internal.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.existsForLabel;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.nodeKeyForLabel;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.nodeLabelExistenceForLabel;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.relationshipEndpointLabelForRelType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.ExistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.KeyConstraintDescriptor;
import org.neo4j.internal.schema.constraints.NodeLabelExistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.RelationshipEndpointLabelConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;

class ConstraintDescriptorTest extends SchemaRuleTestBase {
    @Test
    void shouldCreateUniquenessConstraint() {
        // GIVEN
        ConstraintDescriptor descriptor = ConstraintDescriptorFactory.uniqueForLabel(LABEL_ID, PROPERTY_ID_1);
        ConstraintDescriptor constraint = descriptor.withId(RULE_ID);

        // THEN
        assertThat(constraint.getId()).isEqualTo(RULE_ID);
        assertThat(constraint.schema()).isEqualTo(descriptor.schema());
        assertThat(constraint).isEqualTo(descriptor);
        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> constraint.asIndexBackedConstraint().ownedIndexId());
    }

    @Test
    void shouldCreateUniquenessConstraintWithOwnedIndex() {
        // GIVEN
        UniquenessConstraintDescriptor descriptor = ConstraintDescriptorFactory.uniqueForLabel(LABEL_ID, PROPERTY_ID_1);
        UniquenessConstraintDescriptor constraint = descriptor.withId(RULE_ID).withOwnedIndexId(RULE_ID_2);

        // THEN
        assertThat(constraint).isEqualTo(descriptor);
        assertThat(constraint.ownedIndexId()).isEqualTo(RULE_ID_2);
    }

    @Test
    void shouldCreateNodeKeyConstraint() {
        // GIVEN
        ConstraintDescriptor descriptor = nodeKeyForLabel(LABEL_ID, PROPERTY_ID_1);
        ConstraintDescriptor constraint = descriptor.withId(RULE_ID);

        // THEN
        assertThat(constraint.getId()).isEqualTo(RULE_ID);
        assertThat(constraint.schema()).isEqualTo(descriptor.schema());
        assertThat(constraint).isEqualTo(descriptor);
        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> constraint.asIndexBackedConstraint().ownedIndexId());
    }

    @Test
    void shouldCreateNodeKeyConstraintWithOwnedIndex() {
        // GIVEN
        KeyConstraintDescriptor descriptor = nodeKeyForLabel(LABEL_ID, PROPERTY_ID_1);
        KeyConstraintDescriptor constraint = descriptor.withId(RULE_ID).withOwnedIndexId(RULE_ID_2);

        // THEN
        assertThat(constraint).isEqualTo(descriptor);
        assertThat(constraint.ownedIndexId()).isEqualTo(RULE_ID_2);
    }

    @Test
    void shouldCreateExistenceConstraint() {
        // GIVEN
        ConstraintDescriptor descriptor = existsForLabel(false, LABEL_ID, PROPERTY_ID_1);
        ConstraintDescriptor constraint = descriptor.withId(RULE_ID);

        // THEN
        assertThat(constraint.getId()).isEqualTo(RULE_ID);
        assertThat(constraint.schema()).isEqualTo(descriptor.schema());
        assertThat(constraint).isEqualTo(descriptor);
        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> constraint.asIndexBackedConstraint().ownedIndexId());
    }

    @ParameterizedTest
    @EnumSource(EndpointType.class)
    void shouldCreateRelationshipEndpointLabelConstraint(EndpointType endpointType) {
        // GIVEN
        RelationshipEndpointLabelConstraintDescriptor descriptor =
                relationshipEndpointLabelForRelType(REL_TYPE_ID, LABEL_ID, endpointType);
        RelationshipEndpointLabelConstraintDescriptor constraint = descriptor.withId(RULE_ID);
        RelationshipEndpointLabelConstraintDescriptor relationshipEndpointLabelConstraint =
                constraint.asRelationshipEndpointLabelConstraint();

        assertThat(constraint.getId()).isEqualTo(RULE_ID);
        assertThat(constraint.schema()).isEqualTo(descriptor.schema());
        assertThat(constraint).isEqualTo(descriptor);
        assertThat(relationshipEndpointLabelConstraint.endpointLabelId()).isEqualTo(LABEL_ID);
        assertThat(relationshipEndpointLabelConstraint.endpointType()).isEqualTo(endpointType);
        assertThatExceptionOfType(IllegalStateException.class).isThrownBy(constraint::asPropertyExistenceConstraint);
    }

    @Test
    void shouldFailToCreateRelationshipEndpointLabelConstraintWithNegativeEntityTokenId() {
        assertThatThrownBy(() -> relationshipEndpointLabelForRelType(-1, LABEL_ID, EndpointType.END))
                .hasMessageContaining("must not have a negative entity token id");
    }

    @Test
    void shouldFailToCreateRelationshipEndpointLabelConstraintWithNegativeEndpointLabelId() {
        assertThatThrownBy(() -> relationshipEndpointLabelForRelType(REL_TYPE_ID, -1, EndpointType.END))
                .hasMessageContaining("endpointLabelId cannot be negative");
    }

    @Test
    void shouldFailToCreateRelationshipEndpointLabelConstraintWithNullEndpointType() {
        assertThatThrownBy(() -> relationshipEndpointLabelForRelType(REL_TYPE_ID, LABEL_ID, null))
                .hasMessageContaining("EndpointType cannot be null");
    }

    @Test
    void shouldCreateNodeLabelExistenceConstraint() {
        // GIVEN
        NodeLabelExistenceConstraintDescriptor descriptor = nodeLabelExistenceForLabel(LABEL_ID, 11);
        NodeLabelExistenceConstraintDescriptor constraint = descriptor.withId(RULE_ID);
        NodeLabelExistenceConstraintDescriptor nodeLabelExistenceConstraint =
                constraint.asNodeLabelExistenceConstraint();

        assertThat(constraint.getId()).isEqualTo(RULE_ID);
        assertThat(constraint.schema()).isEqualTo(descriptor.schema());
        assertThat(constraint).isEqualTo(descriptor);
        assertThat(nodeLabelExistenceConstraint.requiredLabelId()).isEqualTo(11);
        assertThatExceptionOfType(IllegalStateException.class).isThrownBy(constraint::asPropertyExistenceConstraint);
    }

    @Test
    void shouldFailToCreateNodeLabelExistenceConstraintWithNegativeEntityTokenId() {
        assertThatThrownBy(() -> nodeLabelExistenceForLabel(-1, LABEL_ID))
                .hasMessageContaining("must not have a negative entity token id");
    }

    @Test
    void shouldFailToCreateNodeLabelExistenceConstraintWithNegativeRequiredLabelId() {
        assertThatThrownBy(() -> nodeLabelExistenceForLabel(LABEL_ID, -1))
                .hasMessageContaining("requiredLabelId cannot be negative");
    }

    @Test
    void shouldFailToCreateNodeLabelExistenceConstraintWithEqualRequiredLabelAndLabelId() {
        assertThatThrownBy(() -> nodeLabelExistenceForLabel(LABEL_ID, LABEL_ID))
                .hasMessageContaining("requiredLabelId cannot be same as schema labelId");
    }

    @Test
    void indexRulesAreEqualBasedOnConstraintDescriptor() {
        assertEqualityByDescriptor(ConstraintDescriptorFactory.existsForLabel(false, LABEL_ID, PROPERTY_ID_1));
        assertEqualityByDescriptor(ConstraintDescriptorFactory.uniqueForLabel(LABEL_ID, PROPERTY_ID_1));
        assertEqualityByDescriptor(ConstraintDescriptorFactory.nodeKeyForLabel(LABEL_ID, PROPERTY_ID_1));
        assertEqualityByDescriptor(ConstraintDescriptorFactory.existsForRelType(false, REL_TYPE_ID, PROPERTY_ID_1));
        assertEqualityByDescriptor(
                ConstraintDescriptorFactory.existsForLabel(false, LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2));
        assertEqualityByDescriptor(ConstraintDescriptorFactory.uniqueForLabel(LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2));
        assertEqualityByDescriptor(ConstraintDescriptorFactory.nodeKeyForLabel(LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2));
    }

    @Test
    void isRelationshipEndpointLabelConstraintShouldReturnFalseWhenNot() {
        KeyConstraintDescriptor nodeKeyConstraintDescriptor = nodeKeyForLabel(LABEL_ID, PROPERTY_ID_1);
        assertThat(nodeKeyConstraintDescriptor.isRelationshipEndpointLabelConstraint())
                .isFalse();

        UniquenessConstraintDescriptor nodeUniquenessConstraintDescriptor =
                ConstraintDescriptorFactory.uniqueForLabel(LABEL_ID, PROPERTY_ID_1);
        assertThat(nodeUniquenessConstraintDescriptor.isRelationshipEndpointLabelConstraint())
                .isFalse();

        ExistenceConstraintDescriptor nodeExistenceConstraintDescriptor =
                existsForLabel(false, LABEL_ID, PROPERTY_ID_1);
        assertThat(nodeExistenceConstraintDescriptor.isRelationshipEndpointLabelConstraint())
                .isFalse();

        ExistenceConstraintDescriptor relationshipExistenceConstraintDescriptor =
                ConstraintDescriptorFactory.existsForRelType(false, REL_TYPE_ID, PROPERTY_ID_1);
        assertThat(relationshipExistenceConstraintDescriptor.isRelationshipEndpointLabelConstraint())
                .isFalse();
    }

    @Test
    void sameSchemaDifferentIndexTypesShouldNotConflict() {
        LabelSchemaDescriptor schemaDescriptor = SchemaDescriptors.forLabel(LABEL_ID, PROPERTY_ID_1);
        UniquenessConstraintDescriptor range =
                ConstraintDescriptorFactory.uniqueForSchema(schemaDescriptor, IndexType.RANGE);
        UniquenessConstraintDescriptor text =
                ConstraintDescriptorFactory.uniqueForSchema(schemaDescriptor, IndexType.TEXT);
        assertThat(range.conflictsWith(text)).isFalse();
    }

    @Test
    void sameSchemaSameIndexTypesShouldConflict() {
        LabelSchemaDescriptor schemaDescriptor = SchemaDescriptors.forLabel(LABEL_ID, PROPERTY_ID_1);
        UniquenessConstraintDescriptor range =
                ConstraintDescriptorFactory.uniqueForSchema(schemaDescriptor, IndexType.RANGE);
        UniquenessConstraintDescriptor range2 =
                ConstraintDescriptorFactory.uniqueForSchema(schemaDescriptor, IndexType.RANGE);
        assertThat(range.conflictsWith(range2)).isTrue();
    }

    private static void assertEqualityByDescriptor(UniquenessConstraintDescriptor descriptor) {
        ConstraintDescriptor rule1 = descriptor.withId(RULE_ID).withOwnedIndexId(RULE_ID_2);
        ConstraintDescriptor rule2 = descriptor.withId(RULE_ID_2);

        assertEquality(rule1, rule2);
    }

    private static void assertEqualityByDescriptor(ConstraintDescriptor descriptor) {
        ConstraintDescriptor rule1 = descriptor.withId(RULE_ID);
        ConstraintDescriptor rule2 = descriptor.withId(RULE_ID_2);

        assertEquality(rule1, rule2);
    }
}
