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
package org.neo4j.internal.collector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.ConstraintType;
import org.neo4j.internal.schema.EndpointType;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.internal.schema.constraints.SchemaValueType;
import org.neo4j.test.InMemoryTokens;

class ConstraintSubSectionTest {

    private enum Constraint {
        NODE_PROPERTY_UNIQUENESS(ConstraintDescriptorFactory.uniqueForSchema(SchemaDescriptors.forLabel(0, 0))),
        RELATIONSHIP_PROPERTY_UNIQUENESS(
                ConstraintDescriptorFactory.uniqueForSchema(SchemaDescriptors.forRelType(0, 0))),
        NODE_PROPERTY_EXISTENCE(ConstraintDescriptorFactory.existsForSchema(SchemaDescriptors.forLabel(0, 0), false)),
        RELATIONSHIP_PROPERTY_EXISTENCE(
                ConstraintDescriptorFactory.existsForSchema(SchemaDescriptors.forRelType(0, 0), false)),
        NODE_PROPERTY_TYPE(ConstraintDescriptorFactory.typeForSchema(
                SchemaDescriptors.forLabel(0, 0), PropertyTypeSet.of(SchemaValueType.INTEGER), false)),
        RELATIONSHIP_PROPERTY_TYPE(ConstraintDescriptorFactory.typeForSchema(
                SchemaDescriptors.forRelType(0, 0), PropertyTypeSet.of(SchemaValueType.INTEGER), false)),
        NODE_KEY(ConstraintDescriptorFactory.keyForSchema(SchemaDescriptors.forLabel(0, 0))),
        RELATIONSHIP_KEY(ConstraintDescriptorFactory.keyForSchema(SchemaDescriptors.forRelType(0, 0))),
        ENDPOINT_START(ConstraintDescriptorFactory.relationshipEndpointLabelForSchema(
                SchemaDescriptors.forRelationshipEndpointLabel(0), 0, EndpointType.START)),
        ENDPOINT_END(ConstraintDescriptorFactory.relationshipEndpointLabelForSchema(
                SchemaDescriptors.forRelationshipEndpointLabel(0), 0, EndpointType.END)),
        NODE_LABEL_EXISTENCE(
                ConstraintDescriptorFactory.nodeLabelExistenceForSchema(SchemaDescriptors.forNodeLabelExistence(0), 1));

        public final ConstraintDescriptor descriptor;

        Constraint(ConstraintDescriptor descriptor) {
            this.descriptor = descriptor;
        }
    }

    private final TokenNameLookup tokens = new InMemoryTokens()
            .label(0, "Label")
            .label(1, "Label2")
            .relationshipType(0, "REL")
            .propertyKey(0, "prop");

    private Map<String, Object> serializeConstraint(Constraint constraint) {
        return ConstraintSubSection.constraint(tokens, Anonymizer.PLAIN_TEXT, constraint.descriptor);
    }

    /**
     * Ensures that all types of constraints are covered when collecting GraphCounts, while remaining loosely coupled.
     * If this test fails, please add one or more new constraints to the enum at the top of the file, and add a serialization test for each of them below.
     * One could merely add a constraint to the enum without testing that it does serialise properly, but you wouldn't do that, would you? ಠ_ಠ
     */
    @Test
    void constraintTypesCoverage() {
        Set<ConstraintType> constraintTypes = Arrays.stream(Constraint.values())
                .map(constraint -> constraint.descriptor.type())
                .collect(Collectors.toSet());
        for (ConstraintType constraintType : ConstraintType.values()) {
            assertThat(constraintTypes)
                    .as("Missing test coverage for constraint type: " + constraintType)
                    .contains(constraintType);
        }
    }

    @Test
    void noExceptionsDuringSerialization() {
        for (Constraint constraint : Constraint.values()) {
            assertThatCode(() -> serializeConstraint(constraint))
                    .withFailMessage("Unexpected exception thrown while serializing: " + constraint)
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void nodePropertyUniquenessConstraintSerialization() {
        Map<String, Object> expectedData = Map.of(
                "label", "Label",
                "type", "Uniqueness constraint",
                "properties", List.of("prop"));
        assertThat(expectedData)
                .containsExactlyInAnyOrderEntriesOf(serializeConstraint(Constraint.NODE_PROPERTY_UNIQUENESS));
    }

    @Test
    void relationshipPropertyUniquenessConstraintSerialization() {
        Map<String, Object> expectedData = Map.of(
                "relationshipType", "REL",
                "type", "Uniqueness constraint",
                "properties", List.of("prop"));
        assertThat(expectedData)
                .containsExactlyInAnyOrderEntriesOf(serializeConstraint(Constraint.RELATIONSHIP_PROPERTY_UNIQUENESS));
    }

    @Test
    void nodePropertyExistenceConstraintSerialization() {
        Map<String, Object> expectedData = Map.of(
                "label", "Label",
                "type", "Existence constraint",
                "properties", List.of("prop"));
        assertThat(expectedData)
                .containsExactlyInAnyOrderEntriesOf(serializeConstraint(Constraint.NODE_PROPERTY_EXISTENCE));
    }

    @Test
    void relationshipPropertyExistenceConstraintSerialization() {
        Map<String, Object> expectedData = Map.of(
                "relationshipType", "REL",
                "type", "Existence constraint",
                "properties", List.of("prop"));
        assertThat(expectedData)
                .containsExactlyInAnyOrderEntriesOf(serializeConstraint(Constraint.RELATIONSHIP_PROPERTY_EXISTENCE));
    }

    @Test
    void nodePropertyTypeConstraintSerialization() {
        Map<String, Object> expectedData = Map.ofEntries(
                Map.entry("label", "Label"),
                Map.entry("type", "Property type constraint"),
                Map.entry("properties", List.of("prop")),
                Map.entry("propertyTypes", List.of("INTEGER")));
        assertThat(expectedData).containsExactlyInAnyOrderEntriesOf(serializeConstraint(Constraint.NODE_PROPERTY_TYPE));
    }

    @Test
    void relationshipPropertyTypeConstraintSerialization() {
        Map<String, Object> expectedData = Map.ofEntries(
                Map.entry("relationshipType", "REL"),
                Map.entry("type", "Property type constraint"),
                Map.entry("properties", List.of("prop")),
                Map.entry("propertyTypes", List.of("INTEGER")));
        assertThat(expectedData)
                .containsExactlyInAnyOrderEntriesOf(serializeConstraint(Constraint.RELATIONSHIP_PROPERTY_TYPE));
    }

    @Test
    void nodeKeyConstraintSerialization() {
        Map<String, Object> expectedData = Map.of(
                "label", "Label",
                "type", "Node Key",
                "properties", List.of("prop"));
        assertThat(expectedData).containsExactlyInAnyOrderEntriesOf(serializeConstraint(Constraint.NODE_KEY));
    }

    @Test
    void relationshipKeyConstraintSerialization() {
        Map<String, Object> expectedData = Map.of(
                "relationshipType", "REL",
                "type", "Node Key", // TODO: do we really want to serialize a relationship key constraint as Node Key?
                "properties", List.of("prop"));
        assertThat(expectedData).containsExactlyInAnyOrderEntriesOf(serializeConstraint(Constraint.RELATIONSHIP_KEY));
    }

    @Test
    void relationshipEndpointLabelConstraintSerialization() {
        Map<String, Object> expectedDataStart = Map.of(
                "relationshipType", "REL",
                "type", "Relationship endpoint label constraint",
                "endpointType", "START",
                "properties", List.of(),
                "enforcedLabel", "Label");
        assertThat(expectedDataStart)
                .containsExactlyInAnyOrderEntriesOf(serializeConstraint(Constraint.ENDPOINT_START));

        Map<String, Object> expectedDataEnd = Map.of(
                "relationshipType", "REL",
                "type", "Relationship endpoint label constraint",
                "endpointType", "END",
                "properties", List.of(),
                "enforcedLabel", "Label");
        assertThat(expectedDataEnd).containsExactlyInAnyOrderEntriesOf(serializeConstraint(Constraint.ENDPOINT_END));
    }

    @Test
    void nodeLabelExistenceConstraintSerialization() {
        Map<String, Object> expectedData = Map.of(
                "label", "Label",
                "type", "Node label existence constraint",
                "properties", List.of(),
                "enforcedLabel", "Label2");
        assertThat(expectedData)
                .containsExactlyInAnyOrderEntriesOf(serializeConstraint(Constraint.NODE_LABEL_EXISTENCE));
    }
}
