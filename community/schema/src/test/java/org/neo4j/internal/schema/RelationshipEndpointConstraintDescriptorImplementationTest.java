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

import org.junit.jupiter.api.Test;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.RelationshipEndpointConstraintDescriptor;

final class RelationshipEndpointConstraintDescriptorImplementationTest {
    @Test
    void RelationshipEndpointConstraintShouldBeOnlyRelationshipEndpointConstraint() {
        RelationshipEndpointConstraintDescriptor endpointConstraintDescriptor =
                ConstraintDescriptorFactory.relationshipEndpointForRelType(0, 0, EndpointType.START);
        assertThat(endpointConstraintDescriptor.isRelationshipEndpointConstraint())
                .isTrue();
        assertThat(endpointConstraintDescriptor.isRelationshipPropertyTypeConstraint())
                .isFalse();
        assertThat(endpointConstraintDescriptor.isRelationshipKeyConstraint()).isFalse();
        assertThat(endpointConstraintDescriptor.isRelationshipPropertyExistenceConstraint())
                .isFalse();
        assertThat(endpointConstraintDescriptor.isRelationshipUniquenessConstraint())
                .isFalse();

        assertThat(endpointConstraintDescriptor.isKeyConstraint()).isFalse();
        assertThat(endpointConstraintDescriptor.isIndexBackedConstraint()).isFalse();
        assertThat(endpointConstraintDescriptor.isNodeKeyConstraint()).isFalse();
        assertThat(endpointConstraintDescriptor.isUniquenessConstraint()).isFalse();
        assertThat(endpointConstraintDescriptor.isPropertyTypeConstraint()).isFalse();
        assertThat(endpointConstraintDescriptor.isNodePropertyExistenceConstraint())
                .isFalse();
        assertThat(endpointConstraintDescriptor.isNodeUniquenessConstraint()).isFalse();
        assertThat(endpointConstraintDescriptor.isPropertyExistenceConstraint()).isFalse();
        assertThat(endpointConstraintDescriptor.isNodePropertyTypeConstraint()).isFalse();
    }
}
