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
package org.neo4j.kernel.api.schema.constraints;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.SIMPLE_NAME_LOOKUP;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.assertEquality;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.ConstraintType;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;

class ConstraintDescriptorFactoryTest {
    private static final int LABEL_ID = 0;
    private static final int REL_TYPE_ID = 0;

    @Test
    void shouldCreateExistsConstraintDescriptors() {
        ConstraintDescriptor desc;

        desc = ConstraintDescriptorFactory.existsForLabel(false, LABEL_ID, 1);
        assertThat(desc.type()).isEqualTo(ConstraintType.EXISTS);
        assertThat(desc.schema()).isEqualTo(SchemaDescriptors.forLabel(LABEL_ID, 1));

        desc = ConstraintDescriptorFactory.existsForRelType(false, REL_TYPE_ID, 1);
        assertThat(desc.type()).isEqualTo(ConstraintType.EXISTS);
        assertThat(desc.schema()).isEqualTo(SchemaDescriptors.forRelType(REL_TYPE_ID, 1));
    }

    @Test
    void shouldCreateUniqueConstraintDescriptors() {
        ConstraintDescriptor desc;

        desc = ConstraintDescriptorFactory.uniqueForLabel(LABEL_ID, 1);
        assertThat(desc.type()).isEqualTo(ConstraintType.UNIQUE);
        assertThat(desc.schema()).isEqualTo(SchemaDescriptors.forLabel(LABEL_ID, 1));
    }

    @Test
    void shouldCreateNodeKeyConstraintDescriptors() {
        ConstraintDescriptor desc;

        desc = ConstraintDescriptorFactory.nodeKeyForLabel(LABEL_ID, 1);
        assertThat(desc.type()).isEqualTo(ConstraintType.UNIQUE_EXISTS);
        assertThat(desc.schema()).isEqualTo(SchemaDescriptors.forLabel(LABEL_ID, 1));
    }

    @Test
    void shouldCreateConstraintDescriptorsFromSchema() {
        ConstraintDescriptor desc;

        desc = ConstraintDescriptorFactory.uniqueForSchema(SchemaDescriptors.forLabel(LABEL_ID, 1));
        assertThat(desc.type()).isEqualTo(ConstraintType.UNIQUE);
        assertThat(desc.schema()).isEqualTo(SchemaDescriptors.forLabel(LABEL_ID, 1));

        desc = ConstraintDescriptorFactory.keyForSchema(SchemaDescriptors.forLabel(LABEL_ID, 1));
        assertThat(desc.type()).isEqualTo(ConstraintType.UNIQUE_EXISTS);
        assertThat(desc.schema()).isEqualTo(SchemaDescriptors.forLabel(LABEL_ID, 1));

        desc = ConstraintDescriptorFactory.existsForSchema(SchemaDescriptors.forRelType(REL_TYPE_ID, 1), false);
        assertThat(desc.type()).isEqualTo(ConstraintType.EXISTS);
        assertThat(desc.schema()).isEqualTo(SchemaDescriptors.forRelType(REL_TYPE_ID, 1));
    }

    @Test
    void shouldCreateEqualDescriptors() {
        ConstraintDescriptor desc1;
        ConstraintDescriptor desc2;

        desc1 = ConstraintDescriptorFactory.uniqueForLabel(LABEL_ID, 1);
        desc2 = ConstraintDescriptorFactory.uniqueForLabel(LABEL_ID, 1);
        assertEquality(desc1, desc2);

        desc1 = ConstraintDescriptorFactory.existsForLabel(false, LABEL_ID, 1);
        desc2 = ConstraintDescriptorFactory.existsForLabel(false, LABEL_ID, 1);
        assertEquality(desc1, desc2);

        desc1 = ConstraintDescriptorFactory.existsForRelType(false, LABEL_ID, 1);
        desc2 = ConstraintDescriptorFactory.existsForRelType(false, LABEL_ID, 1);
        assertEquality(desc1, desc2);
    }

    @Test
    void shouldGiveNiceUserDescriptions() {
        assertThat(ConstraintDescriptorFactory.existsForLabel(false, 1, 2)
                        .withId(1)
                        .withName("Constraint 1")
                        .userDescription(SIMPLE_NAME_LOOKUP))
                .isEqualTo(
                        "Constraint( id=1, name='Constraint 1', type='NODE PROPERTY EXISTENCE', schema=(:Label1 {property2}) )");
        assertThat(ConstraintDescriptorFactory.existsForRelType(false, 1, 3)
                        .withId(2)
                        .withName("Constraint 2")
                        .userDescription(SIMPLE_NAME_LOOKUP))
                .isEqualTo(
                        "Constraint( id=2, name='Constraint 2', type='RELATIONSHIP PROPERTY EXISTENCE', schema=()-[:RelType1 {property3}]-() )");
        assertThat(ConstraintDescriptorFactory.uniqueForLabel(2, 4)
                        .withId(3)
                        .withName("Constraint 3")
                        .userDescription(SIMPLE_NAME_LOOKUP))
                .isEqualTo("Constraint( id=3, name='Constraint 3', type='UNIQUENESS', schema=(:Label2 {property4}) )");
    }
}
