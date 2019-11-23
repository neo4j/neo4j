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
package org.neo4j.internal.schema;

import org.junit.jupiter.api.Test;

import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.NodeKeyConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.existsForLabel;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.nodeKeyForLabel;

class ConstraintDescriptorTest extends SchemaRuleTestBase
{
    @Test
    void shouldCreateUniquenessConstraint()
    {
        // GIVEN
        ConstraintDescriptor descriptor = ConstraintDescriptorFactory.uniqueForLabel( LABEL_ID, PROPERTY_ID_1 );
        ConstraintDescriptor constraint = descriptor.withId( RULE_ID );

        // THEN
        assertThat( constraint.getId() ).isEqualTo( RULE_ID );
        assertThat( constraint.schema() ).isEqualTo( descriptor.schema() );
        assertThat( constraint ).isEqualTo( descriptor );
        assertThrows( IllegalStateException.class, () -> constraint.asIndexBackedConstraint().ownedIndexId() );
    }

    @Test
    void shouldCreateUniquenessConstraintWithOwnedIndex()
    {
        // GIVEN
        UniquenessConstraintDescriptor descriptor = ConstraintDescriptorFactory.uniqueForLabel( LABEL_ID, PROPERTY_ID_1 );
        UniquenessConstraintDescriptor constraint = descriptor.withId( RULE_ID ).withOwnedIndexId( RULE_ID_2 );

        // THEN
        assertThat( constraint ).isEqualTo( descriptor );
        assertThat( constraint.ownedIndexId() ).isEqualTo( RULE_ID_2 );
    }

    @Test
    void shouldCreateNodeKeyConstraint()
    {
        // GIVEN
        ConstraintDescriptor descriptor = nodeKeyForLabel( LABEL_ID, PROPERTY_ID_1 );
        ConstraintDescriptor constraint = descriptor.withId( RULE_ID );

        // THEN
        assertThat( constraint.getId() ).isEqualTo( RULE_ID );
        assertThat( constraint.schema() ).isEqualTo( descriptor.schema() );
        assertThat( constraint ).isEqualTo( descriptor );
        assertThrows( IllegalStateException.class, () -> constraint.asIndexBackedConstraint().ownedIndexId() );
    }

    @Test
    void shouldCreateNodeKeyConstraintWithOwnedIndex()
    {
        // GIVEN
        NodeKeyConstraintDescriptor descriptor = nodeKeyForLabel( LABEL_ID, PROPERTY_ID_1 );
        NodeKeyConstraintDescriptor constraint = descriptor.withId( RULE_ID ).withOwnedIndexId( RULE_ID_2 );

        // THEN
        assertThat( constraint ).isEqualTo( descriptor );
        assertThat( constraint.ownedIndexId() ).isEqualTo( RULE_ID_2 );
    }

    @Test
    void shouldCreateExistenceConstraint()
    {
        // GIVEN
        ConstraintDescriptor descriptor = existsForLabel( LABEL_ID, PROPERTY_ID_1 );
        ConstraintDescriptor constraint = descriptor.withId( RULE_ID );

        // THEN
        assertThat( constraint.getId() ).isEqualTo( RULE_ID );
        assertThat( constraint.schema() ).isEqualTo( descriptor.schema() );
        assertThat( constraint ).isEqualTo( descriptor );
        assertThrows( IllegalStateException.class, () -> constraint.asIndexBackedConstraint().ownedIndexId() );
    }

    @Test
    void indexRulesAreEqualBasedOnConstraintDescriptor()
    {
        assertEqualityByDescriptor( ConstraintDescriptorFactory.existsForLabel( LABEL_ID, PROPERTY_ID_1 ) );
        assertEqualityByDescriptor( ConstraintDescriptorFactory.uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ) );
        assertEqualityByDescriptor( ConstraintDescriptorFactory.nodeKeyForLabel( LABEL_ID, PROPERTY_ID_1 ) );
        assertEqualityByDescriptor( ConstraintDescriptorFactory.existsForRelType( REL_TYPE_ID, PROPERTY_ID_1 ) );
        assertEqualityByDescriptor( ConstraintDescriptorFactory.existsForLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ) );
        assertEqualityByDescriptor( ConstraintDescriptorFactory.uniqueForLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ) );
        assertEqualityByDescriptor( ConstraintDescriptorFactory.nodeKeyForLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ) );
    }

    private void assertEqualityByDescriptor( UniquenessConstraintDescriptor descriptor )
    {
        ConstraintDescriptor rule1 = descriptor.withId( RULE_ID ).withOwnedIndexId( RULE_ID_2 );
        ConstraintDescriptor rule2 = descriptor.withId( RULE_ID_2 );

        assertEquality( rule1, rule2 );
    }

    private void assertEqualityByDescriptor( ConstraintDescriptor descriptor )
    {
        ConstraintDescriptor rule1 = descriptor.withId( RULE_ID );
        ConstraintDescriptor rule2 = descriptor.withId( RULE_ID_2 );

        assertEquality( rule1, rule2 );
    }
}
