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

import java.util.NoSuchElementException;
import java.util.OptionalLong;
import java.util.function.BiFunction;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.test.assertion.Assert.assertException;

class IndexDescriptorSampleTest extends SchemaRuleTestBase
{
    @Test
    void shouldCreateGeneralIndex()
    {
        // GIVEN
        IndexPrototype prototype = forLabel( LABEL_ID, PROPERTY_ID_1 );
        IndexDescriptor indexRule = prototype.withName( "index" ).materialise( RULE_ID );

        // THEN
        assertThat( indexRule.getId(), equalTo( RULE_ID ) );
        assertFalse( indexRule.isUnique() );
        assertThat( indexRule.schema(), equalTo( prototype.schema() ) );
        assertThat( indexRule, equalTo( prototype ) );
        assertThat( indexRule.getIndexProvider(), equalTo( PROVIDER ) );
        assertException( indexRule.getOwningConstraintId()::getAsLong, NoSuchElementException.class );
    }

    @Test
    void shouldCreateUniqueIndex()
    {
        // GIVEN
        IndexPrototype prototype = uniqueForLabel( LABEL_ID, PROPERTY_ID_1 );
        IndexDescriptor indexRule = prototype.withName( "index" ).materialise( RULE_ID );

        // THEN
        assertThat( indexRule.getId(), equalTo( RULE_ID ) );
        assertTrue( indexRule.isUnique() );
        assertThat( indexRule.schema(), equalTo( prototype.schema() ) );
        assertThat( indexRule, equalTo( prototype ) );
        assertThat( indexRule.getIndexProvider(), equalTo( PROVIDER ) );
        assertTrue( indexRule.getOwningConstraintId().isEmpty() );

        IndexDescriptor withConstraint = indexRule.withOwningConstraintId( RULE_ID_2 );
        OptionalLong owningConstraintId = withConstraint.getOwningConstraintId();
        assertTrue( owningConstraintId.isPresent() );
        assertThat( owningConstraintId.getAsLong(), equalTo( RULE_ID_2 ) );
        assertTrue( indexRule.getOwningConstraintId().isEmpty() ); // this is unchanged
    }

    @Test
    void indexRulesAreEqualBasedOnIndexDescriptor()
    {
        assertEqualityByDescriptor( forLabel( LABEL_ID, PROPERTY_ID_1 ) );
        assertEqualityByDescriptor( uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ) );
        assertEqualityByDescriptor( forLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ) );
        assertEqualityByDescriptor( uniqueForLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ) );
    }

    @Test
    void detectUniqueIndexWithoutOwningConstraint()
    {
        IndexPrototype descriptor = namedUniqueForLabel( "index", LABEL_ID, PROPERTY_ID_1 );
        IndexDescriptor indexRule = descriptor.materialise( RULE_ID );

        assertTrue( indexRule.isUnique() && indexRule.getOwningConstraintId().isEmpty() );
    }

    private void assertEqualityByDescriptor( IndexPrototype descriptor )
    {
        IndexDescriptor rule1 = descriptor.withName( "a" ).materialise( RULE_ID );
        IndexDescriptor rule2 = descriptor.withName( "b" ).materialise( RULE_ID_2 );

        BiFunction<SchemaDescriptor,IndexProviderDescriptor,IndexPrototype> factory =
                descriptor.isUnique() ? IndexPrototype::uniqueForSchema : IndexPrototype::forSchema;
        IndexDescriptor rule3 = factory.apply( descriptor.schema(), descriptor.getIndexProvider() ).withName( "c" ).materialise( RULE_ID );

        assertEquality( rule1, rule2 );
        assertEquality( rule1, rule3 );
    }
}
