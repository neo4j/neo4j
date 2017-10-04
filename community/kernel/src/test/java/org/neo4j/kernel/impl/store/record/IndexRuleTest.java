/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.store.record;

import org.junit.Test;

import org.neo4j.kernel.api.schema.index.IndexDescriptor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.api.schema.index.IndexDescriptorFactory.forLabel;
import static org.neo4j.kernel.api.schema.index.IndexDescriptorFactory.uniqueForLabel;
import static org.neo4j.test.assertion.Assert.assertException;

public class IndexRuleTest extends SchemaRuleTestBase
{
    @Test
    public void shouldCreateGeneralIndex() throws Exception
    {
        // GIVEN
        IndexDescriptor descriptor = forLabel( LABEL_ID, PROPERTY_ID_1 );
        IndexRule indexRule = IndexRule.indexRule( RULE_ID, descriptor, PROVIDER_DESCRIPTOR );

        // THEN
        assertThat( indexRule.getId(), equalTo( RULE_ID ) );
        assertFalse( indexRule.canSupportUniqueConstraint() );
        assertThat( indexRule.schema(), equalTo( descriptor.schema() ) );
        assertThat( indexRule.getIndexDescriptor(), equalTo( descriptor ) );
        assertThat( indexRule.getProviderDescriptor(), equalTo( PROVIDER_DESCRIPTOR ) );
        assertException( indexRule::getOwningConstraint, IllegalStateException.class );
        assertException( () -> indexRule.withOwningConstraint( RULE_ID_2 ), IllegalStateException.class );
    }

    @Test
    public void shouldCreateUniqueIndex() throws Exception
    {
        // GIVEN
        IndexDescriptor descriptor = uniqueForLabel( LABEL_ID, PROPERTY_ID_1 );
        IndexRule indexRule = IndexRule.indexRule( RULE_ID, descriptor, PROVIDER_DESCRIPTOR );

        // THEN
        assertThat( indexRule.getId(), equalTo( RULE_ID ) );
        assertTrue( indexRule.canSupportUniqueConstraint() );
        assertThat( indexRule.schema(), equalTo( descriptor.schema() ) );
        assertThat( indexRule.getIndexDescriptor(), equalTo( descriptor ) );
        assertThat( indexRule.getProviderDescriptor(), equalTo( PROVIDER_DESCRIPTOR ) );
        assertThat( indexRule.getOwningConstraint(), equalTo( null ) );

        IndexRule withConstraint = indexRule.withOwningConstraint( RULE_ID_2 );
        assertThat( withConstraint.getOwningConstraint(), equalTo( RULE_ID_2 ) );
        assertThat( indexRule.getOwningConstraint(), equalTo( null ) ); // this is unchanged
    }

    @Test
    public void indexRulesAreEqualBasedOnIndexDescriptor() throws Exception
    {
        assertEqualityByDescriptor( forLabel( LABEL_ID, PROPERTY_ID_1 ) );
        assertEqualityByDescriptor( uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ) );
        assertEqualityByDescriptor( forLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ) );
        assertEqualityByDescriptor( uniqueForLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ) );
    }

    private void assertEqualityByDescriptor( IndexDescriptor descriptor )
    {
        IndexRule rule1 = IndexRule.indexRule( RULE_ID, descriptor, PROVIDER_DESCRIPTOR );
        IndexRule rule2 = IndexRule.indexRule( RULE_ID_2, descriptor, PROVIDER_DESCRIPTOR_2 );

        assertEquality( rule1, rule2 );
    }
}
