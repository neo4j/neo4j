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
package org.neo4j.kernel.impl.store.record;

import org.junit.Test;

import java.util.Optional;

import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.storageengine.api.DefaultStorageIndexReference;
import org.neo4j.storageengine.api.StorageIndexReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.test.assertion.Assert.assertException;

public class IndexRuleTest extends SchemaRuleTestBase
{
    @Test
    public void shouldCreateGeneralIndex()
    {
        // GIVEN
        IndexDescriptor descriptor = forLabel( LABEL_ID, PROPERTY_ID_1 );
        StorageIndexReference indexRule = new DefaultStorageIndexReference( descriptor, RULE_ID, null );

        // THEN
        assertThat( indexRule.getId(), equalTo( RULE_ID ) );
        assertFalse( indexRule.isUnique() );
        assertThat( indexRule.schema(), equalTo( descriptor.schema() ) );
        assertThat( indexRule, equalTo( descriptor ) );
        assertThat( indexRule.providerKey(), equalTo( PROVIDER_KEY ) );
        assertThat( indexRule.providerVersion(), equalTo( PROVIDER_VERSION ) );
        assertException( indexRule::owningConstraintReference, IllegalStateException.class );
    }

    @Test
    public void shouldCreateUniqueIndex()
    {
        // GIVEN
        IndexDescriptor descriptor = uniqueForLabel( LABEL_ID, PROPERTY_ID_1 );
        StorageIndexReference indexRule = new DefaultStorageIndexReference( descriptor, RULE_ID, null );

        // THEN
        assertThat( indexRule.getId(), equalTo( RULE_ID ) );
        assertTrue( indexRule.isUnique() );
        assertThat( indexRule.schema(), equalTo( descriptor.schema() ) );
        assertThat( indexRule, equalTo( descriptor ) );
        assertThat( indexRule.providerKey(), equalTo( PROVIDER_KEY ) );
        assertThat( indexRule.providerVersion(), equalTo( PROVIDER_VERSION ) );
        assertThat( indexRule.hasOwningConstraintReference(), equalTo( false ) );

        StorageIndexReference withConstraint = new DefaultStorageIndexReference( indexRule, RULE_ID_2 );
        assertThat( withConstraint.owningConstraintReference(), equalTo( RULE_ID_2 ) );
        assertThat( indexRule.hasOwningConstraintReference(), equalTo( false ) ); // this is unchanged
    }

    @Test
    public void indexRulesAreEqualBasedOnIndexDescriptor()
    {
        assertEqualityByDescriptor( forLabel( LABEL_ID, PROPERTY_ID_1 ) );
        assertEqualityByDescriptor( uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ) );
        assertEqualityByDescriptor( forLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ) );
        assertEqualityByDescriptor( uniqueForLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ) );
    }

    @Test
    public void detectUniqueIndexWithoutOwningConstraint()
    {
        IndexDescriptor descriptor = uniqueForLabel( LABEL_ID, PROPERTY_ID_1 );
        StorageIndexReference indexRule = new DefaultStorageIndexReference( descriptor, RULE_ID, null );

        assertTrue( indexRule.isUnique() && !indexRule.hasOwningConstraintReference() );
    }

    private void assertEqualityByDescriptor( IndexDescriptor descriptor )
    {
        StorageIndexReference rule1 = new DefaultStorageIndexReference( descriptor, RULE_ID, null );
        StorageIndexReference rule2 = new DefaultStorageIndexReference( descriptor, RULE_ID_2, null );
        StorageIndexReference rule3 = new DefaultStorageIndexReference( descriptor.schema(), descriptor.providerKey(), descriptor.providerVersion(),
                RULE_ID, Optional.empty(), descriptor.isUnique(), null, false );

        assertEquality( rule1, rule2 );
        assertEquality( rule1, rule3 );
    }
}
