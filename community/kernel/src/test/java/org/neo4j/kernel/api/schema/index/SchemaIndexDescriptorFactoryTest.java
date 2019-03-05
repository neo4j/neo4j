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
package org.neo4j.kernel.api.schema.index;

import org.junit.jupiter.api.Test;

import org.neo4j.internal.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.impl.index.schema.IndexDescriptor;
import org.neo4j.kernel.impl.index.schema.IndexDescriptorFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.assertEquality;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.simpleNameLookup;

class SchemaIndexDescriptorFactoryTest
{
    private static final int LABEL_ID = 0;

    @Test
    void shouldCreateIndexDescriptors()
    {
        IndexDescriptor desc;

        desc = TestIndexDescriptorFactory.forLabel( LABEL_ID, 1 );
        assertFalse( desc.isUnique() );
        assertThat( desc.schema(), equalTo( SchemaDescriptorFactory.forLabel( LABEL_ID, 1 ) ) );
    }

    @Test
    void shouldCreateUniqueIndexDescriptors()
    {
        IndexDescriptor desc;

        desc = TestIndexDescriptorFactory.uniqueForLabel( LABEL_ID, 1 );
        assertTrue( desc.isUnique() );
        assertThat( desc.schema(), equalTo( SchemaDescriptorFactory.forLabel( LABEL_ID, 1 ) ) );
    }

    @Test
    void shouldCreateIndexDescriptorsFromSchema()
    {
        IndexDescriptor desc;

        desc = IndexDescriptorFactory.forSchema( SchemaDescriptorFactory.forLabel( LABEL_ID, 1 ) );
        assertFalse( desc.isUnique() );
        assertThat( desc.schema(), equalTo( SchemaDescriptorFactory.forLabel( LABEL_ID, 1 ) ) );

        desc = IndexDescriptorFactory.uniqueForSchema( SchemaDescriptorFactory.forLabel( LABEL_ID, 1 ) );
        assertTrue( desc.isUnique() );
        assertThat( desc.schema(), equalTo( SchemaDescriptorFactory.forLabel( LABEL_ID, 1 ) ) );
    }

    @Test
    void shouldCreateEqualDescriptors()
    {
        IndexDescriptor desc1;
        IndexDescriptor desc2;
        desc1 = TestIndexDescriptorFactory.forLabel( LABEL_ID, 1 );
        desc2 = TestIndexDescriptorFactory.forLabel( LABEL_ID, 1 );
        assertEquality( desc1, desc2 );

        desc1 = TestIndexDescriptorFactory.uniqueForLabel( LABEL_ID, 1 );
        desc2 = TestIndexDescriptorFactory.uniqueForLabel( LABEL_ID, 1 );
        assertEquality( desc1, desc2 );
    }

    @Test
    void shouldGiveNiceUserDescriptions()
    {
        assertThat( TestIndexDescriptorFactory.forLabel( 1, 2 ).userDescription( simpleNameLookup ),
                    equalTo( "Index( GENERAL, :Label1(property2) )" ) );
        assertThat( TestIndexDescriptorFactory.uniqueForLabel( 2, 4 ).userDescription( simpleNameLookup ),
                    equalTo( "Index( UNIQUE, :Label2(property4) )" ) );
    }
}
