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

import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.SIMPLE_NAME_LOOKUP;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.assertEquality;

class SchemaIndexDescriptorFactoryTest
{
    private static final int LABEL_ID = 0;

    @Test
    void shouldCreateIndexDescriptors()
    {
        IndexDescriptor desc;

        desc = TestIndexDescriptorFactory.forLabel( LABEL_ID, 1 );
        assertFalse( desc.isUnique() );
        assertThat( desc.schema() ).isEqualTo( SchemaDescriptor.forLabel( LABEL_ID, 1 ) );
    }

    @Test
    void shouldCreateUniqueIndexDescriptors()
    {
        IndexDescriptor desc;

        desc = TestIndexDescriptorFactory.uniqueForLabel( LABEL_ID, 1 );
        assertTrue( desc.isUnique() );
        assertThat( desc.schema() ).isEqualTo( SchemaDescriptor.forLabel( LABEL_ID, 1 ) );
    }

    @Test
    void shouldCreateIndexDescriptorsFromSchema()
    {
        IndexDescriptor desc;

        desc = TestIndexDescriptorFactory.forSchema( SchemaDescriptor.forLabel( LABEL_ID, 1 ) );
        assertFalse( desc.isUnique() );
        assertThat( desc.schema() ).isEqualTo( SchemaDescriptor.forLabel( LABEL_ID, 1 ) );

        desc = TestIndexDescriptorFactory.uniqueForSchema( SchemaDescriptor.forLabel( LABEL_ID, 1 ) );
        assertTrue( desc.isUnique() );
        assertThat( desc.schema() ).isEqualTo( SchemaDescriptor.forLabel( LABEL_ID, 1 ) );
    }

    @Test
    void shouldCreateEqualDescriptors()
    {
        IndexDescriptor desc1;
        IndexDescriptor desc2;
        desc1 = TestIndexDescriptorFactory.forLabel( LABEL_ID, 1 );
        desc2 = TestIndexDescriptorFactory.forLabel( LABEL_ID, 1 );
        assertEquality( desc1.schema(), desc2.schema() );
        assertEquality( desc1.isUnique(), desc2.isUnique() );

        desc1 = TestIndexDescriptorFactory.uniqueForLabel( LABEL_ID, 1 );
        desc2 = TestIndexDescriptorFactory.uniqueForLabel( LABEL_ID, 1 );
        assertEquality( desc1.schema(), desc2.schema() );
        assertEquality( desc1.isUnique(), desc2.isUnique() );
    }

    @Test
    void shouldGiveNiceUserDescriptions()
    {
        IndexDescriptor forLabel = TestIndexDescriptorFactory.forLabel( 1, 2 );
        long forLabelId = forLabel.getId();
        IndexDescriptor uniqueForLabel = TestIndexDescriptorFactory.uniqueForLabel( 2, 4 );
        String providerName = forLabel.getIndexProvider().name();
        long uniqueForLabelId = uniqueForLabel.getId();
        assertThat( forLabel.userDescription( SIMPLE_NAME_LOOKUP ) ).isEqualTo(
                "Index( " + forLabelId + ", 'index_" + forLabelId + "', GENERAL BTREE, :Label1(property2), " + providerName + " )" );
        assertThat( uniqueForLabel.userDescription( SIMPLE_NAME_LOOKUP ) ).isEqualTo(
                "Index( " + uniqueForLabelId + ", 'index_" + uniqueForLabelId + "', UNIQUE BTREE, :Label2(property4), " + providerName + " )" );
    }
}
