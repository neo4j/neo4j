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

import org.junit.Test;

import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.assertEquality;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.simpleNameLookup;

public class SchemaIndexDescriptorFactoryTest
{
    private static final int LABEL_ID = 0;

    @Test
    public void shouldCreateIndexDescriptors()
    {
        SchemaIndexDescriptor desc;

        desc = SchemaIndexDescriptorFactory.forLabel( LABEL_ID, 1 );
        assertThat( desc.type(), equalTo( SchemaIndexDescriptor.Type.GENERAL ) );
        assertThat( desc.schema(), equalTo( SchemaDescriptorFactory.forLabel( LABEL_ID, 1 ) ) );
    }

    @Test
    public void shouldCreateUniqueIndexDescriptors()
    {
        SchemaIndexDescriptor desc;

        desc = SchemaIndexDescriptorFactory.uniqueForLabel( LABEL_ID, 1 );
        assertThat( desc.type(), equalTo( SchemaIndexDescriptor.Type.UNIQUE ) );
        assertThat( desc.schema(), equalTo( SchemaDescriptorFactory.forLabel( LABEL_ID, 1 ) ) );
    }

    @Test
    public void shouldCreateIndexDescriptorsFromSchema()
    {
        SchemaIndexDescriptor desc;

        desc = SchemaIndexDescriptorFactory.forSchema( SchemaDescriptorFactory.forLabel( LABEL_ID, 1 ) );
        assertThat( desc.type(), equalTo( SchemaIndexDescriptor.Type.GENERAL ) );
        assertThat( desc.schema(), equalTo( SchemaDescriptorFactory.forLabel( LABEL_ID, 1 ) ) );

        desc = SchemaIndexDescriptorFactory.uniqueForSchema( SchemaDescriptorFactory.forLabel( LABEL_ID, 1 ) );
        assertThat( desc.type(), equalTo( SchemaIndexDescriptor.Type.UNIQUE) );
        assertThat( desc.schema(), equalTo( SchemaDescriptorFactory.forLabel( LABEL_ID, 1 ) ) );
    }

    @Test
    public void shouldCreateEqualDescriptors()
    {
        SchemaIndexDescriptor desc1;
        SchemaIndexDescriptor desc2;
        desc1 = SchemaIndexDescriptorFactory.forLabel( LABEL_ID, 1 );
        desc2 = SchemaIndexDescriptorFactory.forLabel( LABEL_ID, 1 );
        assertEquality( desc1, desc2 );

        desc1 = SchemaIndexDescriptorFactory.uniqueForLabel( LABEL_ID, 1 );
        desc2 = SchemaIndexDescriptorFactory.uniqueForLabel( LABEL_ID, 1 );
        assertEquality( desc1, desc2 );
    }

    @Test
    public void shouldGiveNiceUserDescriptions()
    {
        assertThat( SchemaIndexDescriptorFactory.forLabel( 1, 2 ).userDescription( simpleNameLookup ),
                equalTo( "Index( GENERAL, :Label1(property2) )" ) );
        assertThat( SchemaIndexDescriptorFactory.uniqueForLabel( 2, 4 ).userDescription( simpleNameLookup ),
                equalTo( "Index( UNIQUE, :Label2(property4) )" ) );
    }
}
