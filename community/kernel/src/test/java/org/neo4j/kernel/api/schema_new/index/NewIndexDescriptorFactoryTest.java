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
package org.neo4j.kernel.api.schema_new.index;

import org.junit.Test;

import org.neo4j.kernel.api.schema_new.SchemaDescriptorFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.kernel.api.schema_new.SchemaTestUtil.assertEquality;
import static org.neo4j.kernel.api.schema_new.SchemaTestUtil.simpleNameLookup;

public class NewIndexDescriptorFactoryTest
{
    private static final int LABEL_ID = 0;

    @Test
    public void shouldCreateIndexDescriptors()
    {
        NewIndexDescriptor desc;

        desc = NewIndexDescriptorFactory.forLabel( LABEL_ID, 1 );
        assertThat( desc.type(), equalTo( NewIndexDescriptor.Type.GENERAL ) );
        assertThat( desc.schema(), equalTo( SchemaDescriptorFactory.forLabel( LABEL_ID, 1 ) ) );
    }

    @Test
    public void shouldCreateUniqueIndexDescriptors()
    {
        NewIndexDescriptor desc;

        desc = NewIndexDescriptorFactory.uniqueForLabel( LABEL_ID, 1 );
        assertThat( desc.type(), equalTo( NewIndexDescriptor.Type.UNIQUE ) );
        assertThat( desc.schema(), equalTo( SchemaDescriptorFactory.forLabel( LABEL_ID, 1 ) ) );
    }

    @Test
    public void shouldCreateEqualDescriptors()
    {
        NewIndexDescriptor desc1;
        NewIndexDescriptor desc2;
        desc1 = NewIndexDescriptorFactory.forLabel( LABEL_ID, 1 );
        desc2 = NewIndexDescriptorFactory.forLabel( LABEL_ID, 1 );
        assertEquality( desc1, desc2 );

        desc1 = NewIndexDescriptorFactory.uniqueForLabel( LABEL_ID, 1 );
        desc2 = NewIndexDescriptorFactory.uniqueForLabel( LABEL_ID, 1 );
        assertEquality( desc1, desc2 );
    }

    @Test
    public void shouldGiveNiceUserDescriptions()
    {
        assertThat( NewIndexDescriptorFactory.forLabel( 1, 2 ).userDescription( simpleNameLookup ),
                equalTo( "Index( GENERAL, :Label1(property2) )" ) );
        assertThat( NewIndexDescriptorFactory.uniqueForLabel( 2, 4 ).userDescription( simpleNameLookup ),
                equalTo( "Index( UNIQUE, :Label2(property4) )" ) );
    }
}
