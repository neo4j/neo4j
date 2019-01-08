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
package org.neo4j.kernel.api.schema.constaints;

import org.junit.Test;

import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.assertEquality;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.simpleNameLookup;

public class ConstraintDescriptorFactoryTest
{
    private static final int LABEL_ID = 0;
    private static final int REL_TYPE_ID = 0;

    @Test
    public void shouldCreateExistsConstraintDescriptors()
    {
        ConstraintDescriptor desc;

        desc = ConstraintDescriptorFactory.existsForLabel( LABEL_ID, 1 );
        assertThat( desc.type(), equalTo( ConstraintDescriptor.Type.EXISTS ) );
        assertThat( desc.schema(), equalTo( SchemaDescriptorFactory.forLabel( LABEL_ID, 1 ) ) );

        desc = ConstraintDescriptorFactory.existsForRelType( REL_TYPE_ID, 1 );
        assertThat( desc.type(), equalTo( ConstraintDescriptor.Type.EXISTS ) );
        assertThat( desc.schema(), equalTo( SchemaDescriptorFactory.forRelType( REL_TYPE_ID, 1 ) ) );
    }

    @Test
    public void shouldCreateUniqueConstraintDescriptors()
    {
        ConstraintDescriptor desc;

        desc = ConstraintDescriptorFactory.uniqueForLabel( LABEL_ID, 1 );
        assertThat( desc.type(), equalTo( ConstraintDescriptor.Type.UNIQUE ) );
        assertThat( desc.schema(), equalTo( SchemaDescriptorFactory.forLabel( LABEL_ID, 1 ) ) );
    }

    @Test
    public void shouldCreateNodeKeyConstraintDescriptors()
    {
        ConstraintDescriptor desc;

        desc = ConstraintDescriptorFactory.nodeKeyForLabel( LABEL_ID, 1 );
        assertThat( desc.type(), equalTo( ConstraintDescriptor.Type.UNIQUE_EXISTS ) );
        assertThat( desc.schema(), equalTo( SchemaDescriptorFactory.forLabel( LABEL_ID, 1 ) ) );
    }

    @Test
    public void shouldCreateConstraintDescriptorsFromSchema()
    {
        ConstraintDescriptor desc;

        desc = ConstraintDescriptorFactory.uniqueForSchema( SchemaDescriptorFactory.forLabel( LABEL_ID, 1 ) );
        assertThat( desc.type(), equalTo( ConstraintDescriptor.Type.UNIQUE ) );
        assertThat( desc.schema(), equalTo( SchemaDescriptorFactory.forLabel( LABEL_ID, 1 ) ) );

        desc = ConstraintDescriptorFactory.nodeKeyForSchema( SchemaDescriptorFactory.forLabel( LABEL_ID, 1 ) );
        assertThat( desc.type(), equalTo( ConstraintDescriptor.Type.UNIQUE_EXISTS ) );
        assertThat( desc.schema(), equalTo( SchemaDescriptorFactory.forLabel( LABEL_ID, 1 ) ) );

        desc = ConstraintDescriptorFactory.existsForSchema( SchemaDescriptorFactory.forRelType( REL_TYPE_ID, 1 ) );
        assertThat( desc.type(), equalTo( ConstraintDescriptor.Type.EXISTS) );
        assertThat( desc.schema(), equalTo( SchemaDescriptorFactory.forRelType( REL_TYPE_ID, 1 ) ) );
    }

    @Test
    public void shouldCreateEqualDescriptors()
    {
        ConstraintDescriptor desc1;
        ConstraintDescriptor desc2;

        desc1 = ConstraintDescriptorFactory.uniqueForLabel( LABEL_ID, 1 );
        desc2 = ConstraintDescriptorFactory.uniqueForLabel( LABEL_ID, 1 );
        assertEquality( desc1, desc2 );

        desc1 = ConstraintDescriptorFactory.existsForLabel( LABEL_ID, 1 );
        desc2 = ConstraintDescriptorFactory.existsForLabel( LABEL_ID, 1 );
        assertEquality( desc1, desc2 );

        desc1 = ConstraintDescriptorFactory.existsForRelType( LABEL_ID, 1 );
        desc2 = ConstraintDescriptorFactory.existsForRelType( LABEL_ID, 1 );
        assertEquality( desc1, desc2 );
    }

    @Test
    public void shouldGiveNiceUserDescriptions()
    {
        assertThat( ConstraintDescriptorFactory.existsForLabel( 1, 2 ).userDescription( simpleNameLookup ),
                equalTo( "Constraint( EXISTS, :Label1(property2) )" ) );
        assertThat( ConstraintDescriptorFactory.existsForRelType( 1, 3 ).userDescription( simpleNameLookup ),
                equalTo( "Constraint( EXISTS, -[:RelType1(property3)]- )" ) );
        assertThat( ConstraintDescriptorFactory.uniqueForLabel( 2, 4 ).userDescription( simpleNameLookup ),
                equalTo( "Constraint( UNIQUE, :Label2(property4) )" ) );
    }
}
