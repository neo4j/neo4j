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
package org.neo4j.kernel.api.schema;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.assertArray;

public class SchemaDescriptorFactoryTest
{

    private static final int REL_TYPE_ID = 0;
    private static final int LABEL_ID = 0;

    @Test
    public void shouldCreateLabelDescriptors()
    {
        LabelSchemaDescriptor labelDesc;
        labelDesc = SchemaDescriptorFactory.forLabel( LABEL_ID, 1 );
        assertThat( labelDesc.getLabelId(), equalTo( LABEL_ID ) );
        assertArray( labelDesc.getPropertyIds(), 1 );

        labelDesc = SchemaDescriptorFactory.forLabel( LABEL_ID, 1, 2, 3 );
        assertThat( labelDesc.getLabelId(), equalTo( LABEL_ID ) );
        SchemaTestUtil.assertArray( labelDesc.getPropertyIds(), 1, 2, 3 );

        labelDesc = SchemaDescriptorFactory.forLabel( LABEL_ID );
        assertThat( labelDesc.getLabelId(), equalTo( LABEL_ID ) );
        SchemaTestUtil.assertArray( labelDesc.getPropertyIds() );
    }

    @Test
    public void shouldCreateRelTypeDescriptors()
    {
        RelationTypeSchemaDescriptor relTypeDesc;
        relTypeDesc = SchemaDescriptorFactory.forRelType( REL_TYPE_ID, 1 );
        assertThat( relTypeDesc.getRelTypeId(), equalTo( REL_TYPE_ID ) );
        assertArray( relTypeDesc.getPropertyIds(), 1 );

        relTypeDesc = SchemaDescriptorFactory.forRelType( REL_TYPE_ID, 1, 2, 3 );
        assertThat( relTypeDesc.getRelTypeId(), equalTo( REL_TYPE_ID ) );
        SchemaTestUtil.assertArray( relTypeDesc.getPropertyIds(), 1, 2, 3 );

        relTypeDesc = SchemaDescriptorFactory.forRelType( REL_TYPE_ID );
        assertThat( relTypeDesc.getRelTypeId(), equalTo( REL_TYPE_ID ) );
        SchemaTestUtil.assertArray( relTypeDesc.getPropertyIds() );
    }

    @Test
    public void shouldCreateEqualLabels()
    {
        LabelSchemaDescriptor desc1 = SchemaDescriptorFactory.forLabel( LABEL_ID, 1 );
        LabelSchemaDescriptor desc2 = SchemaDescriptorFactory.forLabel( LABEL_ID, 1 );
        SchemaTestUtil.assertEquality( desc1, desc2 );
    }

    @Test
    public void shouldCreateEqualRelTypes()
    {
        RelationTypeSchemaDescriptor desc1 = SchemaDescriptorFactory.forRelType( REL_TYPE_ID, 1 );
        RelationTypeSchemaDescriptor desc2 = SchemaDescriptorFactory.forRelType( REL_TYPE_ID, 1 );
        SchemaTestUtil.assertEquality( desc1, desc2 );
    }

    @Test
    public void shouldGiveNiceUserDescriptions()
    {
        assertThat( SchemaDescriptorFactory.forLabel( 1, 2 ).userDescription( SchemaTestUtil.simpleNameLookup ),
                equalTo( ":Label1(property2)" ) );
        assertThat( SchemaDescriptorFactory.forRelType( 1, 3 ).userDescription( SchemaTestUtil.simpleNameLookup ),
                equalTo( "-[:RelType1(property3)]-" ) );
    }
}
