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

import org.junit.jupiter.api.Test;

import org.neo4j.common.EntityType;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.PropertySchemaType;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.values.storable.Values;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.assertArray;

class SchemaDescriptorTest
{
    private static final int REL_TYPE_ID = 0;
    private static final int LABEL_ID = 0;

    @Test
    void shouldCreateLabelDescriptors()
    {
        LabelSchemaDescriptor labelDesc;
        labelDesc = SchemaDescriptor.forLabel( LABEL_ID, 1 );
        assertThat( labelDesc.getLabelId(), equalTo( LABEL_ID ) );
        assertThat( labelDesc.getIndexType(), is( IndexType.ANY_GENERAL ) );
        assertThat( labelDesc.entityType(), is( EntityType.NODE ) );
        assertThat( labelDesc.propertySchemaType(), is( PropertySchemaType.COMPLETE_ALL_TOKENS ) );
        assertArray( labelDesc.getPropertyIds(), 1 );

        labelDesc = SchemaDescriptor.forLabel( LABEL_ID, 1, 2, 3 );
        assertThat( labelDesc.getLabelId(), equalTo( LABEL_ID ) );
        assertArray( labelDesc.getPropertyIds(), 1, 2, 3 );
    }

    @Test
    void shouldCreateRelTypeDescriptors()
    {
        RelationTypeSchemaDescriptor relTypeDesc;
        relTypeDesc = SchemaDescriptor.forRelType( REL_TYPE_ID, 1 );
        assertThat( relTypeDesc.getRelTypeId(), equalTo( REL_TYPE_ID ) );
        assertThat( relTypeDesc.getIndexType(), is( IndexType.ANY_GENERAL ) );
        assertThat( relTypeDesc.entityType(), is( EntityType.RELATIONSHIP ) );
        assertThat( relTypeDesc.propertySchemaType(), is( PropertySchemaType.COMPLETE_ALL_TOKENS ) );
        assertArray( relTypeDesc.getPropertyIds(), 1 );

        relTypeDesc = SchemaDescriptor.forRelType( REL_TYPE_ID, 1, 2, 3 );
        assertThat( relTypeDesc.getRelTypeId(), equalTo( REL_TYPE_ID ) );
        assertArray( relTypeDesc.getPropertyIds(), 1, 2, 3 );
    }

    @Test
    void shouldCreateEqualLabels()
    {
        LabelSchemaDescriptor desc1 = SchemaDescriptor.forLabel( LABEL_ID, 1 );
        LabelSchemaDescriptor desc2 = SchemaDescriptor.forLabel( LABEL_ID, 1 );
        SchemaTestUtil.assertEquality( desc1, desc2 );
    }

    @Test
    void shouldCreateEqualRelTypes()
    {
        RelationTypeSchemaDescriptor desc1 = SchemaDescriptor.forRelType( REL_TYPE_ID, 1 );
        RelationTypeSchemaDescriptor desc2 = SchemaDescriptor.forRelType( REL_TYPE_ID, 1 );
        SchemaTestUtil.assertEquality( desc1, desc2 );
    }

    @Test
    void shouldGiveNiceUserDescriptions()
    {
        assertThat( SchemaDescriptor.forLabel( 1, 2 ).userDescription( SchemaTestUtil.SIMPLE_NAME_LOOKUP ),
                equalTo( ":Label1(property2)" ) );
        assertThat( SchemaDescriptor.forRelType( 1, 3 ).userDescription( SchemaTestUtil.SIMPLE_NAME_LOOKUP ),
                equalTo( "-[:RelType1(property3)]-" ) );
    }

    @Test
    void updatingIndexConfigLeavesOriginalDescriptorUntouched()
    {
        LabelSchemaDescriptor a = SchemaDescriptor.forLabel( 1, 2, 3 );
        LabelSchemaDescriptor aa = SchemaDescriptor.forLabel( 1, 2, 3 );
        LabelSchemaDescriptor b = a.withIndexConfig( a.getIndexConfig().withIfAbsent( "x", Values.stringValue( "y" ) ) );

        assertThat( a.getIndexConfig(), not( equalTo( b.getIndexConfig() ) ) );
        assertThat( a, equalTo( b ) );
        assertThat( a, equalTo( aa ) );
        assertThat( a.getIndexConfig(), equalTo( aa.getIndexConfig() ) );
        assertThat( b.getIndexConfig().get( "x" ), equalTo( Values.stringValue( "y" ) ) );
        assertThat( a.getIndexConfig().get( "x" ), is( nullValue() ) );
    }
}
