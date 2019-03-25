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
package org.neo4j.kernel.impl.api.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.impl.index.schema.CapableIndexDescriptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.kernel.impl.index.schema.IndexDescriptorFactory.forSchema;

class IndexMapTest
{
    private static final long[] noEntityToken = {};
    private IndexMap indexMap;

    private LabelSchemaDescriptor schema3_4 = SchemaDescriptorFactory.forLabel( 3, 4 );
    private LabelSchemaDescriptor schema5_6_7 = SchemaDescriptorFactory.forLabel( 5, 6, 7 );
    private LabelSchemaDescriptor schema5_8 = SchemaDescriptorFactory.forLabel( 5, 8 );
    private SchemaDescriptor node35_8 = SchemaDescriptorFactory.multiToken( new int[] {3,5}, NODE, 8 );
    private SchemaDescriptor rel35_8 = SchemaDescriptorFactory.multiToken( new int[] {3,5}, RELATIONSHIP, 8 );

    @BeforeEach
    void setup()
    {
        indexMap = new IndexMap();
        indexMap.putIndexProxy( new TestIndexProxy( forSchema( schema3_4 ).withId( 1 ).withoutCapabilities() ) );
        indexMap.putIndexProxy( new TestIndexProxy( forSchema( schema5_6_7 ).withId( 2 ).withoutCapabilities() ) );
        indexMap.putIndexProxy( new TestIndexProxy( forSchema( schema5_8 ).withId( 3 ).withoutCapabilities() ) );
        indexMap.putIndexProxy( new TestIndexProxy( forSchema( node35_8 ).withId( 4 ).withoutCapabilities() ) );
        indexMap.putIndexProxy( new TestIndexProxy( forSchema( rel35_8 ).withId( 5 ).withoutCapabilities() ) );
    }

    @Test
    void shouldGetById()
    {
        assertEquals( schema3_4, indexMap.getIndexProxy( 1L ).getDescriptor().schema() );
        assertEquals( schema5_6_7, indexMap.getIndexProxy( 2L ).getDescriptor().schema() );
    }

    @Test
    void shouldGetByDescriptor()
    {
        assertEquals( schema5_8, indexMap.getIndexProxy( schema5_8 ).getDescriptor().schema() );
        assertEquals( node35_8, indexMap.getIndexProxy( node35_8 ).getDescriptor().schema() );
    }

    // HELPERS

    private class TestIndexProxy extends IndexProxyAdapter
    {
        private final CapableIndexDescriptor descriptor;

        private TestIndexProxy( CapableIndexDescriptor descriptor )
        {
            this.descriptor = descriptor;
        }

        @Override
        public CapableIndexDescriptor getDescriptor()
        {
            return descriptor;
        }
    }
}
