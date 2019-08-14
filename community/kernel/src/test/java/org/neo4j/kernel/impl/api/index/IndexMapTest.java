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

import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;

class IndexMapTest
{
    private IndexMap indexMap;

    private LabelSchemaDescriptor schema3_4 = SchemaDescriptor.forLabel( 3, 4 );
    private LabelSchemaDescriptor schema5_6_7 = SchemaDescriptor.forLabel( 5, 6, 7 );
    private LabelSchemaDescriptor schema5_8 = SchemaDescriptor.forLabel( 5, 8 );
    private SchemaDescriptor node35_8 = SchemaDescriptor.fulltext( NODE, IndexConfig.empty(), new int[]{3, 5}, new int[]{8} );
    private SchemaDescriptor rel35_8 = SchemaDescriptor.fulltext( RELATIONSHIP, IndexConfig.empty(), new int[]{3, 5}, new int[]{8} );

    @BeforeEach
    void setup()
    {
        indexMap = new IndexMap();
        indexMap.putIndexProxy( new TestIndexProxy( forSchema( schema3_4 ).withName( "index_1" ).materialise( 1 ) ) );
        indexMap.putIndexProxy( new TestIndexProxy( forSchema( schema5_6_7 ).withName( "index_2" ).materialise( 2 ) ) );
        indexMap.putIndexProxy( new TestIndexProxy( forSchema( schema5_8 ).withName( "index_3" ).materialise( 3 ) ) );
        indexMap.putIndexProxy( new TestIndexProxy( forSchema( node35_8 ).withName( "index_4" ).materialise( 4 ) ) );
        indexMap.putIndexProxy( new TestIndexProxy( forSchema( rel35_8 ).withName( "index_5" ).materialise( 5 ) ) );
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

    private static class TestIndexProxy extends IndexProxyAdapter
    {
        private final IndexDescriptor descriptor;

        private TestIndexProxy( IndexDescriptor descriptor )
        {
            this.descriptor = descriptor;
        }

        @Override
        public IndexDescriptor getDescriptor()
        {
            return descriptor;
        }
    }
}
