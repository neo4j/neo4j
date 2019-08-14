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
package org.neo4j.internal.schema;

import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

class IndexDescriptorTest
{
    private static final SchemaDescriptor[] SCHEMAS = {
            SchemaDescriptor.fulltext( EntityType.NODE, IndexConfig.empty(), new int[] {1}, new int[] {1} ),
            SchemaDescriptor.fulltext( EntityType.NODE, IndexConfig.empty(), new int[] {1}, new int[] {2} ),
            SchemaDescriptor.fulltext( EntityType.NODE, IndexConfig.empty(), new int[] {2}, new int[] {1} ),
            SchemaDescriptor.fulltext( EntityType.NODE, IndexConfig.empty(), new int[] {1, 1}, new int[] {1} ),
            SchemaDescriptor.fulltext( EntityType.NODE, IndexConfig.empty(), new int[] {1}, new int[] {1, 1} ),
            SchemaDescriptor.fulltext( EntityType.NODE, IndexConfig.empty(), new int[] {1, 1}, new int[] {1, 1} ),
            SchemaDescriptor.fulltext( EntityType.NODE, IndexConfig.empty(), new int[] {1, 2}, new int[] {1, 1} ),
            SchemaDescriptor.fulltext( EntityType.NODE, IndexConfig.empty(), new int[] {1, 1}, new int[] {1, 2} ),
            SchemaDescriptor.fulltext( EntityType.NODE, IndexConfig.empty(), new int[] {1, 2}, new int[] {1, 2} ),

            SchemaDescriptor.fulltext( EntityType.RELATIONSHIP, IndexConfig.empty(), new int[] {1}, new int[] {1} ),
            SchemaDescriptor.fulltext( EntityType.RELATIONSHIP, IndexConfig.empty(), new int[] {1}, new int[] {2} ),
            SchemaDescriptor.fulltext( EntityType.RELATIONSHIP, IndexConfig.empty(), new int[] {2}, new int[] {1} ),
            SchemaDescriptor.fulltext( EntityType.RELATIONSHIP, IndexConfig.empty(), new int[] {1, 1}, new int[] {1} ),
            SchemaDescriptor.fulltext( EntityType.RELATIONSHIP, IndexConfig.empty(), new int[] {1}, new int[] {1, 1} ),
            SchemaDescriptor.fulltext( EntityType.RELATIONSHIP, IndexConfig.empty(), new int[] {1, 1}, new int[] {1, 1} ),
            SchemaDescriptor.fulltext( EntityType.RELATIONSHIP, IndexConfig.empty(), new int[] {1, 2}, new int[] {1, 1} ),
            SchemaDescriptor.fulltext( EntityType.RELATIONSHIP, IndexConfig.empty(), new int[] {1, 1}, new int[] {1, 2} ),
            SchemaDescriptor.fulltext( EntityType.RELATIONSHIP, IndexConfig.empty(), new int[] {1, 2}, new int[] {1, 2} ),

            SchemaDescriptor.forLabelOfType( IndexType.FULLTEXT, 1, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.FULLTEXT, 1, 2 ),
            SchemaDescriptor.forLabelOfType( IndexType.FULLTEXT, 2, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.FULLTEXT, 2, 2 ),
            SchemaDescriptor.forLabelOfType( IndexType.FULLTEXT, 1, 1, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.FULLTEXT, 1, 1, 2 ),
            SchemaDescriptor.forLabelOfType( IndexType.FULLTEXT, 1, 2, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.FULLTEXT, 1, 2, 2 ),
            SchemaDescriptor.forLabelOfType( IndexType.FULLTEXT, 2, 1, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.FULLTEXT, 2, 2, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.FULLTEXT, 2, 1, 2 ),
            SchemaDescriptor.forLabelOfType( IndexType.FULLTEXT, 2, 2, 2 ),

            SchemaDescriptor.forLabelOfType( IndexType.ANY_GENERAL, 1, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.ANY_GENERAL, 1, 2 ),
            SchemaDescriptor.forLabelOfType( IndexType.ANY_GENERAL, 2, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.ANY_GENERAL, 2, 2 ),
            SchemaDescriptor.forLabelOfType( IndexType.ANY_GENERAL, 1, 1, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.ANY_GENERAL, 1, 1, 2 ),
            SchemaDescriptor.forLabelOfType( IndexType.ANY_GENERAL, 1, 2, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.ANY_GENERAL, 1, 2, 2 ),
            SchemaDescriptor.forLabelOfType( IndexType.ANY_GENERAL, 2, 1, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.ANY_GENERAL, 2, 2, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.ANY_GENERAL, 2, 1, 2 ),
            SchemaDescriptor.forLabelOfType( IndexType.ANY_GENERAL, 2, 2, 2 ),

            SchemaDescriptor.forLabelOfType( IndexType.TREE, 1, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.TREE, 1, 2 ),
            SchemaDescriptor.forLabelOfType( IndexType.TREE, 2, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.TREE, 2, 2 ),
            SchemaDescriptor.forLabelOfType( IndexType.TREE, 1, 1, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.TREE, 1, 1, 2 ),
            SchemaDescriptor.forLabelOfType( IndexType.TREE, 1, 2, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.TREE, 1, 2, 2 ),
            SchemaDescriptor.forLabelOfType( IndexType.TREE, 2, 1, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.TREE, 2, 2, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.TREE, 2, 1, 2 ),
            SchemaDescriptor.forLabelOfType( IndexType.TREE, 2, 2, 2 ),

            SchemaDescriptor.forLabelOfType( IndexType.INVERTED, 1, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.INVERTED, 1, 2 ),
            SchemaDescriptor.forLabelOfType( IndexType.INVERTED, 2, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.INVERTED, 2, 2 ),
            SchemaDescriptor.forLabelOfType( IndexType.INVERTED, 1, 1, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.INVERTED, 1, 1, 2 ),
            SchemaDescriptor.forLabelOfType( IndexType.INVERTED, 1, 2, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.INVERTED, 1, 2, 2 ),
            SchemaDescriptor.forLabelOfType( IndexType.INVERTED, 2, 1, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.INVERTED, 2, 2, 1 ),
            SchemaDescriptor.forLabelOfType( IndexType.INVERTED, 2, 1, 2 ),
            SchemaDescriptor.forLabelOfType( IndexType.INVERTED, 2, 2, 2 ),

            SchemaDescriptor.forRelTypeOfType( IndexType.FULLTEXT, 1, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.FULLTEXT, 1, 2 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.FULLTEXT, 2, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.FULLTEXT, 2, 2 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.FULLTEXT, 1, 1, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.FULLTEXT, 1, 1, 2 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.FULLTEXT, 1, 2, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.FULLTEXT, 1, 2, 2 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.FULLTEXT, 2, 1, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.FULLTEXT, 2, 2, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.FULLTEXT, 2, 1, 2 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.FULLTEXT, 2, 2, 2 ),

            SchemaDescriptor.forRelTypeOfType( IndexType.ANY_GENERAL, 1, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.ANY_GENERAL, 1, 2 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.ANY_GENERAL, 2, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.ANY_GENERAL, 2, 2 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.ANY_GENERAL, 1, 1, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.ANY_GENERAL, 1, 1, 2 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.ANY_GENERAL, 1, 2, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.ANY_GENERAL, 1, 2, 2 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.ANY_GENERAL, 2, 1, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.ANY_GENERAL, 2, 2, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.ANY_GENERAL, 2, 1, 2 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.ANY_GENERAL, 2, 2, 2 ),

            SchemaDescriptor.forRelTypeOfType( IndexType.TREE, 1, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.TREE, 1, 2 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.TREE, 2, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.TREE, 2, 2 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.TREE, 1, 1, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.TREE, 1, 1, 2 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.TREE, 1, 2, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.TREE, 1, 2, 2 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.TREE, 2, 1, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.TREE, 2, 2, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.TREE, 2, 1, 2 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.TREE, 2, 2, 2 ),

            SchemaDescriptor.forRelTypeOfType( IndexType.INVERTED, 1, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.INVERTED, 1, 2 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.INVERTED, 2, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.INVERTED, 2, 2 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.INVERTED, 1, 1, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.INVERTED, 1, 1, 2 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.INVERTED, 1, 2, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.INVERTED, 1, 2, 2 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.INVERTED, 2, 1, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.INVERTED, 2, 2, 1 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.INVERTED, 2, 1, 2 ),
            SchemaDescriptor.forRelTypeOfType( IndexType.INVERTED, 2, 2, 2 ),
    };

    @Test
    void indexDescriptorsShouldEqualTheirPrototypes()
    {
        IndexPrototype[] prototypes = Stream.of( SCHEMAS )
                .flatMap( schema -> Stream.of( IndexPrototype.forSchema( schema ), IndexPrototype.uniqueForSchema( schema ) ) )
                .toArray( IndexPrototype[]::new );

        for ( int i = 0; i < prototypes.length; i++ )
        {
            IndexRef<?> prototype = prototypes[i];
            IndexRef<?> index = prototypes[i].withName( "index_" + i ).materialise( 0 );
            if ( !index.equals( prototype ) )
            {
                fail( index + " was not equal to its prototype " + prototype + " at index (" + i + ", _)");
            }
            for ( int j = i + 1; j < prototypes.length; j++ )
            {
                prototype = prototypes[j];
                if ( index.equals( prototype ) )
                {
                    fail( index + " was equal to a different prototype " + prototype + " at index (" + i + ", " + j + ")");
                }
            }
        }
    }

    @Test
    void mustThrowWhenCreatingIndexNamedAfterNoIndexName()
    {
        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class, () ->
        {
            IndexPrototype prototype = IndexPrototype.forSchema( SCHEMAS[0] );
            prototype = prototype.withName( IndexDescriptor.NO_INDEX.getName() );
            prototype.materialise( 0 );
        } );
        assertThat( exception.getMessage(), containsString( IndexDescriptor.NO_INDEX.getName() ) );
    }

    @Test
    void toStringMustIncludeSchemaDescription()
    {
        IndexPrototype prototype = IndexPrototype.forSchema( SCHEMAS[0] );
        IndexDescriptor index = prototype.withName( "index" ).materialise( 1 );
        String schemaDescription = SCHEMAS[0].userDescription( TokenNameLookup.idTokenNameLookup );
        assertThat( prototype.toString(), containsString( schemaDescription ) );
        assertThat( index.toString(), containsString( schemaDescription ) );
    }
}
