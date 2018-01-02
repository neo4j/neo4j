/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.perftest.enterprise.generator;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.perftest.enterprise.util.Configuration;
import org.neo4j.unsafe.batchinsert.BatchInserter;

import java.util.Map;

import static java.util.Arrays.asList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.*;

public class DataGeneratorTest
{
    @Test
    public void shouldGenerateNodesAndRelationshipsWithProperties() throws Exception
    {
        // given
        Configuration.Builder config = Configuration.builder();
        config.setValue( DataGenerator.node_count, 5 );
        config.setValue( DataGenerator.relationships, asList( new RelationshipSpec( "FOO", 1 ),
                                                              new RelationshipSpec( "BAR", 2 ) ) );
        config.setValue( DataGenerator.node_properties,
                asList( new PropertySpec( PropertyGenerator.STRING, 2 ) ) );
        config.setValue( DataGenerator.relationship_properties,
                asList( new PropertySpec( PropertyGenerator.STRING, 1 ) ) );

        DataGenerator generator = new DataGenerator( config.build() );

        BatchInserter batchInserter = mock( BatchInserter.class );

        // when
        generator.generateData( batchInserter );

        // then
        verify( batchInserter, times( 5 ) ).createNode( argThat( hasSize( 2 ) ) );
        verify( batchInserter, times( 5 ) ).createRelationship( anyLong(), anyLong(), argThat( hasName( "FOO" ) ),
                                                                argThat( hasSize( 1 ) ) );
        verify( batchInserter, times( 10 ) )
                .createRelationship( anyLong(), anyLong(), argThat( hasName( "BAR" ) ), argThat( hasSize( 1 ) ) );
        verifyNoMoreInteractions( batchInserter );
    }

    private static Matcher<RelationshipType> hasName( final String name )
    {
        return new TypeSafeMatcher<RelationshipType>()
        {
            @Override
            public boolean matchesSafely( RelationshipType item )
            {
                return name.equals( item.name() );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "RelationshipType with name() == " ).appendValue( name );
            }
        };
    }

    private static Matcher<Map<String, Object>> hasSize( final int size )
    {
        return new TypeSafeMatcher<Map<String, Object>>()
        {
            @Override
            public boolean matchesSafely( Map<String, Object> item )
            {
                return item.size() == size;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "Map<String,Object> with size() == " ).appendValue( size );
            }
        };
    }
}
