package org.neo4j.perftest.enterprise.ccheck;

import java.util.Map;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.junit.internal.matchers.TypeSafeMatcher;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.perftest.enterprise.util.Configuration;

import static java.util.Arrays.asList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class DataGeneratorTest
{
    @Test
    public void shouldGenerateNodesAndRelationshipsWithProperties() throws Exception
    {
        // given
        Configuration.Builder config = Configuration.builder();
        config.setValue( DataGenerator.node_count, 5 );
        config.setValue( DataGenerator.relationships, asList( new DataGenerator.RelationshipSpec( "FOO", 1 ),
                                                              new DataGenerator.RelationshipSpec( "BAR", 2 ) ) );
        config.setValue( DataGenerator.node_properties,
                         asList( DataGenerator.PropertyGenerator.STRING, DataGenerator.PropertyGenerator.STRING ) );
        config.setValue( DataGenerator.relationship_properties, asList( DataGenerator.PropertyGenerator.STRING ) );

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
