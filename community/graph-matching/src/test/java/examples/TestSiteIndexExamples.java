/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package examples;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Set;

import org.junit.ClassRule;
import org.junit.Test;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphmatching.CommonValueMatchers;
import org.neo4j.graphmatching.PatternMatch;
import org.neo4j.graphmatching.PatternMatcher;
import org.neo4j.graphmatching.PatternNode;
import org.neo4j.graphmatching.PatternRelationship;
import org.neo4j.graphmatching.ValueMatcher;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Example code for the index page of the component site.
 *
 * @author Tobias Ivarsson
 */
public class TestSiteIndexExamples
{
    @ClassRule
    public static EmbeddedDatabaseRule graphDb = new EmbeddedDatabaseRule();

    // START SNIPPET: findNodesWithRelationshipsTo
    public static Iterable<Node> findNodesWithRelationshipsTo(
            RelationshipType type, Node... nodes )
    {
        if ( nodes == null || nodes.length == 0 )
        {
            throw new IllegalArgumentException( "No nodes supplied" );
        }
        final PatternNode requested = new PatternNode();
        PatternNode anchor = null;
        for ( Node node : nodes )
        {
            PatternNode pattern = new PatternNode();
            pattern.setAssociation( node );
            pattern.createRelationshipTo( requested, type );
            if ( anchor == null )
            {
                anchor = pattern;
            }
        }
        PatternMatcher matcher = PatternMatcher.getMatcher();
        Iterable<PatternMatch> matches = matcher.match( anchor, nodes[0] );
        return new IterableWrapper<Node, PatternMatch>( matches )
        {
            @Override
            protected Node underlyingObjectToObject( PatternMatch match )
            {
                return match.getNodeFor( requested );
            }
        };
    }
    // END SNIPPET: findNodesWithRelationshipsTo

    // START SNIPPET: findFriends
    private static final long MILLSECONDS_PER_DAY = 1000 * 60 * 60 * 24;

    enum FriendshipTypes implements RelationshipType
    {
        FRIEND,
        LIVES_IN
    }

    /**
     * Find all friends the specified person has known for more than the
     * specified number of years.
     *
     * @param me the node to find the friends of.
     * @param livesIn The name of the place where the friends should live.
     * @param knownForYears the minimum age (in years) of the friendship.
     * @return all nodes that live in the specified place that the specified
     *         nodes has known for the specified number of years.
     */
    public Iterable<Node> findFriendsSinceSpecifiedTimeInSpecifiedPlace(
            Node me, String livesIn,
            final int knownForYears )
    {
        PatternNode root = new PatternNode(), place = new PatternNode();
        final PatternNode friend = new PatternNode();
        // Define the friendship
        PatternRelationship friendship = root.createRelationshipTo( friend,
                FriendshipTypes.FRIEND, Direction.BOTH );
        // Define the age of the friendship
        friendship.addPropertyConstraint( "since", new ValueMatcher()
        {
            long now = new Date().getTime();

            @Override
            public boolean matches( Object value )
            {
                if ( value instanceof Long )
                {
                    long ageInDays = ( now - (Long) value )
                                     / MILLSECONDS_PER_DAY;
                    return ageInDays > ( knownForYears * 365 );
                }
                return false;
            }
        } );
        // Define the place where the friend lives
        friend.createRelationshipTo( place, FriendshipTypes.LIVES_IN );
        place.addPropertyConstraint( "name",
                CommonValueMatchers.exact( livesIn ) );
        // Perform the matching
        PatternMatcher matcher = PatternMatcher.getMatcher();
        Iterable<PatternMatch> matches = matcher.match( root, me );
        // Return the result
        return new IterableWrapper<Node, PatternMatch>( matches )
        {
            @Override
            protected Node underlyingObjectToObject( PatternMatch match )
            {
                return match.getNodeFor( friend );
            }
        };
    }

    // END SNIPPET: findFriends

    @Test
    public void verifyFunctionalityOfFindNodesWithRelationshipsTo()
            throws Exception
    {
        final RelationshipType type = DynamicRelationshipType.withName( "RELATED" );
        Node[] nodes = createGraph( new GraphDefinition<Node[]>()
        {
            @Override
            public Node[] create( GraphDatabaseService graphdb )
            {
                Node[] nodes = new Node[5];
                for ( int i = 0; i < nodes.length; i++ )
                {
                    nodes[i] = graphdb.createNode();
                }
                for ( int i = 0; i < 3; i++ )
                {
                    Node node = graphdb.createNode();
                    for ( int j = 0; j < nodes.length; j++ )
                    {
                        nodes[j].createRelationshipTo( node, type );
                    }
                }
                return nodes;
            }
        } );
        try ( Transaction tx = graphDb.getGraphDatabaseService().beginTx() )
        {
            assertEquals( 3, count( findNodesWithRelationshipsTo( type, nodes ) ) );
            tx.success();
        }
    }

    @Test
    public void verifyFunctionalityOfFindFriendsSinceSpecifiedTimeInSpecifiedPlace()
            throws Exception
    {
        Node root = createGraph( new GraphDefinition<Node>()
        {
            @Override
            public Node create( GraphDatabaseService graphdb )
            {
                Node me = graphdb.createNode();
                Node stockholm = graphdb.createNode(), gothenburg = graphdb.createNode();
                stockholm.setProperty( "name", "Stockholm" );
                gothenburg.setProperty( "name", "Gothenburg" );

                Node andy = friend( me, graphdb.createNode(), "Andy", 10,
                        stockholm );
                friend( me, graphdb.createNode(), "Bob", 5, stockholm );
                Node cecilia = friend( me, graphdb.createNode(), "Cecilia", 2,
                        stockholm );
                andy.createRelationshipTo( cecilia, FriendshipTypes.FRIEND ).setProperty(
                        "since", yearsAgo( 10 ) );
                friend( me, graphdb.createNode(), "David", 10, gothenburg );

                return me;
            }

            Node friend( Node me, Node friend, String name, int knownForYears,
                    Node place )
            {
                friend.setProperty( "name", name );
                me.createRelationshipTo( friend, FriendshipTypes.FRIEND ).setProperty(
                        "since", yearsAgo( knownForYears ) );
                friend.createRelationshipTo( place, FriendshipTypes.LIVES_IN );
                return friend;
            }

            Calendar calendar = Calendar.getInstance();

            long yearsAgo( int years )
            {
                return new GregorianCalendar( calendar.get( Calendar.YEAR )
                                              - years,
                        calendar.get( Calendar.MONTH ),
                        calendar.get( Calendar.DATE ) ).getTime().getTime();
            }
        } );

        Set<String> expected = new HashSet<>( Arrays.asList( "Andy", "Bob" ) );
        Iterable<Node> friends = findFriendsSinceSpecifiedTimeInSpecifiedPlace( root, "Stockholm", 3 );
        
        try ( Transaction transaction = graphDb.getGraphDatabaseService().beginTx() )
        {
            for ( Node friend : friends )
            {
                String name = (String) friend.getProperty( "name", null );
                assertNotNull( name );
                assertTrue( "Unexpected friend: " + name, expected.remove( name ) );
            }
            assertTrue( "These friends were not found: " + expected, expected.isEmpty() );
        }
    }

    private int count( Iterable<?> objects )
    {
        int count = 0;
        for ( @SuppressWarnings( "unused" ) Object object : objects )
        {
            count++;
        }
        return count;
    }

    private interface GraphDefinition<RESULT>
    {
        RESULT create( GraphDatabaseService graphdb );
    }

    private <T> T createGraph( GraphDefinition<T> definition )
    {
        try ( Transaction tx = graphDb.getGraphDatabaseService().beginTx() )
        {
            T result = definition.create( graphDb.getGraphDatabaseService() );
            tx.success();
            return result;
        }
    }
}
