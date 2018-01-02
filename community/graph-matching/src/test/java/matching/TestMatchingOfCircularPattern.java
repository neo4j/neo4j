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
package matching;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TraversalPosition;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.graphmatching.PatternMatch;
import org.neo4j.graphmatching.PatternMatcher;
import org.neo4j.graphmatching.PatternNode;
import org.neo4j.helpers.collection.IteratorWrapper;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

public class TestMatchingOfCircularPattern
{
    static private final boolean STATIC_PATTERN = false;

    private static class VisibleMessagesByFollowedUsers implements
            Iterable<Node>
    {
        private final PatternNode start = new PatternNode();
        private final PatternNode message = new PatternNode();

        private final Node startNode;

        public VisibleMessagesByFollowedUsers( Node startNode )
        {
            this.startNode = startNode;
            if ( !STATIC_PATTERN )
            { start.setAssociation( startNode ); }
            PatternNode user = new PatternNode();
            start.createRelationshipTo( user, withName( "FOLLOWS" ) );
            user.createRelationshipTo( message, withName( "CREATED" ) );
            message.createRelationshipTo( start, withName( "IS_VISIBLE_BY" ) );
        }

        @Override
        public Iterator<Node> iterator()
        {
            Iterable<PatternMatch> matches = PatternMatcher.getMatcher().match(
                    start, startNode );
            return new IteratorWrapper<Node,PatternMatch>( matches.iterator() )
            {
                @Override
                protected Node underlyingObjectToObject( PatternMatch match )
                {
                    return match.getNodeFor( message );
                }
            };
        }
    }

    private static final int EXPECTED_VISIBLE_MESSAGE_COUNT = 3;
    private static Node user;

    public static void setupGraph()
    {
        user = graphdb.createNode();
        Node user1 = graphdb.createNode(), user2 = graphdb.createNode(), user3 = graphdb.createNode();
        user.createRelationshipTo( user1, withName( "FOLLOWS" ) );
        user1.createRelationshipTo( user3, withName( "FOLLOWS" ) );
        user.createRelationshipTo( user2, withName( "FOLLOWS" ) );
        createMessage( user, "invisible", user1, user2 );
        createMessage( user1, "visible", user, user2, user3 );
        createMessage( user1, "visible", user );
        createMessage( user2, "visible", user, user1 );
        createMessage( user2, "invisible", user1, user3 );
        createMessage( user3, "invisible", user1, user2 );
        createMessage( user3, "invisible", user );
    }

    private static void createMessage( Node creator, String text,
            Node... visibleBy )
    {
        Node message = graphdb.createNode();
        message.setProperty( "text", text );
        creator.createRelationshipTo( message, withName( "CREATED" ) );
        for ( Node user : visibleBy )
        {
            message.createRelationshipTo( user, withName( "IS_VISIBLE_BY" ) );
        }
    }

    @Test
    public void straightPathsWork()
    {
        Node start = graphdb.createNode();
        Node u1 = graphdb.createNode(), u2 = graphdb.createNode(), u3 = graphdb.createNode();
        start.createRelationshipTo( u1, withName( "FOLLOWS" ) );
        start.createRelationshipTo( u2, withName( "FOLLOWS" ) );
        start.createRelationshipTo( u3, withName( "FOLLOWS" ) );
        createMessage( u1, "visible", start );
        createMessage( u2, "visible", start );
        createMessage( u3, "visible", start );
        for ( Node message : new VisibleMessagesByFollowedUsers( start ) )
        {
            verifyMessage( message );
        }
        tx.success();
    }

    @Test
    public void messageNodesAreOnlyReturnedOnce()
    {
        Map<Node,Integer> counts = new HashMap<Node,Integer>();
        for ( Node message : new VisibleMessagesByFollowedUsers( user ) )
        {
            Integer seen = counts.get( message );
            counts.put( message, seen == null ? 1 : (seen + 1) );
            count++;
        }
        StringBuilder duplicates = null;
        for ( Map.Entry<Node,Integer> seen : counts.entrySet() )
        {
            if ( seen.getValue() > 1 )
            {
                if ( duplicates == null )
                {
                    duplicates = new StringBuilder(
                            "These nodes occured multiple times (expected once): " );
                }
                else
                {
                    duplicates.append( ", " );
                }
                duplicates.append( seen.getKey() );
                duplicates.append( " (" );
                duplicates.append( seen.getValue() );
                duplicates.append( " times)" );
            }
        }
        if ( duplicates != null )
        {
            fail( duplicates.toString() );
        }
        tx.success();
    }

    @Test
    public void canFindMessageNodesThroughGraphMatching()
    {
        for ( Node message : new VisibleMessagesByFollowedUsers( user ) )
        {
            verifyMessage( message );
        }
        tx.success();
    }

    @Test
    public void canFindMessageNodesThroughTraversing()
    {
        for ( Node message : traverse( user ) )
        {
            verifyMessage( message );
        }
        tx.success();
    }

    private void verifyMessage( Node message )
    {
        assertNotNull( message );
        assertEquals( "visible", message.getProperty( "text", null ) );
        count++;
    }

    private int count;
    private Transaction tx;

    @Before
    public void resetCount()
    {
        count = 0;
        tx = graphdb.beginTx();
    }

    @After
    public void verifyCount()
    {
        tx.close();
        tx = null;
        assertEquals( EXPECTED_VISIBLE_MESSAGE_COUNT, count );
    }

    private static Iterable<Node> traverse( final Node startNode )
    {
        return startNode.traverse( Order.BREADTH_FIRST, stopAtDepth( 2 ),
                new ReturnableEvaluator()
                {
                    @Override
                    public boolean isReturnableNode( TraversalPosition pos )
                    {
                        Node node = pos.currentNode();
                        return isMessage( node )
                               && isVisibleTo( node, startNode );
                    }
                }, withName( "FOLLOWS" ), Direction.OUTGOING,
                withName( "CREATED" ), Direction.OUTGOING );
    }

    public static StopEvaluator stopAtDepth( final int depth )
    {
        return new StopEvaluator()
        {
            @Override
            public boolean isStopNode( TraversalPosition currentPos )
            {
                return currentPos.depth() >= depth;
            }
        };
    }

    static boolean isMessage( Node node )
    {
        return node.hasProperty( "text" );
    }

    static boolean isVisibleTo( Node message, Node user )
    {
        for ( Relationship visibility : message.getRelationships(
                withName( "IS_VISIBLE_BY" ), Direction.OUTGOING ) )
        {
            if ( visibility.getEndNode().equals( user ) )
            {
                return true;
            }
        }
        return false;
    }

    private static GraphDatabaseService graphdb;

    @BeforeClass
    public static void setUpDb()
    {
        graphdb = new TestGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.graphDbDir() );
        try ( Transaction tx = graphdb.beginTx() )
        {
            setupGraph();
            tx.success();
        }
    }

    @AfterClass
    public static void stopGraphdb()
    {
        graphdb.shutdown();
        graphdb = null;
    }

    @ClassRule
    public static TargetDirectory.TestDirectory testDirectory =
            TargetDirectory.testDirForTest( TestMatchingOfCircularPattern.class );
}
