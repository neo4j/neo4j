/*
 * Copyright (c) 2008-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package local;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.api.core.Direction;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;
import org.neo4j.api.core.Transaction;
import org.neo4j.api.core.TraversalPosition;
import org.neo4j.api.core.Traverser;
import org.neo4j.api.core.Traverser.Order;
import org.neo4j.remote.BasicNeoServer;
import org.neo4j.remote.RemoteIndexService;
import org.neo4j.remote.RemoteNeo;
import org.neo4j.remote.sites.LocalSite;
import org.neo4j.util.index.IndexService;
import org.neo4j.util.index.LuceneIndexService;

public class MatrixTest
{
    private static EmbeddedNeo backend;

    private static BasicNeoServer server;

    private static IndexService nodeIndex;

    @BeforeClass
    public static void startBackend()
    {
        backend = new EmbeddedNeo( "target/neo" );
        server = new LocalSite( backend );
        nodeIndex = new LuceneIndexService( backend );
        server.registerIndexService( "node index", nodeIndex );
    }

    @AfterClass
    public static void stopBackend()
    {
        nodeIndex.shutdown();
        backend.shutdown();
    }

    private NeoService neo;

    private IndexService index;

    @Before
    public void connect()
    {
        neo = new RemoteNeo( server );
        index = new RemoteIndexService( neo, "node index" );
    }

    @After
    public void disconnect()
    {
        neo.shutdown();
    }

    @Test
    public void testHasIndex() throws Exception
    {
        Assert.assertNotNull( "No indexes could be retreived.", index );
    }

    private static enum MatrixRelation implements RelationshipType
    {
        KNOWS, CODED_BY, LOVES
    }

    private static void defineMatrix( NeoService neo, IndexService index )
        throws Exception
    {
        // Define nodes
        Node mrAndersson, morpheus, trinity, cypher, agentSmith, theArchitect;
        mrAndersson = neo.createNode();
        morpheus = neo.createNode();
        trinity = neo.createNode();
        cypher = neo.createNode();
        agentSmith = neo.createNode();
        theArchitect = neo.createNode();
        // Define relationships
        @SuppressWarnings( "unused" )
        Relationship aKm, aKt, mKt, mKc, cKs, sCa, tLa;
        aKm = mrAndersson.createRelationshipTo( morpheus, MatrixRelation.KNOWS );
        aKt = mrAndersson.createRelationshipTo( trinity, MatrixRelation.KNOWS );
        mKt = morpheus.createRelationshipTo( trinity, MatrixRelation.KNOWS );
        mKc = morpheus.createRelationshipTo( cypher, MatrixRelation.KNOWS );
        cKs = cypher.createRelationshipTo( agentSmith, MatrixRelation.KNOWS );
        sCa = agentSmith.createRelationshipTo( theArchitect,
            MatrixRelation.CODED_BY );
        tLa = trinity.createRelationshipTo( mrAndersson, MatrixRelation.LOVES );
        // Define node properties
        mrAndersson.setProperty( "name", "Thomas Andersson" );
        morpheus.setProperty( "name", "Morpheus" );
        trinity.setProperty( "name", "Trinity" );
        cypher.setProperty( "name", "Cypher" );
        agentSmith.setProperty( "name", "Agent Smith" );
        theArchitect.setProperty( "name", "The Architect" );
        // Define relationship properties
        // Index nodes
        indexNodes( index, "name", mrAndersson, morpheus, trinity, cypher,
            agentSmith, theArchitect );
    }

    private static void indexNodes( IndexService index, String key,
        Node... nodes )
    {
        for ( Node node : nodes )
        {
            index.index( node, key, node.getProperty( key ) );
        }
    }

    private static void verifyFriendsOf( Node thomas ) throws Exception
    {
        Traverser traverser = thomas.traverse( Order.BREADTH_FIRST,
            StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL_BUT_START_NODE,
            MatrixRelation.KNOWS, Direction.OUTGOING );
        Set<String> actual = new HashSet<String>();
        for ( Node friend : traverser )
        {
            Assert.assertTrue( "Same friend added twice.", actual
                .add( ( String ) friend.getProperty( "name" ) ) );
        }
        Assert.assertEquals( "Thomas Anderssons friends are incorrect.",
            new HashSet<String>( Arrays.asList( "Trinity", "Morpheus",
                "Cypher", "Agent Smith" ) ), actual );
    }

    @SuppressWarnings( "serial" )
    private static void verifyHackersInNetworkOf( Node thomas )
        throws Exception
    {
        Traverser traverser = thomas.traverse( Order.BREADTH_FIRST,
            StopEvaluator.END_OF_GRAPH, new ReturnableEvaluator()
            {
                public boolean isReturnableNode( TraversalPosition pos )
                {
                    return pos.notStartNode()
                        && pos.lastRelationshipTraversed().isType(
                            MatrixRelation.CODED_BY );
                }
            }, MatrixRelation.CODED_BY, Direction.OUTGOING,
            MatrixRelation.KNOWS, Direction.OUTGOING );
        Map<String, Integer> actual = new HashMap<String, Integer>();
        for ( Node hacker : traverser )
        {
            Assert.assertNull( "Same hacker found twice.", actual.put(
                ( String ) hacker.getProperty( "name" ), traverser
                    .currentPosition().depth() ) );
        }
        Assert.assertEquals( "", new HashMap<String, Integer>()
        {
            {
                put( "The Architect", 4 );
            }
        }, actual );
    }

    private enum PlaceboVerifiction
    {
        REQUIRE_PROPER
        {
            @Override
            void of( String id, Transaction tx )
            {
                Assert.assertFalse( "Transaction \"" + id
                    + "\" is a placebo transaction.", tx.toString().startsWith(
                    "Placebo" ) );
            }
        },
        REQUIRE_PLACEBO
        {
            @Override
            void of( String id, Transaction tx )
            {
                Assert.assertTrue( "Transaction \"" + id
                    + "\" is not a placebo transaction.", tx.toString()
                    .startsWith( "Placebo" ) );
            }
        },
        EITHER
        {
            @Override
            void of( String id, Transaction tx )
            {
            }
        };

        abstract void of( String id, Transaction tx );

    }

    private static void verifyTransaction( String id, Transaction tx,
        PlaceboVerifiction verifcation )
    {
        Assert.assertNotNull( "Transaction \"" + id + "\" is null.", tx );
        verifcation.of( id, tx );
    }

    @Test
    public void testTheMatrix() throws Exception
    {
        Transaction tx = neo.beginTx();
        verifyTransaction( "nr 1", tx, PlaceboVerifiction.REQUIRE_PROPER );
        try
        {
            defineMatrix( neo, index );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        tx = neo.beginTx();
        verifyTransaction( "nr 2", tx, PlaceboVerifiction.REQUIRE_PROPER );
        try
        {
            verifyFriendsOf( index.getSingleNode( "name", "Thomas Andersson" ) );
            verifyHackersInNetworkOf( index.getSingleNode( "name",
                "Thomas Andersson" ) );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
}
