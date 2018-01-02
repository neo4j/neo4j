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
package org.neo4j.server.plugins;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.UniformInterfaceException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.NeoServer;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.server.ExclusiveServerTestBase;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CloneSubgraphPluginTest extends ExclusiveServerTestBase
{
    private static final RelationshipType KNOWS = DynamicRelationshipType.withName( "knows" );
    private static final RelationshipType WORKED_FOR = DynamicRelationshipType.withName( "worked_for" );

    private static NeoServer server;
    private static FunctionalTestHelper functionalTestHelper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        server = ServerHelper.createNonPersistentServer();
        functionalTestHelper = new FunctionalTestHelper( server );
    }
    
    @AfterClass
    public static void shutdownServer()
    {
        try
        {
            if ( server != null ) server.stop();
        }
        finally
        {
            server = null;
        }
    }

    @Before
    public void setupTheDatabase()
    {
        ServerHelper.cleanTheDatabase( server );
        createASocialNetwork( server.getDatabase().getGraph() );
    }

    private Node jw;

    private void createASocialNetwork( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            jw = db.createNode();
            jw.setProperty( "name", "jim" );
            Node sp = db.createNode();
            sp.setProperty( "name", "savas" );
            Node bg = db.createNode();
            bg.setProperty( "name", "bill" );
            Node th = db.createNode();
            th.setProperty( "name", "tony" );
            Node rj = db.createNode();
            rj.setProperty( "name", "rhodri" );
            rj.setProperty( "hobby", "family" );
            Node nj = db.createNode();
            nj.setProperty( "name", "ned" );
            nj.setProperty( "hobby", "cs" );
            Node ml = db.createNode();
            ml.setProperty( "name", "mark" );
            Node mf = db.createNode();
            mf.setProperty( "name", "martin" );
            Node rp = db.createNode();
            rp.setProperty( "name", "rebecca" );
            Node rs = db.createNode();
            rs.setProperty( "name", "roy" );
            Node sc = db.createNode();
            sc.setProperty( "name", "steve" );
            sc.setProperty( "hobby", "cloud" );
            Node sw = db.createNode();
            sw.setProperty( "name", "stuart" );
            sw.setProperty( "hobby", "cs" );

            jw.createRelationshipTo( sp, KNOWS );
            jw.createRelationshipTo( mf, KNOWS );
            jw.createRelationshipTo( rj, KNOWS );
            rj.createRelationshipTo( nj, KNOWS );

            mf.createRelationshipTo( rp, KNOWS );
            mf.createRelationshipTo( rs, KNOWS );

            sp.createRelationshipTo( bg, KNOWS );
            sp.createRelationshipTo( th, KNOWS );
            sp.createRelationshipTo( mf, KNOWS );
            sp.createRelationshipTo( ml, WORKED_FOR );

            ml.createRelationshipTo( sc, KNOWS );
            ml.createRelationshipTo( sw, KNOWS );

            jw.setProperty( "hobby", "cs" );
            sp.setProperty( "hobby", "cs" );
            bg.setProperty( "hobby", "cs" );
            ml.setProperty( "hobby", "cs" );
            mf.setProperty( "hobby", "cs" );

            rp.setProperty( "hobby", "lisp" );
            rs.setProperty( "hobby", "socialism" );
            th.setProperty( "hobby", "fishing" );
            tx.success();
        }
    }

    @Test
    public void shouldAdvertiseExtenstionThatPluginCreates() throws JsonParseException, ClientHandlerException,
            UniformInterfaceException
    {
        int originalCount = nodeCount();

        // Find the start node URI from the server
        JaxRsResponse response = new RestRequest().get(functionalTestHelper.dataUri() + "node/1");

        String entity = response.getEntity();

        Map<String, Object> map = JsonHelper.jsonToMap( entity );

        HashMap<?, ?> extensionsMap = (HashMap<?, ?>) map.get( "extensions" );

        assertNotNull( extensionsMap );
        assertFalse( extensionsMap.isEmpty() );

        final String GRAPH_CLONER_KEY = "GraphCloner";
        assertTrue( extensionsMap.keySet()
                .contains( GRAPH_CLONER_KEY ) );

        final String CLONE_SUBGRAPH_KEY = "clonedSubgraph";
        String clonedSubgraphUri = (String) ( (HashMap<?, ?>) extensionsMap.get( GRAPH_CLONER_KEY ) ).get( CLONE_SUBGRAPH_KEY );
        assertNotNull( clonedSubgraphUri );

        final String CLONE_DEPTH_MUCH_LARGER_THAN_THE_GRAPH = "99";
        response.close();
        response = new RestRequest().post( clonedSubgraphUri, "depth=" + CLONE_DEPTH_MUCH_LARGER_THAN_THE_GRAPH, MediaType.APPLICATION_FORM_URLENCODED_TYPE );

        Assert.assertEquals( response.getEntity(), 200, response.getStatus() );

        int doubleTheNumberOfNodes = ( originalCount * 2 );
        assertEquals( doubleTheNumberOfNodes, nodeCount() );
    }

    private int nodeCount()
    {
        try ( Transaction tx = server.getDatabase().getGraph().beginTx() )
        {
            int count = 0;
            for ( @SuppressWarnings("unused") Node node : GlobalGraphOperations.at( server.getDatabase().getGraph() )
                                                                               .getAllNodes() )
            {
                count++;
            }
            return count;
        }
    }
}
