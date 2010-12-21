/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.plugins;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerBuilder;
import org.neo4j.server.rest.FunctionalTestHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.*;

public class CloneSubgraphPluginTest {

    private static final RelationshipType KNOWS = new RelationshipType() {
        @Override
        public String name() {
            return "knows";
        }
    };

    private static final RelationshipType WORKED_FOR = new RelationshipType() {
        @Override
        public String name() {
            return "worked_for";
        }
    };

    private NeoServer server;
    private FunctionalTestHelper functionalTestHelper;

    private Node jw;

    @Before
    public void setupServer() throws IOException {
        server = ServerBuilder.server().withRandomDatabaseDir().withPassingStartupHealthcheck().build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper(server);
        createASocialNetwork(server.getDatabase().graph);
    }
    
    private void createASocialNetwork(GraphDatabaseService db) {
        Transaction tx = db.beginTx();
        try {
            jw = db.createNode();
            jw.setProperty("name", "jim");
            Node sp = db.createNode();
            sp.setProperty("name", "savas");
            Node bg = db.createNode();
            bg.setProperty("name", "bill");
            Node th = db.createNode();
            th.setProperty("name", "tony");
            Node rj = db.createNode();
            rj.setProperty("name", "rhodri");
            rj.setProperty("hobby", "family");
            Node nj = db.createNode();
            nj.setProperty("name", "ned");
            nj.setProperty("hobby", "cs");
            Node ml = db.createNode();
            ml.setProperty("name", "mark");
            Node mf = db.createNode();
            mf.setProperty("name", "martin");
            Node rp = db.createNode();
            rp.setProperty("name", "rebecca");
            Node rs = db.createNode();
            rs.setProperty("name", "roy");
            Node sc = db.createNode();
            sc.setProperty("name", "steve");
            sc.setProperty("hobby", "cloud");
            Node sw = db.createNode();
            sw.setProperty("name", "stuart");
            sw.setProperty("hobby", "cs");

            jw.createRelationshipTo(sp, KNOWS);
            jw.createRelationshipTo(mf, KNOWS);
            jw.createRelationshipTo(rj, KNOWS);
            rj.createRelationshipTo(nj, KNOWS);

            mf.createRelationshipTo(rp, KNOWS);
            mf.createRelationshipTo(rs, KNOWS);

            sp.createRelationshipTo(bg, KNOWS);
            sp.createRelationshipTo(th, KNOWS);
            sp.createRelationshipTo(mf, KNOWS);
            sp.createRelationshipTo(ml, WORKED_FOR);

            ml.createRelationshipTo(sc, KNOWS);
            ml.createRelationshipTo(sw, KNOWS);

            jw.setProperty("hobby", "cs");
            sp.setProperty("hobby", "cs");
            bg.setProperty("hobby", "cs");
            ml.setProperty("hobby", "cs");
            mf.setProperty("hobby", "cs");

            rp.setProperty("hobby", "lisp");
            rs.setProperty("hobby", "socialism");
            th.setProperty("hobby", "fishing");
            tx.success();
        } finally {
            tx.finish();
        }

    }

    @After
    public void stopServer() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void shouldAdvertiseExtenstionThatPluginCreates() throws JsonParseException, ClientHandlerException, UniformInterfaceException {
        int originalCount = eagerlyCount(server.getDatabase().graph.getAllNodes());
        originalCount--; // Don't count the reference node

        // Find the start node URI from the server
        ClientResponse response = Client.create().resource(functionalTestHelper.dataUri() + "node/1").accept(MediaType.APPLICATION_JSON)
                .get(ClientResponse.class);

        String entity = response.getEntity(String.class);
        System.out.println(entity);
        Map<String, Object> map = JsonHelper.jsonToMap(entity);

        HashMap<?, ?> extensionsMap = (HashMap<?, ?>) map.get("extensions");
        
        assertNotNull(extensionsMap);
        assertFalse(extensionsMap.isEmpty());
        
        final String GRAPH_CLONER_KEY = "GraphCloner";
        assertTrue(extensionsMap.keySet().contains(GRAPH_CLONER_KEY));
                
        final String CLONE_SUBGRAPH_KEY = "clonedSubgraph";
        String clonedSubgraphUri = (String) ((HashMap<?, ?>)extensionsMap.get(GRAPH_CLONER_KEY)).get(CLONE_SUBGRAPH_KEY);
        assertNotNull(clonedSubgraphUri);
        
        final String CLONE_DEPTH_MUCH_LARGER_THAN_THE_GRAPH = "99";
        response = Client.create().resource(clonedSubgraphUri).type(MediaType.APPLICATION_FORM_URLENCODED).entity("depth=" + CLONE_DEPTH_MUCH_LARGER_THAN_THE_GRAPH).post(ClientResponse.class);
        
        assertEquals(200, response.getStatus());
        

        int doubleTheNumberOfNodes = (originalCount * 2) + 1;
        assertEquals(doubleTheNumberOfNodes, eagerlyCount(server.getDatabase().graph.getAllNodes()));
    }

    private int eagerlyCount(Iterable<?> items) {
        if (items == null)
            return 0;

        int count = 0;
        Iterator<?> iterator = items.iterator();
        while (iterator.hasNext()) {
            count++;
            iterator.next();
        }

        return count;
    }
}