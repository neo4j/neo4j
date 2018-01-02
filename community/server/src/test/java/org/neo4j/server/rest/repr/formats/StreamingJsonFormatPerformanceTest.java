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
package org.neo4j.server.rest.repr.formats;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.WrappingNeoServer;
import org.neo4j.test.TestGraphDatabaseFactory;

/**
 * Ignored performance test.
 * TODO: Move this into performance-regression project.
 */
@Ignore
public class StreamingJsonFormatPerformanceTest {

    public static final String QUERY = "start n=node(*) match p=n-[r:TYPE]->m return n,r,m,p";
    private GraphDatabaseService gdb;
    private WrappingNeoServer server;

    @Before
    public void setUp() {
        gdb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        for ( int i = 0; i < 10; i++ ) {
            createData();
        }
        server = new WrappingNeoServer( (GraphDatabaseAPI) gdb );
        server.start();
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    @Test
    public void testStreamCypherResults() throws Exception {
        final String query = "{\"query\":\"" + QUERY + "\"}";
        measureQueryTime(query);
    }

    private long measureQueryTime(String query) throws IOException {
        final URI baseUri = server.baseUri();
        final URL url = new URL(baseUri.toURL(), "db/data/cypher");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setDoInput(true);
        connection.setInstanceFollowRedirects(false);
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Accept", "application/json; stream=true");

        OutputStream os = connection.getOutputStream();
        os.write(query.getBytes());
        os.close();
        final InputStream input = new BufferedInputStream(connection.getInputStream());
        long time=System.currentTimeMillis();
        //final CountingInputStream counter = new CountingInputStream(input);
        while (input.read()!=-1);
        input.close();
        final long delta = System.currentTimeMillis() - time;
        //System.out.println("result.length() = " + counter.getCount()+" took "+ delta +" ms.");
        return delta;
    }


    private void createData() {
        try ( Transaction tx = gdb.beginTx() )
        {
            final DynamicRelationshipType TYPE = DynamicRelationshipType.withName("TYPE");
            Node last = gdb.createNode();
            last.setProperty("id", 0);
            for (int i = 1; i < 10000; i++) {
                final Node node = gdb.createNode();
                last.setProperty("id", i);
                node.createRelationshipTo(last, TYPE);
                last = node;
            }
            tx.success();
        }
    }
}
