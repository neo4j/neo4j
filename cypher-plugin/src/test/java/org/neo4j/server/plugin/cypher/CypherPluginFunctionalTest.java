/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.plugin.cypher;

import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import javax.ws.rs.core.Response.Status;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.WrappingNeoServerBootstrapper;
import org.neo4j.server.rest.DocsGenerator;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphDescription.NODE;
import org.neo4j.test.GraphDescription.PROP;
import org.neo4j.test.GraphDescription.REL;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TestData;
import org.neo4j.test.TestData.Title;

public class CypherPluginFunctionalTest implements GraphHolder
{
    private static final String ENDPOINT = "http://localhost:7474/db/data/ext/CypherPlugin/graphdb/execute_query";
    private static ImpermanentGraphDatabase graphdb;
    public @Rule
    TestData<Map<String, Node>> data = TestData.producedThrough( GraphDescription.createGraphFor(
            this, true ) );

    public @Rule
    TestData<DocsGenerator> gen = TestData.producedThrough( DocsGenerator.PRODUCER );
    private static WrappingNeoServerBootstrapper server;

    /**
     * A simple query returning all nodes connected to node 1, returning the
     * node and the name property, if it exists, otherwise `null`:
     */
    @Test
    @Documented
    @Title( "Send a Query" )
    @Graph( nodes = {
            @NODE( name = "I", setNameProperty = true ),
            @NODE( name = "you", setNameProperty = true ),
            @NODE( name = "him", setNameProperty = true, properties = { 
                    @PROP( key = "age", value = "25", type = GraphDescription.PropType.INTEGER ) } ) }, 
            relationships = {
            @REL( start = "I", end = "him", type = "know", properties = {} ),
            @REL( start = "I", end = "you", type = "know", properties = {} ) } )
    public void testPropertyColumn() throws UnsupportedEncodingException
    {
        String script = "start x  = (" + data.get().get( "I" ).getId()
                        + ") match (x) --> (n) return n.name?, n.age?";
        String response = gen.get().expectedStatus( Status.OK.getStatusCode() ).payload(
                "{\"query\": \"" + script + "\"}" ).description(
                formatCypher( script ) ).post( ENDPOINT ).entity();
        assertTrue( response.contains( "you" ) );
        assertTrue( response.contains( "him" ) );
        assertTrue( response.contains( "25" ) );
        assertTrue( !response.contains( "\"x\"" ) );
    }

    @BeforeClass
    public static void startDatabase()
    {
        graphdb = new ImpermanentGraphDatabase( "target/db"
                                                + System.currentTimeMillis() );

    }

    @AfterClass
    public static void stopDatabase()
    {
    }

    @Override
    public GraphDatabaseService graphdb()
    {
        return graphdb;
    }

    @Before
    public void startServer()
    {
        graphdb.cleanContent();
        server = new WrappingNeoServerBootstrapper( graphdb );
        server.start();
        gen.get().setGraph( graphdb );
    }

    @After
    public void shutdownServer()
    {
        server.stop();
    }

    private String formatCypher( String script )
    {
        return "_Cypher query_\n\n" + "[source,cypher]\n" + "----\n" + script
               + "\n----\n";
    }
}
