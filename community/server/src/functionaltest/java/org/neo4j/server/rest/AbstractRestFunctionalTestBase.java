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
package org.neo4j.server.rest;

import java.util.ArrayList;
import java.util.Map;

import org.jruby.compiler.ir.operands.Array;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.server.WrappingNeoServerBootstrapper;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TestData;

public class AbstractRestFunctionalTestBase implements GraphHolder
{

    private static ImpermanentGraphDatabase graphdb;
    protected static final String NODES = "http://localhost:7474/db/data/node/";

    public @Rule
    TestData<Map<String, Node>> data = TestData.producedThrough( GraphDescription.createGraphFor(
            this, true ) );

    public @Rule
    TestData<DocsGenerator> gen = TestData.producedThrough( DocsGenerator.PRODUCER );
    protected static WrappingNeoServerBootstrapper server;

    @BeforeClass
    public static void startDatabase()
    {
        graphdb = new ImpermanentGraphDatabase( "target/db" );

    }

    protected String startGraph( String name )
    {
        return "_Starting Graph:_\n\n"
               + gen.get().createGraphViz( graphdb(), name );
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

    protected String getDataUri()
    {
        return "http://localhost:7474/db/data/";
    }

    protected String getNodeUri( Node node )
    {
        return getDataUri() + "node/" + node.getId();
    }

    protected Node getNode( String name )
    {
        return data.get().get( name );
    }

    protected Node[] getNodes( String... names )
    {
        Node[] nodes = {};
        ArrayList<Node> result = new ArrayList<Node>();
        for (String name : names)
        {
            result.add( getNode( name ) );
        }
        return result.toArray(nodes);
    }
}
