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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.server.WrappingNeoServerBootstrapper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.web.PropertyValueException;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TestData;
import org.neo4j.visualization.asciidoc.AsciidocHelper;

public class AbstractRestFunctionalTestBase implements GraphHolder
{

    static ImpermanentGraphDatabase graphdb;
    protected static final String NODES = "http://localhost:7474/db/data/node/";

    public @Rule
    TestData<Map<String, Node>> data = TestData.producedThrough( GraphDescription.createGraphFor(
            this, true ) );

    public @Rule
    TestData<RESTDocsGenerator> gen = TestData.producedThrough( RESTDocsGenerator.PRODUCER );
    protected static WrappingNeoServerBootstrapper server;

    @BeforeClass
    public static void startDatabase()
    {
        graphdb = new ImpermanentGraphDatabase( "target/db" );
        server = new WrappingNeoServerBootstrapper( graphdb );
        server.start();

    }

    
    protected String startGraph( String name )
    {
        return AsciidocHelper.createGraphViz( "Starting Graph", graphdb(), name);
    }

  

    @Override
    public GraphDatabaseService graphdb()
    {
        return graphdb;
    }

    @Before
    public void cleanContent()
    {
        graphdb.cleanContent();
        gen.get().setGraph( graphdb );
    }

    @AfterClass
    public static void shutdownServer()
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
    protected String getRelationshipUri( Relationship node )
    {
        return getDataUri() + "relationship/" + node.getId();
    }
    protected String getNodeIndexUri( String indexName, String key, String value )
    {
        return getDataUri() + "index/node/" + indexName + "/" + key + "/" + value;
    }

    protected String getRelationshipIndexUri( String indexName, String key, String value )
    {
        return getDataUri() + "index/relationship/" + indexName + "/" + key + "/" + value;
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
    
    public void assertSize(int expectedSize, String entity) {
        Collection<?> hits;
        try
        {
            hits = (Collection<?>) JsonHelper.jsonToSingleValue( entity );
            assertEquals( expectedSize, hits.size() );
        }
        catch ( PropertyValueException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public String getPropertiesUri( Relationship rel )
    {
        return getRelationshipUri(rel)+  "/properties";
    }
    
    public RESTDocsGenerator gen() {
        return gen.get();
    }
    
    public void description( String description )
    {
        gen().description( description );
        
    }
}
