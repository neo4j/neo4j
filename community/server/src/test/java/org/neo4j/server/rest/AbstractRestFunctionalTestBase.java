/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.Rule;

import java.util.Arrays;
import java.util.Map;

import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.TestData;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.SharedServerTestBase;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;
import static org.neo4j.server.rest.web.Surface.PATH_NODES;
import static org.neo4j.server.rest.web.Surface.PATH_RELATIONSHIPS;
import static org.neo4j.test.server.HTTP.POST;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class AbstractRestFunctionalTestBase extends SharedServerTestBase implements GraphHolder
{
    @Rule
    public TestData<Map<String,Node>> data = TestData.producedThrough( GraphDescription.createGraphFor( this, true ) );

    @Rule
    public TestData<RESTRequestGenerator> gen = TestData.producedThrough( RESTRequestGenerator.PRODUCER );

    private Long idFor( String name )
    {
        return data.get().get( name ).getId();
    }

    @Override
    public GraphDatabaseService graphdb()
    {
        return server().getDatabase().getGraph();
    }

    public <T> T resolveDependency( Class<T> cls )
    {
        return ((GraphDatabaseAPI)graphdb()).getDependencyResolver().resolveDependency( cls );
    }

    protected static String getDataUri()
    {
        return "http://localhost:" + getLocalHttpPort() + "/db/data/";
    }

    protected String getDatabaseUri()
    {
        return "http://localhost:" + getLocalHttpPort() + "/db/";
    }

    protected String getNodeUri( Node node )
    {
        return getNodeUri(node.getId());
    }

    protected String getNodeUri( long node )
    {
        return getDataUri() + PATH_NODES + "/" + node;
    }

    protected String getRelationshipUri( Relationship relationship )
    {
        return getDataUri() + PATH_RELATIONSHIPS + "/" + relationship.getId();
    }

    protected String txUri()
    {
        return getDataUri() + "transaction";
    }

    protected static String txCommitUri()
    {
        return getDataUri() + "transaction/commit";
    }

    protected String txUri( long txId )
    {
        return getDataUri() + "transaction/" + txId;
    }

    public static long extractTxId( HTTP.Response response )
    {
        int lastSlash = response.location().lastIndexOf( '/' );
        String txIdString = response.location().substring( lastSlash + 1 );
        return Long.parseLong( txIdString );
    }

    public RESTRequestGenerator gen()
    {
        return gen.get();
    }

    public static int getLocalHttpPort()
    {
        ConnectorPortRegister connectorPortRegister =
                server().getDatabase().getGraph().getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
        return connectorPortRegister.getLocalAddress( "http" ).getPort();
    }

    public static HTTP.Response runQuery( String query, String...contentTypes  )
    {
        String resultDataContents = "";
        if ( contentTypes.length > 0 )
        {
            resultDataContents = ", 'resultDataContents': [" + Arrays.stream( contentTypes )
                    .map( unquoted -> format( "'%s'", unquoted ) ).collect( joining( "," ) ) + "]";
        }
        return POST( txCommitUri(), quotedJson( format( "{'statements': [{'statement': '%s'%s}]}", query, resultDataContents) ) );
    }

    public static void assertNoErrors( HTTP.Response response ) throws JsonParseException
    {
        assertEquals( "[]", response.get( "errors" ).toString() );
        assertEquals( 0, response.get( "errors" ).size() );
    }
}
