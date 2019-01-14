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

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response.Status;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.TestData;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.SharedServerTestBase;

import static java.lang.String.format;
import static java.net.URLEncoder.encode;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;
import static org.neo4j.server.rest.domain.JsonHelper.createJsonFrom;
import static org.neo4j.server.rest.web.Surface.PATH_NODES;
import static org.neo4j.server.rest.web.Surface.PATH_NODE_INDEX;
import static org.neo4j.server.rest.web.Surface.PATH_RELATIONSHIPS;
import static org.neo4j.server.rest.web.Surface.PATH_RELATIONSHIP_INDEX;
import static org.neo4j.server.rest.web.Surface.PATH_SCHEMA_CONSTRAINT;
import static org.neo4j.server.rest.web.Surface.PATH_SCHEMA_INDEX;
import static org.neo4j.test.server.HTTP.POST;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class AbstractRestFunctionalTestBase extends SharedServerTestBase implements GraphHolder
{
    @Rule
    public TestData<Map<String,Node>> data = TestData.producedThrough( GraphDescription.createGraphFor( this, true ) );

    @Rule
    public TestData<RESTRequestGenerator> gen = TestData.producedThrough( RESTRequestGenerator.PRODUCER );

    @SafeVarargs
    public final String doCypherRestCall( String endpoint, String scriptTemplate, Status status,
            Pair<String, String>... params )
    {
        String parameterString = createParameterString( params );

        return doCypherRestCall( endpoint, scriptTemplate, status, parameterString );
    }

    public String doCypherRestCall( String endpoint, String scriptTemplate, Status status, String parameterString )
    {
        data.get();

        String script = createScript( scriptTemplate );
        String queryString = "{\"query\": \"" + script + "\",\"params\":{" + parameterString + "}}";

        gen().expectedStatus( status.getStatusCode() )
                .payload( queryString );
        return gen().post( endpoint ).entity();
    }

    private Long idFor( String name )
    {
        return data.get().get( name ).getId();
    }

    private String createParameterString( Pair<String, String>[] params )
    {
        String paramString = "";
        for ( Pair<String, String> param : params )
        {
            String delimiter = paramString.isEmpty() || paramString.endsWith( "{" ) ? "" : ",";

            paramString += delimiter + "\"" + param.first() + "\":\"" + param.other() + "\"";
        }

        return paramString;
    }

    protected String createScript( String template )
    {
        for ( String key : data.get().keySet() )
        {
            template = template.replace( "%" + key + "%", idFor( key ).toString() );
        }
        return template;
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

    protected String postNodeIndexUri( String indexName )
    {
        return getDataUri() + PATH_NODE_INDEX + "/" + indexName;
    }

    protected String postRelationshipIndexUri( String indexName )
    {
        return getDataUri() + PATH_RELATIONSHIP_INDEX + "/" + indexName;
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

    protected Node getNode( String name )
    {
        return data.get().get( name );
    }

    protected Node[] getNodes( String... names )
    {
        Node[] nodes = {};
        ArrayList<Node> result = new ArrayList<>();
        for ( String name : names )
        {
            result.add( getNode( name ) );
        }
        return result.toArray( nodes );
    }

    public void assertSize( int expectedSize, String entity )
    {
        Collection<?> hits;
        try
        {
            hits = (Collection<?>) JsonHelper.readJson( entity );
            assertEquals( expectedSize, hits.size() );
        }
        catch ( JsonParseException e )
        {
            throw new RuntimeException( e );
        }
    }

    public String getPropertiesUri( Relationship rel )
    {
        return getRelationshipUri( rel ) + "/properties";
    }

    public String getPropertiesUri( Node node )
    {
        return getNodeUri( node ) + "/properties";
    }

    public RESTRequestGenerator gen()
    {
        return gen.get();
    }

    public String getLabelsUri()
    {
        return format( "%slabels", getDataUri() );
    }

    public String getPropertyKeysUri()
    {
        return format( "%spropertykeys", getDataUri() );
    }

    public String getNodesWithLabelUri( String label )
    {
        return format( "%slabel/%s/nodes", getDataUri(), label );
    }

    public String getNodesWithLabelAndPropertyUri( String label, String property, Object value ) throws UnsupportedEncodingException
    {
        return format( "%slabel/%s/nodes?%s=%s", getDataUri(), label, property,
                encode( createJsonFrom( value ), StandardCharsets.UTF_8.name() ) );
    }

    public String getSchemaIndexUri()
    {
        return getDataUri() + PATH_SCHEMA_INDEX;
    }

    public String getSchemaIndexLabelUri( String label )
    {
        return getDataUri() + PATH_SCHEMA_INDEX + "/" + label;
    }

    public String getSchemaIndexLabelPropertyUri( String label, String property )
    {
        return getDataUri() + PATH_SCHEMA_INDEX + "/" + label + "/" + property;
    }

    public String getSchemaConstraintUri()
    {
        return getDataUri() + PATH_SCHEMA_CONSTRAINT;
    }

    public String getSchemaConstraintLabelUri( String label )
    {
        return getDataUri() + PATH_SCHEMA_CONSTRAINT + "/" + label;
    }

    public String getSchemaConstraintLabelUniquenessUri( String label )
    {
        return getDataUri() + PATH_SCHEMA_CONSTRAINT + "/" + label + "/uniqueness/";
    }

    public String getSchemaConstraintLabelUniquenessPropertyUri( String label, String property )
    {
        return getDataUri() + PATH_SCHEMA_CONSTRAINT + "/" + label + "/uniqueness/" + property;
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
