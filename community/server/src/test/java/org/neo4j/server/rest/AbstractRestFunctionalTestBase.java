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
package org.neo4j.server.rest;

import org.junit.Before;
import org.junit.Rule;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import javax.ws.rs.core.Response.Status;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.TestData;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.SharedServerTestBase;
import org.neo4j.visualization.asciidoc.AsciidocHelper;

import static java.lang.String.format;
import static java.net.URLEncoder.encode;
import static org.junit.Assert.assertEquals;
import static org.neo4j.server.rest.domain.JsonHelper.createJsonFrom;
import static org.neo4j.server.rest.web.Surface.PATH_NODES;
import static org.neo4j.server.rest.web.Surface.PATH_NODE_INDEX;
import static org.neo4j.server.rest.web.Surface.PATH_RELATIONSHIPS;
import static org.neo4j.server.rest.web.Surface.PATH_RELATIONSHIP_INDEX;
import static org.neo4j.server.rest.web.Surface.PATH_SCHEMA_CONSTRAINT;
import static org.neo4j.server.rest.web.Surface.PATH_SCHEMA_INDEX;
import static org.neo4j.server.rest.web.Surface.PATH_SCHEMA_RELATIONSHIP_CONSTRAINT;

public class AbstractRestFunctionalTestBase extends SharedServerTestBase implements GraphHolder
{
    protected static final String NODES = "http://localhost:7474/db/data/node/";

    public @Rule
    TestData<Map<String, Node>> data = TestData.producedThrough( GraphDescription.createGraphFor( this, true ) );

    public @Rule
    TestData<RESTDocsGenerator> gen = TestData.producedThrough( RESTDocsGenerator.PRODUCER );

    @Before
    public void setUp()
    {
        gen().setSection( getDocumentationSectionName() );
        gen().setGraph( graphdb() );
    }

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

        String snippet = org.neo4j.cypher.internal.compiler.v2_3.prettifier.Prettifier$.MODULE$.apply( script );
        gen().expectedStatus( status.getStatusCode() )
                .payload( queryString )
                .description( AsciidocHelper.createAsciiDocSnippet( "cypher", snippet ) );
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

    protected String startGraph( String name )
    {
        return AsciidocHelper.createGraphVizWithNodeId( "Starting Graph", graphdb(), name );
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
        return "http://localhost:7474/db/data/";
    }

    protected String getDatabaseUri()
    {
        return "http://localhost:7474/db/";
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
        int lastSlash = response.location().lastIndexOf( "/" );
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
        for (String name : names)
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
        return getRelationshipUri(rel)+  "/properties";
    }
    public String getPropertiesUri( Node node )
    {
        return getNodeUri( node )+  "/properties";
    }

    public RESTDocsGenerator gen() {
        return gen.get();
    }

    public void description( String description )
    {
        gen().description( description );

    }

    protected String getDocumentationSectionName() {
        return "dev/rest-api";
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
        return format( "%slabel/%s/nodes?%s=%s", getDataUri(), label, property, encode( createJsonFrom( value ), "UTF-8" ) );
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

    public String getSchemaConstraintLabelExistenceUri( String label )
    {
        return getDataUri() + PATH_SCHEMA_CONSTRAINT + "/" + label + "/existence/";
    }

    public String getSchemaRelationshipConstraintTypeExistenceUri( String type )
    {
        return getDataUri() + PATH_SCHEMA_RELATIONSHIP_CONSTRAINT + "/" + type + "/existence/";
    }

    public String getSchemaConstraintLabelUniquenessPropertyUri( String label, String property )
    {
        return getDataUri() + PATH_SCHEMA_CONSTRAINT + "/" + label + "/uniqueness/" + property;
    }

    public String getSchemaConstraintLabelExistencePropertyUri( String label, String property )
    {
        return getDataUri() + PATH_SCHEMA_CONSTRAINT + "/" + label + "/existence/" + property;
    }

    public String getSchemaRelationshipConstraintTypeExistencePropertyUri( String type, String property )
    {
        return getDataUri() + PATH_SCHEMA_RELATIONSHIP_CONSTRAINT + "/" + type + "/existence/" + property;
    }
}
