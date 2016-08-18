/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.server.rest.security;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import javax.ws.rs.core.HttpHeaders;

import org.neo4j.bolt.BoltKernelExtension;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.server.enterprise.helpers.EnterpriseServerBuilder;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.security.enterprise.auth.EnterpriseAuthManager;
import org.neo4j.server.security.enterprise.auth.EnterpriseUserManager;
import org.neo4j.server.security.enterprise.auth.NeoInteractionLevel;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.boltConnector;
import static org.neo4j.kernel.api.security.AuthToken.newBasicAuthToken;

public class NeoFullRESTInteraction extends CommunityServerTestBase implements NeoInteractionLevel<RESTSubject>
{
    String COMMIT_PATH = "db/data/transaction/commit";
    String POST = "POST";

    EnterpriseAuthManager authManager;

    public NeoFullRESTInteraction() throws IOException
    {
        server = EnterpriseServerBuilder.server()
                .withProperty( boltConnector( "0" ).enabled.name(), "true" )
                .withProperty( boltConnector( "0" ).encryption_level.name(), OPTIONAL.name() )
                .withProperty( BoltKernelExtension.Settings.tls_key_file.name(),
                                    NeoInteractionLevel.tempPath( "key", ".key" ) )
                .withProperty( BoltKernelExtension.Settings.tls_certificate_file.name(),
                                    NeoInteractionLevel.tempPath( "cert", ".cert" ) )
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), Boolean.toString( true ) )
                .withProperty( GraphDatabaseSettings.auth_manager.name(), "enterprise-auth-manager" )
                .build();
        server.start();
        authManager = server.getDependencyResolver().resolveDependency( EnterpriseAuthManager.class );
    }

    @Override
    public EnterpriseUserManager getManager()
    {
        return authManager.getUserManager();
    }

    @Override
    public GraphDatabaseFacade getGraph()
    {
        return server.getDatabase().getGraph();
    }

    @Override
    public InternalTransaction startTransactionAsUser( RESTSubject subject ) throws Throwable
    {
        AuthSubject authSubject = authManager.login( newBasicAuthToken( subject.username, subject.password ) );
        return getGraph().beginTransaction( KernelTransaction.Type.explicit, authSubject );
    }

    @Override
    public String executeQuery( RESTSubject subject, String call, Map<String,Object> params,
            Consumer<ResourceIterator<Map<String, Object>>> resultConsumer )
    {
        String escapedCall = call.replace( "'", "\\'" ).replace( "\"", "\\\"" );
        HTTP.RawPayload payload = HTTP.RawPayload.quotedJson(
                "{'statements':[{'statement':'" + escapedCall + "'}]}" );
        HTTP.Response response = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, subject.principalCredentials )
                .request( POST, commitURL(), payload );

        try
        {
            String error = parseErrorMessage( response );
            if (!error.isEmpty())
            {
                return error;
            }
            JsonNode data = JsonHelper.jsonNode( response.rawContent() );
            if ( data.has( "results" ) && data.get( "results" ).has( 0 ) )
            {
                JsonNode firstResult = data.get( "results" ).get( 0 );
                RESTResult result = new RESTResult( firstResult );
                resultConsumer.accept( result );
            }
        }
        catch ( JsonParseException e )
        {
            fail("Unexpected error parsing Json!");
        }

        return "";
    }

    @Override
    public RESTSubject login( String username, String password ) throws Exception
    {
        String principalCredentials = challengeResponse( username, password );
        return new RESTSubject( username, password, principalCredentials );
    }

    private HTTP.Response authenticate( String principalCredentials )
    {
        return HTTP.withHeaders( HttpHeaders.AUTHORIZATION, principalCredentials ).request( POST, commitURL() );
    }

    @Override
    public void logout( RESTSubject subject ) { }

    @Override
    public void updateAuthToken( RESTSubject subject, String username, String password )
    {
        subject.principalCredentials = challengeResponse( username, password );
    }

    @Override
    public String nameOf( RESTSubject subject )
    {
        return subject.username;
    }

    @Override
    public void tearDown()
    {
        if ( server != null )
        {
            server.stop();
        }
    }

    @Override
    public void assertAuthenticated( RESTSubject subject )
    {
        assertThat( authenticate( subject.principalCredentials ).status(), equalTo( 200 ) );
    }

    @Override
    public void assertPasswordChangeRequired( RESTSubject subject ) throws Exception
    {
        HTTP.Response response = authenticate( subject.principalCredentials );
        assertThat( response.status(), equalTo( 403 ) );
        assertThat( parseErrorMessage( response ), containsString( "User is required to change their password." ) );
    }

    @Override
    public void assertInitFailed( RESTSubject subject )
    {
        assertThat( authenticate( subject.principalCredentials ).status(), not( equalTo( 200 ) ) );
    }

    private String parseErrorMessage( HTTP.Response response )
    {
        try
        {
            JsonNode data = JsonHelper.jsonNode( response.rawContent() );
            if ( data.has( "errors" ) && data.get( "errors" ).has( 0 ) )
            {
                JsonNode firstError = data.get( "errors" ).get( 0 );
                if ( firstError.has( "message" ) )
                {
                    return firstError.get( "message" ).asText();
                }
            }
        }
        catch ( JsonParseException e )
        {
            fail( "Unexpected error parsing Json!" );
        }
        return "";
    }

    protected String commitURL()
    {
        return server.baseUri().resolve( COMMIT_PATH ).toString();
    }

    class RESTResult implements ResourceIterator<Map<String,Object>>
    {
        private JsonNode data;
        private JsonNode columns;
        private int index = 0;

        public RESTResult( JsonNode fullResult )
        {
            this.data = fullResult.get( "data" );
            this.columns = fullResult.get( "columns" );
        }

        @Override
        public void close()
        {
            index = data.size();
        }

        @Override
        public boolean hasNext()
        {
            return index < data.size();
        }

        @Override
        public Map<String,Object> next()
        {
            JsonNode row = data.get( index++ ).get( "row" );
            TreeMap<String,Object> map = new TreeMap();
            for ( int i = 0; i < columns.size(); i++ )
            {
                String key = columns.get( i ).asText();
                JsonNode value = row.get( i );
                if ( value instanceof TextNode )
                {
                    map.put( key, row.get( i ).asText() );
                }
                else if ( value instanceof ArrayNode )
                {
                    ArrayNode aNode = (ArrayNode) value;
                    ArrayList<String> listValue = new ArrayList( aNode.size() );
                    for ( int j = 0; j < aNode.size(); j++ )
                    {
                        listValue.add( aNode.get( j ).asText() );
                    }
                    map.put( key, listValue );
                }
                else if ( value instanceof ObjectNode )
                {
                    map.put( key, value );
                }
                else if ( value instanceof IntNode )
                {
                    map.put( key, value.getIntValue() );
                }
                else
                {
                    throw new RuntimeException( "Unhandled REST value type '" + value.getClass() +
                            "'. Need String (TextNode), List (ArrayNode), Object (ObjectNode) or int (IntNode)." );
                }
            }

            return map;
        }
    }
}
