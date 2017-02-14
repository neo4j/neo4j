/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import javax.ws.rs.core.HttpHeaders;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.LongNode;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;

import org.neo4j.bolt.BoltKernelExtension;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.server.enterprise.helpers.EnterpriseServerBuilder;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.security.enterprise.auth.EnterpriseAuthAndUserManager;
import org.neo4j.server.security.enterprise.auth.EnterpriseUserManager;
import org.neo4j.server.security.enterprise.auth.NeoInteractionLevel;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.api.security.AuthToken.newBasicAuthToken;
import static org.neo4j.kernel.configuration.BoltConnector.EncryptionLevel.OPTIONAL;

abstract class AbstractRESTInteraction extends CommunityServerTestBase implements NeoInteractionLevel<RESTSubject>
{
    abstract String commitPath();

    abstract void consume( Consumer<ResourceIterator<Map<String,Object>>> resultConsumer, JsonNode data );

    abstract HTTP.RawPayload constructQuery( String query );

    protected abstract HTTP.Response authenticate( String principalCredentials );

    static final String POST = "POST";

    private EnterpriseAuthManager authManager;

    AbstractRESTInteraction( Map<String,String> config ) throws IOException
    {
        CommunityServerBuilder builder = EnterpriseServerBuilder.server();
        builder = builder
                .withProperty( new BoltConnector( "bolt" ).type.name(), "BOLT" )
                .withProperty( new BoltConnector( "bolt" ).enabled.name(), "true" )
                .withProperty( new BoltConnector( "bolt" ).encryption_level.name(), OPTIONAL.name() )
                .withProperty( BoltKernelExtension.Settings.tls_key_file.name(),
                        NeoInteractionLevel.tempPath( "key", ".key" ) )
                .withProperty( BoltKernelExtension.Settings.tls_certificate_file.name(),
                        NeoInteractionLevel.tempPath( "cert", ".cert" ) )
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), Boolean.toString( true ) );

        for ( Map.Entry<String,String> entry : config.entrySet() )
        {
            builder = builder.withProperty( entry.getKey(), entry.getValue() );
        }
        this.server = builder.build();
        this.server.start();
        authManager = this.server.getDependencyResolver().resolveDependency( EnterpriseAuthManager.class );
    }

    @Override
    public EnterpriseUserManager getLocalUserManager() throws Exception
    {
        if ( authManager instanceof EnterpriseAuthAndUserManager )
        {
            return ((EnterpriseAuthAndUserManager) authManager).getUserManager();
        }
        throw new Exception("The used configuration does not have a user manager");
    }

    @Override
    public GraphDatabaseFacade getLocalGraph()
    {
        return server.getDatabase().getGraph();
    }

    @Override
    public FileSystemAbstraction fileSystem()
    {
        return new DefaultFileSystemAbstraction();
    }

    @Override
    public InternalTransaction beginLocalTransactionAsUser( RESTSubject subject, KernelTransaction.Type txType )
            throws Throwable
    {
        SecurityContext securityContext = authManager.login( newBasicAuthToken( subject.username, subject.password ) );
        return getLocalGraph().beginTransaction( txType, securityContext );
    }

    @Override
    public String executeQuery( RESTSubject subject, String call, Map<String,Object> params,
            Consumer<ResourceIterator<Map<String, Object>>> resultConsumer )
    {
        HTTP.RawPayload payload = constructQuery( call );
        HTTP.Response response = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, subject.principalCredentials )
                .request( POST, commitURL(), payload );

        try
        {
            String error = parseErrorMessage( response );
            if ( !error.isEmpty() )
            {
                return error;
            }
            consume( resultConsumer, JsonHelper.jsonNode( response.rawContent() ) );
        }
        catch ( JsonParseException e )
        {
            fail( "Unexpected error parsing Json!" );
        }

        return "";
    }

    @Override
    public RESTSubject login( String username, String password ) throws Exception
    {
        String principalCredentials = challengeResponse( username, password );
        return new RESTSubject( username, password, principalCredentials );
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
        HTTP.Response authenticate = authenticate( subject.principalCredentials );
        assertThat( authenticate.rawContent(), authenticate.status(), equalTo( 200 ) );
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

    @Override
    public void assertSessionKilled( RESTSubject subject )
    {
        // There is no session that could have been killed
    }

    @Override
    public String getConnectionDetails()
    {
        return "server-session";
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

    String commitURL()
    {
        return server.baseUri().resolve( commitPath() ).toString();
    }

    abstract class AbstractRESTResult implements ResourceIterator<Map<String,Object>>
    {
        private JsonNode data;
        private JsonNode columns;
        private int index = 0;

        AbstractRESTResult( JsonNode fullResult )
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
            JsonNode row = getRow( data, index++ );
            TreeMap<String,Object> map = new TreeMap<>();
            for ( int i = 0; i < columns.size(); i++ )
            {
                String key = columns.get( i ).asText();
                Object value = getValue( row.get( i ) );
                map.put( key, value );
            }
            return map;
        }

        protected abstract JsonNode getRow( JsonNode data, int i );
    }

    private Object getValue( JsonNode valueNode )
    {
        Object value;

        if ( valueNode instanceof TextNode )
        {
            value = valueNode.asText();
        }
        else if ( valueNode instanceof ObjectNode )
        {
            value = mapValue( valueNode.getFieldNames(), valueNode );
        }
        else if ( valueNode instanceof ArrayNode )
        {
            ArrayNode aNode = (ArrayNode) valueNode;
            ArrayList<String> listValue = new ArrayList<>( aNode.size() );
            for ( int j = 0; j < aNode.size(); j++ )
            {
                listValue.add( aNode.get( j ).asText() );
            }
            value = listValue;
        }
        else if ( valueNode instanceof IntNode )
        {
            value = valueNode.getIntValue();
        }
        else if ( valueNode instanceof LongNode )
        {
            value = valueNode.getLongValue();
        }
        else if ( valueNode.isNull() )
        {
            return null;
        }
        else
        {
            throw new RuntimeException( String.format(
                "Unhandled REST value type '%s'. Need String (TextNode), List (ArrayNode), Object (ObjectNode), long (LongNode), or int (IntNode).",
                valueNode.getClass()
            ) );
        }
        return value;
    }

    private Map<String,Object> mapValue( Iterator<String> columns, JsonNode node )
    {
        TreeMap<String,Object> map = new TreeMap<>();
        while ( columns.hasNext() )
        {
            String key = columns.next();
            Object value = getValue( node.get( key ) );
            map.put( key, value );
        }

        return map;
    }
}
