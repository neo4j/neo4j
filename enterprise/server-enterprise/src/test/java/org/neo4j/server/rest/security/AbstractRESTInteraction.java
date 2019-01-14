/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.rest.security;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.LongNode;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import javax.ws.rs.core.HttpHeaders;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.configuration.ssl.LegacySslPolicyConfig;
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

    private ConnectorPortRegister connectorPortRegister;

    abstract String commitPath();

    abstract void consume( Consumer<ResourceIterator<Map<String,Object>>> resultConsumer, JsonNode data );

    abstract HTTP.RawPayload constructQuery( String query );

    protected abstract HTTP.Response authenticate( String principalCredentials );

    static final String POST = "POST";

    private EnterpriseAuthManager authManager;

    AbstractRESTInteraction( Map<String,String> config ) throws IOException
    {
        CommunityServerBuilder builder = EnterpriseServerBuilder.serverOnRandomPorts();
        builder = builder
                .withProperty( new BoltConnector( "bolt" ).type.name(), "BOLT" )
                .withProperty( new BoltConnector( "bolt" ).enabled.name(), "true" )
                .withProperty( new BoltConnector( "bolt" ).encryption_level.name(), OPTIONAL.name() )
                .withProperty( LegacySslPolicyConfig.tls_key_file.name(),
                        NeoInteractionLevel.tempPath( "key", ".key" ) )
                .withProperty( LegacySslPolicyConfig.tls_certificate_file.name(),
                        NeoInteractionLevel.tempPath( "cert", ".cert" ) )
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), Boolean.toString( true ) );

        for ( Map.Entry<String,String> entry : config.entrySet() )
        {
            builder = builder.withProperty( entry.getKey(), entry.getValue() );
        }
        this.server = builder.build();
        this.server.start();
        authManager = this.server.getDependencyResolver().resolveDependency( EnterpriseAuthManager.class );
        connectorPortRegister = server.getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
    }

    @Override
    public EnterpriseUserManager getLocalUserManager() throws Exception
    {
        if ( authManager instanceof EnterpriseAuthAndUserManager )
        {
            return ((EnterpriseAuthAndUserManager) authManager).getUserManager();
        }
        throw new Exception( "The used configuration does not have a user manager" );
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
        LoginContext loginContext = authManager.login( newBasicAuthToken( subject.username, subject.password ) );
        return getLocalGraph().beginTransaction( txType, loginContext );
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
    public RESTSubject login( String username, String password )
    {
        String principalCredentials = basicAuthHeader( username, password );
        return new RESTSubject( username, password, principalCredentials );
    }

    @Override
    public void logout( RESTSubject subject )
    {
    }

    @Override
    public void updateAuthToken( RESTSubject subject, String username, String password )
    {
        subject.principalCredentials = basicAuthHeader( username, password );
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
    public void assertPasswordChangeRequired( RESTSubject subject )
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
    public String getConnectionProtocol()
    {
        return "http";
    }

    @Override
    public HostnamePort lookupConnector( String connectorKey )
    {
        return connectorPortRegister.getLocalAddress( connectorKey );
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
        private int index;

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
                "Unhandled REST value type '%s'. Need String (TextNode), List (ArrayNode), Object (ObjectNode), " +
                        "long (LongNode), or int (IntNode).", valueNode.getClass()
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
