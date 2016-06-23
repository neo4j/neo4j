/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.server.rest.security;

import org.codehaus.jackson.JsonNode;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

import javax.ws.rs.core.HttpHeaders;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.security.AuthenticationResult;
import org.neo4j.server.enterprise.helpers.EnterpriseServerBuilder;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.security.enterprise.auth.EnterpriseAuthManager;
import org.neo4j.server.security.enterprise.auth.NeoInteractionLevel;
import org.neo4j.test.server.HTTP;

import static org.junit.Assert.fail;

public class NeoFullRESTInteraction extends CommunityServerTestBase implements NeoInteractionLevel<RESTSubject>
{
    String COMMIT_PATH = "db/data/transaction/commit";
    String POST = "POST";

    public NeoFullRESTInteraction() throws IOException
    {
        server = EnterpriseServerBuilder.server()
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), Boolean.toString( true ) )
                .withProperty( GraphDatabaseSettings.auth_manager.name(), "enterprise-auth-manager" )
                .build();
        server.start();
    }

    @Override
    public EnterpriseAuthManager getManager()
    {
        return server.getDependencyResolver().resolveDependency( EnterpriseAuthManager.class );
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
            JsonNode data = JsonHelper.jsonNode( response.rawContent() );
            if ( data.has( "errors" ) && data.get( "errors").has( 0 ) )
            {
                JsonNode firstError = data.get( "errors" ).get( 0 );
                if ( firstError.has( "message" ) )
                {
                    return firstError.get( "message" ).asText();
                }
            }
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
    public RESTSubject login( String username, String password ) throws Throwable
    {
        String principalCredentials = challengeResponse( username, password );
        HTTP.Response response = authenticate( principalCredentials );
        return new RESTSubject( response, principalCredentials );
    }

    private HTTP.Response authenticate( String principalCredentials )
    {
        return HTTP.withHeaders( HttpHeaders.AUTHORIZATION, principalCredentials ).request( POST, commitURL() );
    }

    @Override
    public void logout( RESTSubject subject ) { }

    @Override
    public boolean isAuthenticated( RESTSubject subject )
    {
        subject.response = authenticate( subject.principalCredentials );
        return subject.response.status() == 200 || // OK
                subject.response.status() == 403; // PASSWORD CHANGE REQUIRED
    }

    @Override
    public AuthenticationResult authenticationResult( RESTSubject subject )
    {
        switch ( subject.response.status() )
        {
            case 200: return AuthenticationResult.SUCCESS;
            case 403: return AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
            case 429: return AuthenticationResult.TOO_MANY_ATTEMPTS;
            default:  return AuthenticationResult.FAILURE;
        }
    }

    @Override
    public void tearDown()
    {
        if ( server != null )
        {
            server.stop();
        }
    }

    protected String commitURL()
    {
        return server.baseUri().resolve( COMMIT_PATH ).toString();
    }

    class RESTResult implements ResourceIterator<Map<String, Object>>
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
                map.put( key, row.get( i ).asText() );
            }

            return map;
        }
    }
}
