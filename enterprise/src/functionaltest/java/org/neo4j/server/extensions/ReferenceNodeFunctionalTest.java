/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.extensions;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerBuilder;
import org.neo4j.server.rest.FunctionalTestHelper;
import org.neo4j.server.rest.domain.JsonHelper;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

@Ignore( "disabled since the extension provider is not wired in." )
public class ReferenceNodeFunctionalTest
{
    private NeoServer server;
    private FunctionalTestHelper functionalTestHelper;

    @Before
    public void setupServer() throws IOException
    {
        server = ServerBuilder.server().withRandomDatabaseDir().withPassingStartupHealthcheck().build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper( server );
    }

    @After
    public void stopServer()
    {
        if ( server != null ) server.stop();
    }

    @Test
    public void canGetGraphDatabaseExtensionList() throws Exception
    {
        ClientResponse response = Client.create().resource( functionalTestHelper.dataUri() ).accept(
                MediaType.APPLICATION_JSON_TYPE ).get( ClientResponse.class );
        String body = response.getEntity( String.class );

        Map<String, Object> map = JsonHelper.jsonToMap( body );

        assertThat( map.get( "extensions" ), instanceOf( Map.class ) );
    }

    @Test
    public void canGetExtensionDefinitionForReferenceNodeExtension() throws Exception
    {
        ClientResponse response = Client.create().resource( functionalTestHelper.dataUri() ).accept(
                MediaType.APPLICATION_JSON_TYPE ).get( ClientResponse.class );
        String body = response.getEntity( String.class );

        Map<String, Object> map = JsonHelper.jsonToMap( body );
        map = (Map<String, Object>) map.get( "extensions" );

        assertThat( map.get( ReferenceNode.class.getSimpleName() ), instanceOf( Map.class ) );
    }

    @Test
    public void canGetExtensionDataForGetReferenceNode() throws Exception
    {
        ClientResponse response = Client.create().resource( functionalTestHelper.dataUri() ).accept(
                MediaType.APPLICATION_JSON_TYPE ).get( ClientResponse.class );
        String body = response.getEntity( String.class );

        Map<String, Object> map = JsonHelper.jsonToMap( body );
        map = (Map<String, Object>) map.get( "extensions" );
        System.out.println( map );
        map = (Map<String, Object>) map.get( ReferenceNode.class.getSimpleName() );

        System.out.println( map );

        assertThat( (String) map.get( ReferenceNode.GET_REFERENCE_NODE ),
                RegExp.endsWith( String.format( "/ext/%s/graphdb/%s",
                        ReferenceNode.class.getSimpleName(), ReferenceNode.GET_REFERENCE_NODE ) ) );
        assertThat( (String) map.get( "description" ),
                RegExp.endsWith( "/ext/" + ReferenceNode.class.getSimpleName() ) );
    }

    private static class RegExp extends TypeSafeMatcher<String>
    {
        enum MatchType
        {
            end( "ends with" )
            {
                @Override
                boolean match( String pattern, String string )
                {
                    return string.endsWith( pattern );
                }
            },
            matches()
            {
                @Override
                boolean match( String pattern, String string )
                {
                    return string.matches( pattern );
                }
            },
            ;
            private final String description;

            abstract boolean match( String pattern, String string );

            private MatchType()
            {
                this.description = name();
            }

            private MatchType( String description )
            {
                this.description = description;
            }
        }

        private final String pattern;
        private String string;
        private final MatchType type;

        RegExp( String regexp, MatchType type )
        {
            this.pattern = regexp;
            this.type = type;
        }

        @Factory
        public static Matcher<String> endsWith( String pattern )
        {
            return new RegExp( pattern, MatchType.end );
        }

        @Override
        public boolean matchesSafely( String string )
        {
            this.string = string;
            return type.match( pattern, string );
        }

        @Override
        public void describeTo( Description descr )
        {
            descr.appendText( "expected something that " ).appendText( type.description ).appendText(
                    " [" ).appendText( pattern ).appendText( "] but got [" ).appendText( string ).appendText(
                    "]" );
        }
    }
}
