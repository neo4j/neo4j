/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.server.rest.security;

import org.codehaus.jackson.node.ArrayNode;
import org.junit.Test;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.ws.rs.core.HttpHeaders;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.server.enterprise.helpers.EnterpriseServerBuilder;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;

public class EnterpriseAuthenticationIT extends AuthenticationIT
{
    @Override
    public void startServer( boolean authEnabled ) throws IOException
    {
        server = EnterpriseServerBuilder.serverOnRandomPorts()
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), Boolean.toString( authEnabled ) )
                .build();
        server.start();
    }

    @Test
    public void shouldHavePredefinedRoles() throws Exception
    {
        // Given
        startServerWithConfiguredUser();

        // When
        String method = "POST";
        String path = "db/data/transaction/commit";
        HTTP.RawPayload payload = HTTP.RawPayload.quotedJson(
                "{'statements':[{'statement':'CALL dbms.security.listRoles()'}]}" );
        HTTP.Response response = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, basicAuthHeader( "neo4j", "secret" ) )
                .request( method, server.baseUri().resolve( path ).toString(), payload );

        // Then
        assertThat(response.status(), equalTo(200));
        ArrayNode errors = (ArrayNode) response.get("errors");
        assertThat( "Should have no errors", errors.size(), equalTo( 0 ) );
        ArrayNode results = (ArrayNode) response.get("results");
        ArrayNode data = (ArrayNode) results.get(0).get("data");
        assertThat( "Should have 5 predefined roles", data.size(), equalTo( 5 ) );
        Stream<String> values = data.findValues( "row" ).stream().map( row -> row.get(0).asText() );
        assertThat( "Expected specific roles", values.collect( Collectors.toList()),
                hasItems( "admin", "architect", "publisher", "editor", "reader") );

    }

    @Test
    public void shouldAllowExecutingEnterpriseBuiltInProceduresWithAuthDisabled() throws Exception
    {
        // Given
        startServerWithAuthDisabled();

        // When
        String method = "POST";
        String path = "db/data/transaction/commit";
        HTTP.RawPayload payload = HTTP.RawPayload.quotedJson(
                "{'statements':[{'statement':'CALL dbms.listQueries()'}]}" );
        HTTP.Response response = HTTP.request( method, server.baseUri().resolve( path ).toString(), payload );

        // Then
        assertThat(response.status(), equalTo(200));
        ArrayNode errors = (ArrayNode) response.get("errors");
        assertThat( "Should have no errors", errors.size(), equalTo( 0 ) );
        ArrayNode results = (ArrayNode) response.get("results");
        ArrayNode data = (ArrayNode) results.get(0).get("data");
        assertThat( "Should see our own query", data.size(), equalTo( 1 ) );
    }

    private void startServerWithAuthDisabled() throws IOException
    {
        server = EnterpriseServerBuilder.serverOnRandomPorts()
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), Boolean.toString( false ) )
                .build();
        server.start();
    }
}
