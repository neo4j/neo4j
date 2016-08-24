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

public class EnterpriseAuthenticationDocIT extends AuthenticationDocIT
{
    @Override
    public void startServer( boolean authEnabled ) throws IOException
    {
        server = EnterpriseServerBuilder.server()
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), Boolean.toString( authEnabled ) )
                .withProperty( GraphDatabaseSettings.auth_manager.name(), "enterprise-auth-manager" )
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
        HTTP.Response response = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, challengeResponse( "neo4j", "secret" ) )
                .request( method, server.baseUri().resolve( path ).toString(), payload );

        // Then
        assertThat(response.status(), equalTo(200));
        ArrayNode errors = (ArrayNode) response.get("errors");
        assertThat( "Should have no errors", errors.size(), equalTo( 0 ) );
        ArrayNode results = (ArrayNode) response.get("results");
        ArrayNode data = (ArrayNode) results.get(0).get("data");
        assertThat( "Should have 4 predefined roles", data.size(), equalTo( 4 ) );
        Stream<String> values = data.findValues( "row" ).stream().map( row -> row.get(0).asText() );
        assertThat( "Expected specific roles", values.collect( Collectors.toList()),
                hasItems( "admin", "architect", "publisher", "reader") );

    }
}
