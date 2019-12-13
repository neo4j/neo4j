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
package org.neo4j.server;

import org.hamcrest.MatcherAssert;
import org.junit.Test;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.neo4j.server.http.cypher.integration.TransactionMatchers.containsNoErrors;
import static org.neo4j.server.http.cypher.integration.TransactionMatchers.hasErrors;
import static org.neo4j.test.server.HTTP.POST;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class NeoWebServerIT extends AbstractRestFunctionalTestBase
{
    @Test
    public void shouldErrorForUnknownDatabaseViaTransactionalEndpoint()
    {
        HTTP.Response response = POST( txCommitUri( "foo" ), quotedJson( "{ 'statements': [ { 'statement': 'RETURN 1' } ] }" ) );

        MatcherAssert.assertThat( response.status(), is( 404 ) );
        MatcherAssert.assertThat( response, hasErrors( Status.Database.DatabaseNotFound ) );
    }

    @Test
    public void shouldBeAbleToRunQueryAgainstSystemDatabaseViaTransactionalEndpoint()
    {
        HTTP.Response response = POST( txCommitUri( "system" ), quotedJson( "{ 'statements': [ { 'statement': 'SHOW DEFAULT DATABASE' } ] }" ) );

        MatcherAssert.assertThat( response.status(), is( 200 ) );
        MatcherAssert.assertThat( response, containsNoErrors() );
    }

    @Test
    public void shouldRedirectRootToBrowser()
    {
        assertFalse( container().getBaseUri()
                .toString()
                .contains( "browser" ) );

        HTTP.Response res = HTTP.withHeaders( HttpHeaders.ACCEPT, MediaType.TEXT_HTML ).GET( container().getBaseUri().toString() );
        assertThat( res.header( "Location" ), containsString( "browser") );
    }
}
