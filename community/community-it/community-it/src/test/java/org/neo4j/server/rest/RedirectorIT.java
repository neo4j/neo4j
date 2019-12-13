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

import org.junit.Test;

import java.net.URI;
import java.net.http.HttpRequest;

import static java.net.http.HttpClient.newHttpClient;
import static java.net.http.HttpResponse.BodyHandlers.discarding;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class RedirectorIT extends AbstractRestFunctionalTestBase
{
    @Test
    public void shouldRedirectRootToBrowser() throws Exception
    {
        var request = HttpRequest.newBuilder( container().getBaseUri() ).GET().build();

        var response = newHttpClient().send( request, discarding() );

        assertThat( response.statusCode(), is( not( 404 ) ) );
    }

    @Test
    public void shouldNotRedirectTheRestOfTheWorld() throws Exception
    {
        var uri = URI.create( container().getBaseUri() + "a/different/relative/data/uri/" );
        var request = HttpRequest.newBuilder( uri ).GET().build();

        var response = newHttpClient().send( request, discarding() );

        assertThat( response.statusCode(), is( 404 ) );
    }
}
