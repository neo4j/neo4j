/*
 * Copyright (c) "Neo4j"
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

import org.junit.jupiter.params.provider.Arguments;

import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.test.server.HTTP;

public abstract class ParameterizedTransactionEndpointsTestBase extends AbstractRestFunctionalTestBase
{
    protected final HTTP.Builder http = HTTP.withBaseUri( container().getBaseUri() );
    private static final String LEGACY_TX_ENDPOINT = "db/data/transaction";
    protected static final String TX_ENDPOINT = "db/neo4j/tx";

    protected static Stream<Arguments> argumentsProvider()
    {
        return Stream.of( Arguments.of( LEGACY_TX_ENDPOINT ), Arguments.of( TX_ENDPOINT ) );
    }

    public HTTP.Response POST( String uri )
    {
        return http.request( "POST", uri );
    }

    public HTTP.Response POST( String uri, HTTP.RawPayload payload )
    {
        return http.request( "POST", uri, payload );
    }

    public HTTP.Response POST( String uri, HTTP.RawPayload payload, Map<String,String> headers )
    {
        return http.request( "POST", uri, payload, headers );
    }

    public HTTP.Response DELETE( String uri )
    {
        return http.request( "DELETE", uri );
    }
}
