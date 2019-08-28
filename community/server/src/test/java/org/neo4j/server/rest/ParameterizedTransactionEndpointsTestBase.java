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

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import org.neo4j.test.server.HTTP;

import static java.util.Arrays.asList;

@RunWith( Parameterized.class )
public abstract class ParameterizedTransactionEndpointsTestBase extends AbstractRestFunctionalTestBase
{
    protected final HTTP.Builder http = HTTP.withBaseUri( server().baseUri() );
    private final boolean allowsRedirect = true;
    private static final String LEGACY_TX_ENDPOINT = "db/data/transaction";
    protected static final String TX_ENDPOINT = "db/neo4j/tx";

    @Parameterized.Parameter
    public String txUri;

    @Parameterized.Parameters
    public static Collection<String> uris()
    {
        return asList( LEGACY_TX_ENDPOINT, TX_ENDPOINT );
    }

    public HTTP.Response POST( String uri )
    {
        return http.request( "POST", uri, allowsRedirect );
    }

    public HTTP.Response POST( String uri, HTTP.RawPayload payload )
    {
        return http.request( "POST", uri, payload, allowsRedirect );
    }

    public HTTP.Response DELETE( String uri )
    {
        return http.request( "DELETE", uri, allowsRedirect );
    }
}
