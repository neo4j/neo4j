/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.server.rest.transactional;

import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TransactionFacadeTest
{
    @Test
    public void baseUrlMustAddMissingDbDataPrefix() throws URISyntaxException
    {
        URI requestUri = new URI( "http://localhost:7474/" );
        URI baseUri = TransactionFacade.baseUri( requestUri );
        URI expectedUri = new URI( "http://localhost:7474/db/data" );
        assertThat( baseUri, is( expectedUri ) );
    }

    @Test
    public void baseUrlMustReduceOverqualifiedRequestUri() throws URISyntaxException
    {
        URI requestUri = new URI( "http://localhost:7474/db/data/transaction/commit" );
        URI baseUri = TransactionFacade.baseUri( requestUri );
        URI expectedUri = new URI( "http://localhost:7474/db/data" );
        assertThat( baseUri, is( expectedUri ) );
    }

    @Test
    public void baseUrlMustRetainHttpsSchema() throws URISyntaxException
    {
        URI requestUri = new URI( "https://localhost:7474/" );
        URI baseUri = TransactionFacade.baseUri( requestUri );
        URI expectedUri = new URI( "https://localhost:7474/db/data" );
        assertThat( baseUri, is( expectedUri ) );
    }

    @Test
    public void baseUrlMustRetainHost() throws URISyntaxException
    {
        URI requestUri = new URI( "http://dbserver.com:7474/" );
        URI baseUri = TransactionFacade.baseUri( requestUri );
        URI expectedUri = new URI( "http://dbserver.com:7474/db/data" );
        assertThat( baseUri, is( expectedUri ) );
    }

    @Test
    public void baseUrlMustRetainPort() throws URISyntaxException
    {
        URI requestUri = new URI( "http://localhost:7484/" );
        URI baseUri = TransactionFacade.baseUri( requestUri );
        URI expectedUri = new URI( "http://localhost:7484/db/data" );
        assertThat( baseUri, is( expectedUri ) );
    }

    @Test
    public void baseUrlMustRetainUserInfo() throws URISyntaxException
    {
        URI requestUri = new URI( "http://username:password@localhost:7484/" );
        URI baseUri = TransactionFacade.baseUri( requestUri );
        URI expectedUri = new URI( "http://username:password@localhost:7484/db/data" );
        assertThat( baseUri, is( expectedUri ) );
    }
}
