/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.rest.domain;

import static org.junit.Assert.assertEquals;

import java.net.URI;

import org.junit.Test;

public class UriToDatabaseMatcherTest
{

    @Test
    public void shouldMatchDefaults() throws Exception
    {
        UriToDatabaseMatcher matcher = new UriToDatabaseMatcher();
        matcher.add( new GraphDatabaseName( "restbucks" ) );
        matcher.add( new GraphDatabaseName( "order" ) );
        matcher.add( new GraphDatabaseName( "amazon" ) );

        assertEquals( new GraphDatabaseName( "restbucks" ),
                matcher.lookup( new URI( "http://localhost/restbucks/order/1234" ) ) );
        assertEquals( new GraphDatabaseName( "amazon" ),
                matcher.lookup( new URI( "http://www.amazon.com/amazon/product/0596805829" ) ) );
        assertEquals( new GraphDatabaseName( "order" ), matcher.lookup( new URI( "http://restbucks.com/order/1234" ) ) );
    }
}
