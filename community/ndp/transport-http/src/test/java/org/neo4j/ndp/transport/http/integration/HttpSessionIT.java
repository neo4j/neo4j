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
package org.neo4j.ndp.transport.http.integration;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.ndp.transport.http.util.HTTP;
import org.neo4j.ndp.transport.http.util.Neo4jWithHttp;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class HttpSessionIT
{
    @Rule
    public Neo4jWithHttp neo4j = new Neo4jWithHttp();
    private HTTP.Builder http = neo4j.client();

    @Test
    public void shouldBeAbleToCreateAndDestroySession()
    {
        // When
        HTTP.Response rs = http.POST( "/session/" );
        String sessionLocation = rs.location();

        // Then
        assertThat( rs.status(), equalTo( 201 ) );
        assertThat( sessionLocation.matches( "/session/[0-9A-Fa-f\\-]+" ), equalTo( true ) );

        // When
        rs = http.DELETE( sessionLocation );

        // Then
        assertThat( rs.status(), equalTo( 200 ) );
    }

    @Test
    public void shouldNotBeAbleToDestroyNonExistentSession()
    {
        // When
        HTTP.Response rs = http.DELETE( "/session/xxxx" );

        // Then
        assertThat( rs.status(), equalTo( 404 ) );
    }
}
