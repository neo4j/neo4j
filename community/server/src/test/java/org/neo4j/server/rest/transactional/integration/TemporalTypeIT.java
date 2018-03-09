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
package org.neo4j.server.rest.transactional.integration;

import org.codehaus.jackson.JsonNode;
import org.junit.Test;

import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;

public class TemporalTypeIT extends AbstractRestFunctionalTestBase
{
    @Test
    public void shouldWorkWithDateTime() throws Throwable
    {
        HTTP.Response response = runQuery( "RETURN datetime({year: 1, month:10, day:2, timezone:\\\"+01:00\\\"})" );

        assertEquals( 200, response.status() );
        assertNoErrors( response );
        JsonNode data = getSingleData( response );
        assertEquals( "0001-10-02T00:00+01:00", getSingle( data, "row" ).asText() );
        assertEquals( "datetime", getSingle( data, "meta" ).get( "type" ).asText() );
    }

    @Test
    public void shouldWorkWithTime() throws Throwable
    {
        HTTP.Response response = runQuery( "RETURN time({hour: 23, minute: 19, second: 55, timezone:\\\"-07:00\\\"})" );

        assertEquals( 200, response.status() );
        assertNoErrors( response );
        JsonNode data = getSingleData( response );
        assertEquals( "23:19:55-07:00", getSingle( data, "row" ).asText() );
        assertEquals( "time", getSingle( data, "meta" ).get( "type" ).asText() );
    }

    @Test
    public void shouldWorkWithLocalDateTime() throws Throwable
    {
        HTTP.Response response = runQuery( "RETURN localdatetime({year: 1984, month: 10, day: 21, hour: 12, minute: 34})" );

        assertEquals( 200, response.status() );
        assertNoErrors( response );
        JsonNode data = getSingleData( response );
        assertEquals( "1984-10-21T12:34", getSingle( data, "row" ).asText() );
        assertEquals( "localdatetime", getSingle( data, "meta" ).get( "type" ).asText() );
    }

    @Test
    public void shouldWorkWithDate() throws Throwable
    {
        HTTP.Response response = runQuery( "RETURN date({year: 1984, month: 10, day: 11})" );

        assertEquals( 200, response.status() );
        assertNoErrors( response );
        JsonNode data = getSingleData( response );
        assertEquals( "1984-10-11", getSingle( data, "row" ).asText() );
        assertEquals( "date", getSingle( data, "meta" ).get( "type" ).asText() );
    }

    @Test
    public void shouldWorkWithLocalTime() throws Throwable
    {
        HTTP.Response response = runQuery( "RETURN localtime({hour:12, minute:31, second:14, nanosecond: 645876123})" );

        assertEquals( 200, response.status() );
        assertNoErrors( response );
        JsonNode data = getSingleData( response );
        assertEquals( "12:31:14.645876123", getSingle( data, "row" ).asText() );
        assertEquals( "localtime", getSingle( data, "meta" ).get( "type" ).asText() );
    }

    @Test
    public void shouldWorkWithDuration() throws Throwable
    {
        HTTP.Response response = runQuery( "RETURN duration({weeks:2, days:3})" );

        assertEquals( 200, response.status() );
        assertNoErrors( response );
        JsonNode data = getSingleData( response );
        assertEquals( "P17D", getSingle( data, "row" ).asText() );
        assertEquals( "duration", getSingle( data, "meta" ).get( "type" ).asText() );
    }

    @Test
    public void shouldOnlyGetNodeTypeInMetaAsNodeProperties() throws Throwable
    {
        HTTP.Response response =
                runQuery( "CREATE (account {creationTime: localdatetime({year: 1984, month: 10, day: 21, hour: 12, minute: 34})}) RETURN account" );

        assertEquals( 200, response.status() );
        assertNoErrors( response );
        JsonNode data = getSingleData( response );

        JsonNode row = getSingle( data, "row" );
        assertThat( row.get( "creationTime" ).asText(), equalTo( "1984-10-21T12:34" ) );

        JsonNode meta = getSingle( data, "meta" );
        assertThat( meta.get( "type" ).asText(), equalTo( "node" ) );
    }

    private static JsonNode getSingleData( HTTP.Response response ) throws JsonParseException
    {
        JsonNode data = response.get( "results" ).get( 0 ).get( "data" );
        assertEquals( 1, data.size() );
        return data.get( 0 );
    }

    private static JsonNode getSingle( JsonNode node, String key )
    {
        JsonNode data = node.get( key );
        assertEquals( 1, data.size() );
        return data.get( 0 );
    }
}
