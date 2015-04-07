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
import org.neo4j.ndp.messaging.v1.message.Messages;
import org.neo4j.runtime.internal.runner.StreamMatchers;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.ndp.messaging.v1.util.MessageMatchers.equalsMessages;
import static org.neo4j.ndp.messaging.v1.util.MessageMatchers.messages;
import static org.neo4j.ndp.messaging.v1.util.MessageMatchers.msgRecord;
import static org.neo4j.ndp.messaging.v1.util.MessageMatchers.msgSuccess;
import static org.neo4j.ndp.messaging.v1.message.Messages.pullAll;
import static org.neo4j.ndp.messaging.v1.message.Messages.run;
import static org.neo4j.ndp.messaging.v1.message.Messages.success;
import static org.neo4j.ndp.messaging.v1.util.MessageMatchers.serialize;
import static org.neo4j.ndp.transport.http.util.HTTP.createHttpPayload;
import static org.neo4j.stream.Records.record;

public class HttpTransportIT
{
    @Rule
    public Neo4jWithHttp neo4j = new Neo4jWithHttp();
    private HTTP.Builder http = neo4j.client();

    @Test
    public void shouldBeAbleToSubmitRunRequest() throws Throwable
    {
        // Given
        HTTP.Response rs = http.POST( "/session/" );
        String sessionLocation = rs.location();

        // When
        rs = http.POST( sessionLocation, createHttpPayload( serialize(
                run( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ),
                pullAll() )));

        // Then
        assertThat( messages( rs.rawContent() ), equalTo( asList(
                success( map( "fields", asList("a", "a_squared") ) ),
                Messages.record( record( 1l, 1l ) ),
                Messages.record( record( 2l, 4l ) ),
                Messages.record( record( 3l, 9l ) ),
                success( map() )
        ) ) );
    }

    @Test
    public void shouldBeAbleToSendMultipleHttpRequests() throws Throwable
    {
        // Given
        HTTP.Response rs = http.POST( "/session/" );
        String sessionLocation = rs.location();

        // Given I've sent one slew of messages
        http.POST( sessionLocation,  createHttpPayload( serialize(
                run( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ),
                pullAll() )));

        // When I send a second slew of messages
        rs = http.POST( sessionLocation, createHttpPayload( serialize(
                run( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ),
                pullAll() )));

        // Then
        assertThat( messages( rs.rawContent() ), equalTo( asList(
                success( map( "fields", asList("a", "a_squared") ) ),

                Messages.record( record( 1l, 1l ) ),
                Messages.record( record( 2l, 4l ) ),
                Messages.record( record( 3l, 9l ) ),
                success( map() )
        ) ) );
    }

    @Test
    public void shouldBeAbleToSendStringArrayParameters() throws Throwable
    {
        // Given
        HTTP.Response rs = http.POST( "/session/" );
        String sessionLocation = rs.location();

        // Given I've sent one slew of messages
        String[] arrayValue = new String[]{"Mjölnir", "Mjölnir", "Mjölnir"};
        rs = http.POST( sessionLocation,  createHttpPayload( serialize(
                run("CREATE (a {value:{value}}) RETURN a.value", map( "value", arrayValue ) ),
                pullAll() )));

        // Then
        assertThat( messages( rs.rawContent() ), equalsMessages(
                msgSuccess(),

                msgRecord( StreamMatchers.eqRecord( equalTo( asList( arrayValue ) ) ) ),
                msgSuccess()
        ) );
    }
}