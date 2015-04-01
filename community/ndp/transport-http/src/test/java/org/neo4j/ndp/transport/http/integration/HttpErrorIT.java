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

import java.util.Arrays;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.ndp.transport.http.util.HTTP;
import org.neo4j.ndp.transport.http.util.Neo4jWithHttp;
import org.neo4j.ndp.runtime.internal.Neo4jError;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.api.exceptions.Status.Statement.InvalidSyntax;
import static org.neo4j.ndp.transport.http.util.MessageMatchers.createHttpPayload;
import static org.neo4j.ndp.transport.http.util.MessageMatchers.equalsMessages;
import static org.neo4j.ndp.transport.http.util.MessageMatchers.messages;
import static org.neo4j.ndp.transport.http.util.MessageMatchers.msgFailure;
import static org.neo4j.ndp.transport.http.util.MessageMatchers.msgIgnored;
import static org.neo4j.ndp.transport.http.util.MessageMatchers.msgSuccess;
import static org.neo4j.ndp.transport.http.util.MessageMatchers.serialize;
import static org.neo4j.ndp.messaging.v1.message.Messages.ackF;
import static org.neo4j.ndp.messaging.v1.message.Messages.run;
import static org.neo4j.ndp.messaging.v1.message.Messages.discardAll;

public class HttpErrorIT
{
    @Rule
    public Neo4jWithHttp neo4j = new Neo4jWithHttp();
    private HTTP.Builder http = neo4j.client();

    @Test
    public void shouldReturnFailureOnFailure() throws Throwable
    {
        // Given
        HTTP.Response rs = http.POST( "/session/" );
        String sessionLocation = rs.location();

        // When
        rs = http.POST( sessionLocation, messages(
                run( "syntax error" ),
                discardAll() ) );

        // Then
        assertThat( messages( rs ),  equalsMessages(
                msgFailure( new Neo4jError(InvalidSyntax,
                        "Invalid input 'y': expected 't/T' or 'e/E' (line 1, column 2 (offset: 1))\n" +
                         "\"syntax error\"\n" +
                         "  ^")),
                msgIgnored()
        ) );
    }

    @Test
    public void shouldStopIgnoringMessagesAfterErrorAck() throws Throwable
    {
        // Given
        HTTP.Response rs = http.POST( "/session/" );
        String sessionLocation = rs.location();

        // When
        rs = http.POST( sessionLocation, messages(
                run("syntax error" ),
                discardAll(),
                ackF(),
                run( "CREATE (n)" ),
                discardAll()
        ));

        // Then
        assertThat( messages( rs ),  equalsMessages(
                msgFailure( new Neo4jError(InvalidSyntax,
                        "Invalid input 'y': expected 't/T' or 'e/E' (line 1, column 2 (offset: 1))\n" +
                        "\"syntax error\"\n" +
                        "  ^")),
                msgIgnored(),
                msgSuccess(),
                msgSuccess(),
                msgSuccess()
        ) );
    }

    @Test
    public void shouldCloseSessionOnInvalidPayload() throws Throwable
    {
        // Given
        HTTP.Response rs = http.POST( "/session/" );
        String sessionLocation = rs.location();

        byte[] payload = serialize(
                run("CREATE (n)", map( "name", "Bob" ) ),
                discardAll()
        );
        payload = Arrays.copyOf(payload, payload.length / 2);

        // When
        rs = http.POST( sessionLocation, createHttpPayload( payload ) );

        // Then
        assertThat( rs.status(), equalTo(200) );
        assertThat( messages( rs ),  equalsMessages(
                msgFailure( new Neo4jError( Status.Request.Invalid,
                        "One or more malformed messages were received, please verify that your driver is of " +
                        "the latest applicable version and if not, please file a bug report. The session will be " +
                        "terminated."))
        ) );
        assertThat( http.POST( sessionLocation ).status(), equalTo(404) );
    }
}
