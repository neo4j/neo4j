/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.internal.connector.http;

import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.impl.DefaultBHttpClientConnection;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URI;
import java.util.Collections;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.internal.logging.DevNullLogger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HttpConnectionTest
{

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private final DefaultBHttpClientConnection rawConn = mock( DefaultBHttpClientConnection.class );
    private final HttpResponse sessionCreated = new BasicHttpResponse( new BasicStatusLine( HttpVersion.HTTP_1_1, 201,
            "Created" ) );
    private final DevNullLogger log = new DevNullLogger();


    @Test
    public void shouldEstablishSession() throws Throwable
    {
        // Given
        when( rawConn.receiveResponseHeader() ).thenReturn( sessionCreated );

        // When
        new HttpConnection( URI.create( "neo4j://localhost" ), log, 7687, rawConn );

        // Then
        verify( rawConn ).receiveResponseHeader();
    }

    @Test
    public void shouldThrowOnNon200Response() throws Throwable
    {
        // Given
        HttpResponse failedResponse = new BasicHttpResponse( new BasicStatusLine( HttpVersion.HTTP_1_1, 500,
                "Server error" ) );

        when( rawConn.receiveResponseHeader() ).thenReturn( sessionCreated, failedResponse );

        HttpConnection conn = new HttpConnection( URI.create( "neo4j://localhost" ), log, 7687, rawConn );

        // Expect
        exception.expect( DatabaseException.class );
        exception.expectMessage( "A fatal transport error occurred: 'Server error', " +
                                 "please refer to the database logs for details." );

        // When
        conn.run( "somethibg", Collections.EMPTY_MAP, null );
        conn.sync();

    }

    @Test
    public void shouldThrowOn400Response() throws Throwable
    {
        // Given
        HttpResponse failedResponse = new BasicHttpResponse( new BasicStatusLine( HttpVersion.HTTP_1_1, 400,
                "You nasty bugger" ) );

        when( rawConn.receiveResponseHeader() ).thenReturn( sessionCreated, failedResponse );

        HttpConnection conn = new HttpConnection( URI.create( "neo4j://localhost" ), log, 7687, rawConn );

        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "A fatal transport error occurred: 'You nasty bugger', " +
                                 "please refer to the database logs for details." );

        // When
        conn.run( "somethibg", Collections.EMPTY_MAP, null );
        conn.sync();
    }

    @Test
    public void shouldThrowHelpfulErrorOn404() throws Throwable
    {
        // Given
        HttpResponse failedResponse = new BasicHttpResponse( new BasicStatusLine( HttpVersion.HTTP_1_1, 404,
                "AFK" ) );

        when( rawConn.receiveResponseHeader() ).thenReturn( sessionCreated, failedResponse );

        HttpConnection conn = new HttpConnection( URI.create( "neo4j://localhost" ), log, 7687, rawConn );

        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Session is no longer available. This is most often caused by the session timing " +
                                 "out due to idleness, but can also be caused by a previous fatal error." );

        // When
        conn.run( "somethibg", Collections.EMPTY_MAP, null );
        conn.sync();
    }

    @Before
    public void setup()
    {
        sessionCreated.setHeader( "Location", "/session/asd" );
        when( rawConn.isOpen() ).thenReturn( true );
    }
}