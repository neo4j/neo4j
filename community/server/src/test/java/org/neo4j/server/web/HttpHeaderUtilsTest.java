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
package org.neo4j.server.web;

import org.eclipse.jetty.http.HttpHeader;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.server.web.HttpHeaderUtils.MAX_EXECUTION_TIME_HEADER;
import static org.neo4j.server.web.HttpHeaderUtils.getTransactionTimeout;
import static org.neo4j.server.web.HttpHeaderUtils.isValidHttpHeaderName;

public class HttpHeaderUtilsTest
{
    public final AssertableLogProvider logProvider = new AssertableLogProvider( true );
    private final HttpServletRequest request = mock( HttpServletRequest.class );

    @Test
    void retrieveCustomTransactionTimeout()
    {
        when( request.getHeader( MAX_EXECUTION_TIME_HEADER ) ).thenReturn( "100" );
        Log log = logProvider.getLog( HttpServletRequest.class );
        long transactionTimeout = getTransactionTimeout( request, log );
        assertEquals( 100, transactionTimeout, "Transaction timeout should be retrieved." );
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    void defaultValueWhenCustomTransactionTimeoutNotSpecified()
    {
        Log log = logProvider.getLog( HttpServletRequest.class );
        long transactionTimeout = getTransactionTimeout( request, log );
        assertEquals( 0, transactionTimeout, "Transaction timeout not specified." );
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    void defaultValueWhenCustomTransactionTimeoutNotANumber()
    {
        when( request.getHeader( MAX_EXECUTION_TIME_HEADER ) ).thenReturn( "aa" );
        Log log = logProvider.getLog( HttpServletRequest.class );
        long transactionTimeout = getTransactionTimeout( request, log );
        assertEquals( 0, transactionTimeout, "Transaction timeout not specified." );
        logProvider.rawMessageMatcher().assertContains("Fail to parse `max-execution-time` " +
                "header with value: 'aa'. Should be a positive number.");
    }

    @Test
    void shouldCheckHttpHeaders()
    {
        assertFalse( isValidHttpHeaderName( null ) );
        assertFalse( isValidHttpHeaderName( "" ) );
        assertFalse( isValidHttpHeaderName( " " ) );
        assertFalse( isValidHttpHeaderName( "      " ) );
        assertFalse( isValidHttpHeaderName( " \r " ) );
        assertFalse( isValidHttpHeaderName( " \r\n\t " ) );

        assertTrue( isValidHttpHeaderName( HttpHeader.ACCEPT.toString() ) );
        assertTrue( isValidHttpHeaderName( HttpHeader.ACCEPT_ENCODING.toString() ) );
        assertTrue( isValidHttpHeaderName( HttpHeader.AGE.toString() ) );
        assertTrue( isValidHttpHeaderName( HttpHeader.CONTENT_ENCODING.toString() ) );
        assertTrue( isValidHttpHeaderName( HttpHeader.EXPIRES.toString() ) );
        assertTrue( isValidHttpHeaderName( HttpHeader.IF_MATCH.toString() ) );
        assertTrue( isValidHttpHeaderName( HttpHeader.TRANSFER_ENCODING.toString() ) );
        assertTrue( isValidHttpHeaderName( "Weird Header With Spaces" ) );

        assertFalse( isValidHttpHeaderName( "My\nHeader" ) );
        assertFalse( isValidHttpHeaderName( "Other\rStrange-Header" ) );
        assertFalse( isValidHttpHeaderName( "Header-With-Tab\t" ) );
    }
}
