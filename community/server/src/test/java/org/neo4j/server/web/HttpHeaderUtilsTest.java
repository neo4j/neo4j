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

import org.apache.http.HttpHeaders;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.neo4j.server.web.HttpHeaderUtils.MAX_EXECUTION_TIME_HEADER;
import static org.neo4j.server.web.HttpHeaderUtils.getTransactionTimeout;
import static org.neo4j.server.web.HttpHeaderUtils.isValidHttpHeaderName;

public class HttpHeaderUtilsTest
{
    @Rule
    public AssertableLogProvider logProvider = new AssertableLogProvider( true );
    private HttpServletRequest request;

    @Before
    public void setUp()
    {
        request = Mockito.mock( HttpServletRequest.class );
    }

    @Test
    public void retrieveCustomTransactionTimeout()
    {
        when( request.getHeader( MAX_EXECUTION_TIME_HEADER ) ).thenReturn( "100" );
        Log log = logProvider.getLog( HttpServletRequest.class );
        long transactionTimeout = getTransactionTimeout( request, log );
        assertEquals( "Transaction timeout should be retrieved.", 100, transactionTimeout );
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void defaultValueWhenCustomTransactionTimeoutNotSpecified()
    {
        Log log = logProvider.getLog( HttpServletRequest.class );
        long transactionTimeout = getTransactionTimeout( request, log );
        assertEquals( "Transaction timeout not specified.", 0, transactionTimeout );
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void defaultValueWhenCustomTransactionTimeoutNotANumber()
    {
        when( request.getHeader( MAX_EXECUTION_TIME_HEADER ) ).thenReturn( "aa" );
        Log log = logProvider.getLog( HttpServletRequest.class );
        long transactionTimeout = getTransactionTimeout( request, log );
        assertEquals( "Transaction timeout not specified.", 0, transactionTimeout );
        logProvider.assertContainsMessageContaining("Fail to parse `max-execution-time` " +
                "header with value: 'aa'. Should be a positive number.");
    }

    @Test
    public void shouldCheckHttpHeaders()
    {
        assertFalse( isValidHttpHeaderName( null ) );
        assertFalse( isValidHttpHeaderName( "" ) );
        assertFalse( isValidHttpHeaderName( " " ) );
        assertFalse( isValidHttpHeaderName( "      " ) );
        assertFalse( isValidHttpHeaderName( " \r " ) );
        assertFalse( isValidHttpHeaderName( " \r\n\t " ) );

        assertTrue( isValidHttpHeaderName( HttpHeaders.ACCEPT ) );
        assertTrue( isValidHttpHeaderName( HttpHeaders.ACCEPT_ENCODING ) );
        assertTrue( isValidHttpHeaderName( HttpHeaders.AGE ) );
        assertTrue( isValidHttpHeaderName( HttpHeaders.CONTENT_ENCODING ) );
        assertTrue( isValidHttpHeaderName( HttpHeaders.EXPIRES ) );
        assertTrue( isValidHttpHeaderName( HttpHeaders.IF_MATCH ) );
        assertTrue( isValidHttpHeaderName( HttpHeaders.TRANSFER_ENCODING ) );
        assertTrue( isValidHttpHeaderName( "Weird Header With Spaces" ) );

        assertFalse( isValidHttpHeaderName( "My\nHeader" ) );
        assertFalse( isValidHttpHeaderName( "Other\rStrange-Header" ) );
        assertFalse( isValidHttpHeaderName( "Header-With-Tab\t" ) );
    }
}
