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
package org.neo4j.server.web;

import org.junit.jupiter.api.BeforeEach;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

public class HttpHeaderUtilsTest
{
    @Rule
    public AssertableLogProvider logProvider = new AssertableLogProvider( true );
    private HttpServletRequest request;

    @BeforeEach
    public void setUp()
    {
        request = Mockito.mock( HttpServletRequest.class );
    }

    @Test
    public void retrieveCustomTransactionTimeout()
    {
        when( request.getHeader( HttpHeaderUtils.MAX_EXECUTION_TIME_HEADER ) ).thenReturn( "100" );
        Log log = logProvider.getLog( HttpServletRequest.class );
        long transactionTimeout = HttpHeaderUtils.getTransactionTimeout( request, log );
        assertEquals( 100, transactionTimeout, "Transaction timeout should be retrieved." );
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void defaultValueWhenCustomTransactionTimeoutNotSpecified()
    {
        Log log = logProvider.getLog( HttpServletRequest.class );
        long transactionTimeout = HttpHeaderUtils.getTransactionTimeout( request, log );
        assertEquals( 0, transactionTimeout, "Transaction timeout not specified." );
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void defaultValueWhenCustomTransactionTimeoutNotANumber()
    {
        when( request.getHeader( HttpHeaderUtils.MAX_EXECUTION_TIME_HEADER ) ).thenReturn( "aa" );
        Log log = logProvider.getLog( HttpServletRequest.class );
        long transactionTimeout = HttpHeaderUtils.getTransactionTimeout( request, log );
        assertEquals( 0, transactionTimeout, "Transaction timeout not specified." );
        logProvider.assertContainsMessageContaining("Fail to parse `max-execution-time` " +
                "header with value: 'aa'. Should be a positive number.");
    }
}
