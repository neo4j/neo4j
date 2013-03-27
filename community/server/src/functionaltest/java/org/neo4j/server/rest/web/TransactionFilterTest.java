/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.server.rest.web;

import org.junit.Test;
import org.neo4j.server.database.TransactionRegistry;

import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class TransactionFilterTest
{

    @Test
    public void shouldStartTransactionIfNonePresent() throws Exception
    {
        // Given
        FilterChain chain = mock(FilterChain.class);
        TransactionRegistry registry = mock(TransactionRegistry.class);
        TransactionFilter filter = new TransactionFilter(registry);

        // When
        filter.doFilter(req("1337", null), res(), chain);

        // Then
        verify(registry).associateTransactionWithThread(1337l);
        verify(registry).disassociateTransactionWithThread(1337l);
        verify(chain).doFilter(any(ServletRequest.class), any(ServletResponse.class));
        verifyNoMoreInteractions(registry);
        verifyNoMoreInteractions(chain);
    }

    @Test
    public void shouldCommitTransactionWhenAsked() throws Exception
    {
        // Given
        FilterChain chain = mock(FilterChain.class);
        TransactionRegistry registry = mock(TransactionRegistry.class);
        TransactionFilter filter = new TransactionFilter(registry);

        // When
        filter.doFilter(req("1337", "COMMIT"), res(), chain);

        // Then
        verify(registry).associateTransactionWithThread(1337l);
        verify(registry).commitCurrentTransaction(1337l);
        verify(chain).doFilter(any(ServletRequest.class), any(ServletResponse.class));
        verifyNoMoreInteractions(registry);
        verifyNoMoreInteractions(chain);
    }

    @Test
    public void shouldRollbackIfAsked() throws Exception
    {
        // Given
        FilterChain chain = mock(FilterChain.class);
        TransactionRegistry registry = mock(TransactionRegistry.class);
        TransactionFilter filter = new TransactionFilter(registry);

        // When
        filter.doFilter(req("1337", "ROLLBACK"), res(), chain);

        // Then
        verify(registry).associateTransactionWithThread(1337l);
        verify(registry).rollbackCurrentTransaction(1337l);
        verifyNoMoreInteractions(registry);
        verifyNoMoreInteractions(chain);
    }

    private ServletRequest req(String sessionId, String txAction)
    {
        HttpServletRequest req = mock(HttpServletRequest.class);
        if(sessionId != null)
            when(req.getHeader(TransactionFilter.SESSION_HEADER)).thenReturn(sessionId);
        if(txAction != null)
            when(req.getHeader(TransactionFilter.TX_ACTION_HEADER)).thenReturn(txAction);
        return req;
    }

    private ServletResponse res() {
        return mock(HttpServletResponse.class);
    }

}
