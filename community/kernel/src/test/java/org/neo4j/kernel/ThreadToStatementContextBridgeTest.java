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
package org.neo4j.kernel;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;

public class ThreadToStatementContextBridgeTest
{
    @Test(expected = NotInTransactionException.class)
    public void shouldThrowNotInTransactionExceptionWhenNotInTransaction() throws Exception
    {
        // Given
        ThreadToStatementContextBridge bridge = new ThreadToStatementContextBridge(null);

        // When
        bridge.getCtxForWriting();
    }

    @Test(expected = NotInTransactionException.class)
    public void shouldClearStateProperly() throws Exception
    {
        // Given
        ThreadToStatementContextBridge bridge = new ThreadToStatementContextBridge(null);
        bridge.setTransactionContextForThread( mock(TransactionContext.class) );

        // When
//        bridge();
//        bridge.getCtxForWriting();
    }

    @Test
    public void shouldCreateStatementContextFromGivenTransactionContext() throws Exception
    {
        // Given
        TransactionContext mockedTxContext = mock( TransactionContext.class );

        StatementContext mockedStatementCtx = mock( StatementContext.class );
        when( mockedTxContext.newStatementContext() ).thenReturn(  mockedStatementCtx );

        ThreadToStatementContextBridge bridge = new ThreadToStatementContextBridge( null );
        bridge.setTransactionContextForThread( mockedTxContext );

        // When
        StatementContext ctx = bridge.getCtxForWriting();

        // Then
        verify( mockedTxContext ).newStatementContext();
        assertEquals("Should have returned the expected statement context", mockedStatementCtx, ctx);
    }

}
