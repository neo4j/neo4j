/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.TopLevelTransaction.TransactionOutcome;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestPlaceboTransaction
{
    private TopLevelTransaction mockTopLevelTx;
    private Transaction placeboTx;
    private Node resource;

    @Before
    public void before() throws Exception
    {
        ThreadToStatementContextBridge bridge = mock (ThreadToStatementContextBridge.class );
        when( bridge.instance() ).thenReturn( mock( Statement.class ) );
        mockTopLevelTx = mock ( TopLevelTransaction.class);
        final TransactionOutcome outcome = new TransactionOutcome();
        when( mockTopLevelTx.getTransactionOutcome()).thenReturn(outcome);
        doAnswer(new Answer<Void>()
        {
            @Override
            public Void answer(InvocationOnMock invocation) {
                outcome.failed();
                return null;
            }})
                .when(mockTopLevelTx).failure();


        placeboTx = new PlaceboTransaction( mockTopLevelTx );
        resource = mock( Node.class );
        when(resource.getId()).thenReturn( 1l );
    }

    @Test
    public void shouldRollbackParentByDefault()
    {
        // When
        placeboTx.close();
        
        // Then
        verify( mockTopLevelTx ).failure();
    }

    @Test
    public void shouldRollbackParentIfFailureCalled() throws TransactionFailureException
    {
        // When
        placeboTx.failure();
        placeboTx.close();

        // Then
        verify( mockTopLevelTx ).failure();
    }

    @Test
    public void shouldNotRollbackParentIfSuccessCalled()
    {
        // When
        placeboTx.success();
        placeboTx.close();

        // Then
        verify( mockTopLevelTx, times( 0 ) ).failure();
    }

    @Test
    public void successCannotOverrideFailure() throws Exception
    {
        // When
        placeboTx.failure();
        placeboTx.success();
        placeboTx.close();

        // Then
        verify( mockTopLevelTx ).failure();
        verify( mockTopLevelTx, times( 0 ) ).success();
    }

    @Test
    public void canAcquireReadLock() throws Exception
    {
        // when
        placeboTx.acquireReadLock( resource );

        // then
        verify( mockTopLevelTx ).acquireReadLock( resource );
    }

    @Test
    public void canAcquireWriteLock() throws Exception
    {
        // when
        placeboTx.acquireWriteLock( resource );

        // then
        verify( mockTopLevelTx ).acquireWriteLock( resource );
    }
}
