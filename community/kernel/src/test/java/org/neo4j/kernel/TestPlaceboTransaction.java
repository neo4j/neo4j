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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;

public class TestPlaceboTransaction
{
    private AbstractTransactionManager mockTxManager;
    private Transaction mockTopLevelTx;
    private PlaceboTransaction placeboTx;
    private TransactionState state;
    private PropertyContainer resource;
    
    @Before
    public void before() throws Exception
    {
        mockTxManager = mock( AbstractTransactionManager.class );
        mockTopLevelTx = mock( Transaction.class );
        when( mockTxManager.getTransaction() ).thenReturn( mockTopLevelTx );
        state = mock( TransactionState.class );
        placeboTx = new PlaceboTransaction( mockTxManager, state );
        resource = mock( PropertyContainer.class );
    }

    @Test
    public void shouldRollbackParentByDefault() throws SystemException
    {
        // When
        placeboTx.finish();
        
        // Then
        verify( mockTopLevelTx ).setRollbackOnly();
    }

    @Test
    public void shouldRollbackParentIfFailureCalled() throws SystemException
    {
        // When
        placeboTx.failure();
        placeboTx.finish();
        
        // Then
        verify( mockTopLevelTx ).setRollbackOnly();
    }
    
    @Test
    public void shouldNotRollbackParentIfSuccessCalled() throws SystemException
    {
        // When
        placeboTx.success();
        placeboTx.finish();
        
        // Then
        verifyNoMoreInteractions( mockTopLevelTx );
    }
    
    @Test
    public void successCannotOverrideFailure() throws Exception
    {
        // When
        placeboTx.failure();
        placeboTx.success();
        placeboTx.finish();
        
        // Then
        verify( mockTopLevelTx ).setRollbackOnly();
    }

    @Test
    public void canAcquireReadLock() throws Exception
    {
        // when
        placeboTx.acquireReadLock( resource );
        
        // then
        verify( state ).acquireReadLock( resource );
    }

    @Test
    public void canAcquireWriteLock() throws Exception
    {
        // when
        placeboTx.acquireWriteLock( resource );
        
        // then
        verify( state ).acquireWriteLock( resource );
    }
}
