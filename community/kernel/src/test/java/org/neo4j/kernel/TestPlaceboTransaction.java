/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;

import static org.mockito.Mockito.*;

public class TestPlaceboTransaction
{
    private Transaction mockTopLevelTx;
    private PlaceboTransaction placeboTx;
    private Node resource;
    private PersistenceManager persistenceManager;
    private ReadOperations readOps;

    @Before
    public void before() throws Exception
    {
        AbstractTransactionManager mockTxManager = mock( AbstractTransactionManager.class );
        mockTopLevelTx = mock( Transaction.class );
        when( mockTxManager.getTransaction() ).thenReturn( mockTopLevelTx );
        persistenceManager = mock( PersistenceManager.class );

        ThreadToStatementContextBridge stmtProvider = mock( ThreadToStatementContextBridge.class );
        Statement stmt = mock( Statement.class );
        readOps = mock(ReadOperations.class);

        when( stmtProvider.instance()).thenReturn( stmt );
        when( stmt.readOperations()).thenReturn( readOps );

        placeboTx = new PlaceboTransaction( mockTxManager, stmtProvider );
        resource = mock( Node.class );
        when(resource.getId()).thenReturn( 1l );
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
        verify( readOps ).acquireShared( ResourceTypes.NODE, 1l );
    }

    @Test
    public void canAcquireWriteLock() throws Exception
    {
        // when
        placeboTx.acquireWriteLock( resource );
        
        // then
        verify( readOps ).acquireExclusive( ResourceTypes.NODE, 1l );
    }
}
