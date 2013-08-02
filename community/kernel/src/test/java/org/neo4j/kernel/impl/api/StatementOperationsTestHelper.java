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
package org.neo4j.kernel.impl.api;

import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.StatementOperationParts;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.operations.EntityReadOperations;
import org.neo4j.kernel.api.operations.EntityWriteOperations;
import org.neo4j.kernel.api.operations.KeyReadOperations;
import org.neo4j.kernel.api.operations.KeyWriteOperations;
import org.neo4j.kernel.api.operations.SchemaReadOperations;
import org.neo4j.kernel.api.operations.SchemaStateOperations;
import org.neo4j.kernel.api.operations.SchemaWriteOperations;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.impl.api.state.TxState;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

public abstract class StatementOperationsTestHelper
{
    public static StatementOperationParts mockedParts()
    {
        StatementOperationParts stmtContextParts = new StatementOperationParts(
            mock( KeyReadOperations.class ),
            mock( KeyWriteOperations.class ),
            mock( EntityReadOperations.class ),
            mock( EntityWriteOperations.class ),
            mock( SchemaReadOperations.class ),
            mock( SchemaWriteOperations.class ),
            mock( SchemaStateOperations.class ));
        return stmtContextParts;
    }
    
    public static StatementOperationParts mockedParts( KernelTransaction txContext )
    {
        StatementOperationParts mock = mockedParts();
        when( txContext.newStatementOperations() ).thenReturn( mock );
        return mock;
    }
    
    public static StatementState mockedState()
    {
        return mockedState( mock( TxState.class ) );
    }
    
    public static StatementState mockedState( final TxState txState )
    {
        StatementState state = mock( StatementState.class );
        LockHolder lockHolder = mock( LockHolder.class );
        IndexReaderFactory indexReaderFactory = mock( IndexReaderFactory.class );
        try
        {
            IndexReader indexReader = mock( IndexReader.class );
            when( indexReader.lookup( Matchers.any() ) ).thenReturn( IteratorUtil.emptyPrimitiveLongIterator() );
            when( indexReaderFactory.newReader( anyLong() ) ).thenReturn( indexReader );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new Error( e );
        }
        when( state.txState() ).thenReturn( txState );
        when( state.hasTxState() ).thenReturn( true );
        when( state.hasTxStateWithChanges() ).thenAnswer( new Answer<Boolean>() {
            @Override
            public Boolean answer( InvocationOnMock invocation ) throws Throwable
            {
                return txState.hasChanges();
            }
        } );
        when( state.locks() ).thenReturn( lockHolder );
        when( state.indexReaderFactory() ).thenReturn( indexReaderFactory );
        return state;
    }
    
    private StatementOperationsTestHelper()
    {   // Singleton
    }
}
