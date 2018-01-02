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
package org.neo4j.kernel.impl.api;

import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.operations.CountsOperations;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.operations.EntityWriteOperations;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.kernel.impl.api.operations.KeyWriteOperations;
import org.neo4j.kernel.impl.api.operations.LegacyIndexReadOperations;
import org.neo4j.kernel.impl.api.operations.LegacyIndexWriteOperations;
import org.neo4j.kernel.impl.api.operations.LockOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaStateOperations;
import org.neo4j.kernel.impl.api.operations.SchemaWriteOperations;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class StatementOperationsTestHelper
{
    public static StatementOperationParts mockedParts()
    {
        return new StatementOperationParts(
            mock( KeyReadOperations.class ),
            mock( KeyWriteOperations.class ),
            mock( EntityReadOperations.class ),
            mock( EntityWriteOperations.class ),
            mock( SchemaReadOperations.class ),
            mock( SchemaWriteOperations.class ),
            mock( SchemaStateOperations.class ),
            mock( LockOperations.class ),
            mock( CountsOperations.class ),
            mock( LegacyIndexReadOperations.class ),
            mock( LegacyIndexWriteOperations.class ) );
    }

    public static KernelStatement mockedState()
    {
        return mockedState( mock( TransactionState.class ) );
    }

    public static KernelStatement mockedState( final TransactionState txState )
    {
        KernelStatement state = mock( KernelStatement.class );
        Locks.Client lockHolder = mock( Locks.Client.class );
        try
        {
            IndexReader indexReader = mock( IndexReader.class );
            when( indexReader.seek( Matchers.any() ) ).thenReturn( PrimitiveLongCollections.emptyIterator() );
            when( state.getIndexReader( Matchers.<IndexDescriptor>any() ) ).thenReturn( indexReader );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new Error( e );
        }
        when( state.txState() ).thenReturn( txState );
        when( state.hasTxStateWithChanges() ).thenAnswer( new Answer<Boolean>() {
            @Override
            public Boolean answer( InvocationOnMock invocation ) throws Throwable
            {
                return txState.hasChanges();
            }
        } );
        when( state.locks() ).thenReturn( new SimpleStatementLocks( lockHolder ) );
        when( state.readOperations() ).thenReturn( mock( ReadOperations.class ) );
        return state;
    }

    private StatementOperationsTestHelper()
    {   // Singleton
    }
}
