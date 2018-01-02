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

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.operations.CountsOperations;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.operations.EntityWriteOperations;
import org.neo4j.kernel.impl.api.operations.ExplicitIndexReadOperations;
import org.neo4j.kernel.impl.api.operations.ExplicitIndexWriteOperations;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.kernel.impl.api.operations.KeyWriteOperations;
import org.neo4j.kernel.impl.api.operations.LockOperations;
import org.neo4j.kernel.impl.api.operations.QueryRegistrationOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaStateOperations;
import org.neo4j.kernel.impl.api.operations.SchemaWriteOperations;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.schema.IndexReader;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.primitive.PrimitiveLongResourceCollections.emptyIterator;

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
            mock( ExplicitIndexReadOperations.class ),
            mock( ExplicitIndexWriteOperations.class ),
            mock( QueryRegistrationOperations.class ) );
    }

    public static KernelStatement mockedState()
    {
        return mockedState( mock( TransactionState.class ) );
    }

    public static KernelStatement mockedState( final TransactionState txState )
    {
        KernelStatement state = mock( KernelStatement.class );
        Locks.Client locks = mock( Locks.Client.class );
        try
        {
            IndexReader indexReader = mock( IndexReader.class );
            when( indexReader.query( isA( IndexQuery.ExactPredicate.class ) ) )
                    .thenReturn( emptyIterator() );
            StorageStatement storageStatement = mock( StorageStatement.class );
            when( storageStatement.getIndexReader( any() ) ).thenReturn( indexReader );
            when( state.getStoreStatement() ).thenReturn( storageStatement );
        }
        catch ( IndexNotFoundKernelException | IndexNotApplicableKernelException e )
        {
            throw new Error( e );
        }
        when( state.txState() ).thenReturn( txState );
        when( state.hasTxStateWithChanges() ).thenAnswer( invocation -> txState.hasChanges() );
        when( state.locks() ).thenReturn( new SimpleStatementLocks( locks ) );
        when( state.readOperations() ).thenReturn( mock( ReadOperations.class ) );
        return state;
    }

    private StatementOperationsTestHelper()
    {   // Singleton
    }
}
