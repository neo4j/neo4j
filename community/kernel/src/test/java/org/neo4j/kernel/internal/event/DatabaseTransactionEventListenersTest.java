/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.internal.event;

import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.function.BiConsumer;

import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.core.RelationshipEntity;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class DatabaseTransactionEventListenersTest
{
    @Test
    void shouldUnregisterRemainingListenerOnShutdown()
    {
        //Given
        GlobalTransactionEventListeners globalListeners = mock( GlobalTransactionEventListeners.class );
        NamedDatabaseId databaseId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        DatabaseTransactionEventListeners listeners = new DatabaseTransactionEventListeners( mock( GraphDatabaseFacade.class ), globalListeners, databaseId );
        TransactionEventListener<?> firstListener = mock( TransactionEventListener.class );
        TransactionEventListener<?> secondListener = mock( TransactionEventListener.class );

        //When
        listeners.registerTransactionEventListener( firstListener );
        listeners.registerTransactionEventListener( secondListener );

        //Then
        verify( globalListeners ).registerTransactionEventListener( databaseId.name(), firstListener );
        verify( globalListeners ).registerTransactionEventListener( databaseId.name(), secondListener );
        verifyNoMoreInteractions( globalListeners );

        //When
        listeners.unregisterTransactionEventListener( firstListener );

        //Then
        verify( globalListeners ).unregisterTransactionEventListener( databaseId.name(), firstListener );
        verifyNoMoreInteractions( globalListeners );

        //When
        listeners.shutdown();

        //Then
        verify( globalListeners ).unregisterTransactionEventListener( databaseId.name(), secondListener );
        verifyNoMoreInteractions( globalListeners );
    }

    @Test
    void shouldCloseTxSnapshotAfterCommit()
    {
        shouldCloseTxSnapshot( DatabaseTransactionEventListeners::afterCommit );
    }

    @Test
    void shouldCloseTxSnapshotAfterRollback()
    {
        shouldCloseTxSnapshot( DatabaseTransactionEventListeners::afterRollback );
    }

    private void shouldCloseTxSnapshot( BiConsumer<DatabaseTransactionEventListeners,TransactionListenersState> txAction )
    {
        // Given
        GlobalTransactionEventListeners globalListeners = new GlobalTransactionEventListeners();
        NamedDatabaseId databaseId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        DatabaseTransactionEventListeners listeners = new DatabaseTransactionEventListeners( mock( GraphDatabaseFacade.class ), globalListeners, databaseId );
        TransactionEventListener<?> listener = mock( TransactionEventListener.class );
        listeners.registerTransactionEventListener( listener );

        TxState txState = new TxState();
        txState.relationshipDoCreate( 1, 2, 3, 4 );
        KernelTransaction kernelTransaction = mock( KernelTransaction.class );
        InternalTransaction internalTransaction = mock( InternalTransaction.class );
        when( kernelTransaction.memoryTracker() ).thenReturn( EmptyMemoryTracker.INSTANCE );
        when( kernelTransaction.internalTransaction() ).thenReturn( internalTransaction );
        StorageReader storageReader = mock( StorageReader.class );
        StorageRelationshipScanCursor relationshipScanCursor = mock( StorageRelationshipScanCursor.class );
        when( storageReader.allocateRelationshipScanCursor( any(), any() ) ).thenReturn( relationshipScanCursor );
        when( internalTransaction.newRelationshipEntity( anyLong() ) ).then(
                invocationOnMock -> new RelationshipEntity( internalTransaction, invocationOnMock.getArgument( 0, Long.class ) ) );

        // When
        TransactionListenersState state = listeners.beforeCommit( txState, kernelTransaction, storageReader );
        txAction.accept( listeners, state );

        // Then
        verify( relationshipScanCursor ).close();
    }
}
