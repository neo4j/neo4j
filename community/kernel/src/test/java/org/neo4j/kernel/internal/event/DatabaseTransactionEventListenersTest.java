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

import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

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
}
