/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt.txtracking;

import org.junit.jupiter.api.Test;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.impl.store.StoreFileClosedException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.storageengine.api.TransactionIdStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

class SimpleReconciledTransactionTrackerTest
{
    private final GraphDatabaseAPI systemDb = mock( GraphDatabaseAPI.class );
    private final DatabaseManagementService managementService = mock( DatabaseManagementService.class );
    private final ReconciledTransactionTracker tracker = new SimpleReconciledTransactionTracker( managementService, NullLogService.getInstance() );

    @Test
    void shouldNotSupportInitialization()
    {
        assertThrows( UnsupportedOperationException.class, () -> tracker.initialize( 42 ) );
    }

    @Test
    void shouldNotSupportUpdates()
    {
        assertThrows( UnsupportedOperationException.class, () -> tracker.setLastReconciledTransactionId( 42 ) );
    }

    @Test
    void shouldReturnDummyTransactionIdWhenSystemDatabaseNotAvailable()
    {
        when( managementService.database( SYSTEM_DATABASE_NAME ) ).thenReturn( systemDb );
        when( systemDb.isAvailable( anyLong() ) ).thenReturn( false );

        assertEquals( -1, tracker.getLastReconciledTransactionId() );
    }

    @Test
    void shouldReturnDummyTransactionIdWhenSystemDatabaseDoesNotExist()
    {
        when( managementService.database( SYSTEM_DATABASE_NAME ) ).thenThrow( DatabaseNotFoundException.class );

        assertEquals( -1, tracker.getLastReconciledTransactionId() );
    }

    @Test
    void shouldReturnDummyTransactionIdWhenTransactionIdStoreForSystemDatabaseClosed()
    {
        when( managementService.database( SYSTEM_DATABASE_NAME ) ).thenReturn( systemDb );
        when( systemDb.isAvailable( anyLong() ) ).thenReturn( true );
        var dependencyResolver = mock( DependencyResolver.class );
        when( systemDb.getDependencyResolver() ).thenReturn( dependencyResolver );
        var txIdStore = mock( TransactionIdStore.class );
        when( dependencyResolver.resolveDependency( TransactionIdStore.class ) ).thenReturn( txIdStore );
        when( txIdStore.getLastClosedTransactionId() ).thenThrow( StoreFileClosedException.class );

        assertEquals( -1, tracker.getLastReconciledTransactionId() );
    }

    @Test
    void shouldReturnLastClosedTransactionIdFromSystemDatabase()
    {
        when( managementService.database( SYSTEM_DATABASE_NAME ) ).thenReturn( systemDb );
        when( systemDb.isAvailable( anyLong() ) ).thenReturn( true );
        var dependencyResolver = mock( DependencyResolver.class );
        when( systemDb.getDependencyResolver() ).thenReturn( dependencyResolver );
        var txIdStore = mock( TransactionIdStore.class );
        when( dependencyResolver.resolveDependency( TransactionIdStore.class ) ).thenReturn( txIdStore );
        when( txIdStore.getLastClosedTransactionId() ).thenReturn( 42L );

        assertEquals( 42, tracker.getLastReconciledTransactionId() );
    }
}
