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
package org.neo4j.kernel.impl.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.database.DatabaseId;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

class ThreadToStatementContextBridgeTest
{
    private final KernelTransaction kernelTransaction = mock( KernelTransaction.class );
    private final DatabaseId testDb = randomDatabaseId();
    private ThreadToStatementContextBridge bridge;

    @BeforeEach
    void setUp()
    {
        when( kernelTransaction.getDatabaseId() ).thenReturn( testDb );
        when( kernelTransaction.getAvailabilityGuard() ).thenReturn( mock( AvailabilityGuard.class ) );

        bridge = new ThreadToStatementContextBridge();
    }

    @Test
    void bindTransaction()
    {
        assertFalse( bridge.hasTransaction() );

        bridge.bindTransactionToCurrentThread( kernelTransaction );

        assertTrue( bridge.hasTransaction() );
        assertEquals( kernelTransaction, bridge.getAndUnbindAnyTransaction() );
    }

    @Test
    void lookupBoundTransaction()
    {
        bridge.bindTransactionToCurrentThread( kernelTransaction );

        assertSame( kernelTransaction, bridge.getKernelTransactionBoundToThisThread( false, testDb ) );
        assertSame( kernelTransaction, bridge.getKernelTransactionBoundToThisThread( true, testDb ) );

        var otherDb = randomDatabaseId();
        TransactionFailureException exception =
                assertThrows( TransactionFailureException.class,
                        () -> bridge.getKernelTransactionBoundToThisThread( false,  otherDb ) );
        assertThat( exception.getMessage(),
                equalTo( format( "Fail to get transaction for database '%s', since '%s' database transaction already bound to this thread.",
                        otherDb.name(), testDb.name() ) ) );
    }

    @Test
    void bindUnbindAnyTransaction()
    {
        bridge.bindTransactionToCurrentThread( kernelTransaction );
        KernelTransaction tx = bridge.getAndUnbindAnyTransaction();

        assertFalse( bridge.hasTransaction() );
        assertSame( tx, kernelTransaction );

        bridge.bindTransactionToCurrentThread( kernelTransaction );
        assertTrue( bridge.hasTransaction() );
    }
}
