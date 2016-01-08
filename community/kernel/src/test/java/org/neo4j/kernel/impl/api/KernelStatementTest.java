/*
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
package org.neo4j.kernel.impl.api;

import org.junit.Test;

import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.proc.Procedures;
import org.neo4j.storageengine.api.StorageStatement;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KernelStatementTest
{
    @Test(expected = TransactionTerminatedException.class)
    public void shouldThrowTerminateExceptionWhenTransactionTerminated() throws Exception
    {
        KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
        when( transaction.shouldBeTerminated() ).thenReturn( true );

        KernelStatement statement = new KernelStatement(
            transaction, null, null, null, null, new Procedures() );

        statement.readOperations().nodeExists( 0 );
    }

    @Test
    public void shouldCloseStorageStatementWhenForceClosed() throws Exception
    {
        // given
        StorageStatement storeStatement = mock( StorageStatement.class );
        KernelStatement statement = new KernelStatement( mock( KernelTransactionImplementation.class ),
                null, null, null, storeStatement, new Procedures() );

        // when
        statement.forceClose();

        // then
        verify( storeStatement ).close();
    }
}
