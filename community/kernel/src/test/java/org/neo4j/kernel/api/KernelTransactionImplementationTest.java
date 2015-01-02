/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.api;

import org.junit.Test;

import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.LockHolder;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;

import static org.mockito.Mockito.mock;

public class KernelTransactionImplementationTest
{

    private AbstractTransactionManager txm = mock( AbstractTransactionManager.class );

    @Test
    public void shouldBeAbleToRollbackPreparedTransaction() throws Exception
    {
        // given
        KernelTransactionImplementation tx = new KernelTransactionImplementation( null, null, false, null, null,
                null, txm, null, null, mock(LockHolder.class), null, null, mock( NeoStore.class ),
                mock(TransactionState.class) );
        // when
        tx.prepare();

        // then (no exception)
        tx.rollback();
    }
}
