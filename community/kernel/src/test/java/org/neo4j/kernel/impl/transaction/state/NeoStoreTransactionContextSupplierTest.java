/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.state;

import org.neo4j.kernel.impl.transaction.state.TransactionRecordStateContext;
import org.neo4j.kernel.impl.transaction.state.TransactionRecordStateContextSupplier;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import static org.neo4j.kernel.impl.store.NeoStoreMocking.mockNeoStore;

public class NeoStoreTransactionContextSupplierTest
{
    @Test
    public void shouldReturnTheSameWhenOnlyOneExists() throws Exception
    {
        // GIVEN
        TransactionRecordStateContextSupplier supplier = new TransactionRecordStateContextSupplier( mockNeoStore() );

        // WHEN
        TransactionRecordStateContext retrieved = supplier.acquire();
        retrieved.close();

        // THEN
        assertEquals( retrieved, supplier.acquire() );
    }

    @Test
    public void shouldCreateNewInstanceWhenNeeded() throws Exception
    {
        // GIVEN
        TransactionRecordStateContextSupplier supplier = new TransactionRecordStateContextSupplier( mockNeoStore() );

        // WHEN
        TransactionRecordStateContext firstRetrieved = supplier.acquire();
        TransactionRecordStateContext secondRetrieved = supplier.acquire();

        // THEN
        assertNotEquals( firstRetrieved, secondRetrieved );
    }

    @Test
    public void shouldInstantiateInstancesOnlyWhenNecessary() throws Exception
    {

        // GIVEN
        TransactionRecordStateContextSupplier supplier = new TransactionRecordStateContextSupplier( mockNeoStore() );

        TransactionRecordStateContext firstRetrieved = supplier.acquire();
        TransactionRecordStateContext secondRetrieved = supplier.acquire();

        firstRetrieved.close();

        // WHEN
        TransactionRecordStateContext thirdRetrieved = supplier.acquire();

        // THEN
        assertEquals( firstRetrieved, thirdRetrieved );
        assertNotEquals( firstRetrieved, secondRetrieved );
    }
}
