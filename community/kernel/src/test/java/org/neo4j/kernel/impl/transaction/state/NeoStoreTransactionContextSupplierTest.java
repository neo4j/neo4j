/*
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
package org.neo4j.kernel.impl.transaction.state;

import org.neo4j.kernel.impl.transaction.state.NeoStoreTransactionContext;
import org.neo4j.kernel.impl.transaction.state.NeoStoreTransactionContextSupplier;

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
        NeoStoreTransactionContextSupplier supplier = new NeoStoreTransactionContextSupplier( mockNeoStore() );

        // WHEN
        NeoStoreTransactionContext retrieved = supplier.acquire();
        retrieved.close();

        // THEN
        assertEquals( retrieved, supplier.acquire() );
    }

    @Test
    public void shouldCreateNewInstanceWhenNeeded() throws Exception
    {
        // GIVEN
        NeoStoreTransactionContextSupplier supplier = new NeoStoreTransactionContextSupplier( mockNeoStore() );

        // WHEN
        NeoStoreTransactionContext firstRetrieved = supplier.acquire();
        NeoStoreTransactionContext secondRetrieved = supplier.acquire();

        // THEN
        assertNotEquals( firstRetrieved, secondRetrieved );
    }

    @Test
    public void shouldInstantiateInstancesOnlyWhenNecessary() throws Exception
    {

        // GIVEN
        NeoStoreTransactionContextSupplier supplier = new NeoStoreTransactionContextSupplier( mockNeoStore() );

        NeoStoreTransactionContext firstRetrieved = supplier.acquire();
        NeoStoreTransactionContext secondRetrieved = supplier.acquire();

        firstRetrieved.close();

        // WHEN
        NeoStoreTransactionContext thirdRetrieved = supplier.acquire();

        // THEN
        assertEquals( firstRetrieved, thirdRetrieved );
        assertNotEquals( firstRetrieved, secondRetrieved );
    }
}
