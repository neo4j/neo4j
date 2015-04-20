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

import org.junit.Test;

import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.transaction.state.NeoStoreTransactionContext;
import org.neo4j.kernel.impl.transaction.state.NeoStoreTransactionContextSupplier;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import static org.neo4j.kernel.impl.store.NeoStoreMocking.mockNeoStore;

public class NeoStoreTransactionContextTest
{
    @Test
    public void shouldClearRecordSetsOnClose() throws Exception
    {
        // GIVEN
        NeoStore mockStore = mockNeoStore();
        NeoStoreTransactionContextSupplier supplier = new NeoStoreTransactionContextSupplier( mockStore );
        NeoStoreTransactionContext toClose = new NeoStoreTransactionContext( supplier, mockStore );

        toClose.getNodeRecords().create( 1L, null ).forChangingData();
        toClose.getRelGroupRecords().create( 2L, 1 ).forChangingData();
        assertEquals( 1, toClose.getNodeRecords().changeSize() );
        assertEquals( 0, toClose.getPropertyRecords().changeSize() );
        assertEquals( 1, toClose.getRelGroupRecords().changeSize() );

        // WHEN
        toClose.close();

        // THEN
        assertEquals( 0, toClose.getNodeRecords().changeSize() );
        assertEquals( 0, toClose.getRelGroupRecords().changeSize() );
    }

    @Test
    public void shouldCallReleaseOnClose() throws Exception
    {
        // GIVEN
        NeoStore mockStore = mockNeoStore();
        NeoStoreTransactionContextSupplier supplier = spy( new NeoStoreTransactionContextSupplier( mockStore ) );

        NeoStoreTransactionContext toClose = new NeoStoreTransactionContext(
                supplier, mockStore );

        // WHEN
        toClose.close();

        // THEN
        verify( supplier, times( 1 ) ).release( toClose );
        verifyNoMoreInteractions( supplier );
    }
}
