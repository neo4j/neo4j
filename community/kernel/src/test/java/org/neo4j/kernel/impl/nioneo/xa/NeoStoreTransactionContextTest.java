/**
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
package org.neo4j.kernel.impl.nioneo.xa;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.kernel.impl.nioneo.store.NeoStoreMocking.mockNeoStore;

import org.junit.Test;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;

public class NeoStoreTransactionContextTest
{
    @Test
    public void shouldClearRecordSetsOnClose() throws Exception
    {
        // GIVEN
        NeoStore mockStore = mockNeoStore();
        NeoStoreTransactionContextSupplier supplier = new NeoStoreTransactionContextSupplier( mockStore );

        NeoStoreTransactionContext toClose = new NeoStoreTransactionContext(
                supplier, mockStore );

        toClose.getNodeCommands().put( 1l, mock( Command.NodeCommand.class ) );
        toClose.setNeoStoreCommand( mock( Command.NeoStoreCommand.class ) );

        // WHEN
        toClose.close();

        // THEN
        assertTrue( toClose.getNodeCommands().isEmpty() );
        assertNull( toClose.getNeoStoreCommand().getRecord() );
    }

    @Test
    public void shouldClearBindingsOnClose() throws Exception
    {
        // GIVEN
        NeoStore mockStore = mockNeoStore();
        NeoStoreTransactionContextSupplier supplier = new NeoStoreTransactionContextSupplier( mockStore );

        NeoStoreTransactionContext toClose = new NeoStoreTransactionContext(
                supplier, mockStore );
        toClose.bind( mock( TransactionState.class ) );

        // WHEN
        toClose.close();

        // THEN
        assertNull( toClose.getTransactionState() );
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
