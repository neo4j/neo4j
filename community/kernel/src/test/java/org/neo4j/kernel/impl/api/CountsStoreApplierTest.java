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
package org.neo4j.kernel.impl.api;

import org.junit.Test;

import java.io.IOException;

import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.command.Command;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.api.ReadOperations.ANY_LABEL;

public class CountsStoreApplierTest
{
    private final CountsAcceptor countsAcceptor = mock( CountsAcceptor.class );
    private final NodeStore nodeStore = mock( NodeStore.class );

    @Test
    public void shouldNotifyCacheAccessOnHowManyUpdatesOnCountsWeHadSoFar() throws IOException
    {
        // GIVEN

        final CountsStoreApplier applier = new CountsStoreApplier( countsAcceptor, nodeStore );

        // WHEN
        applier.visitNodeCommand( addNodeCommand() );
        applier.apply();

        // THEN
        verify( countsAcceptor, times( 1 ) ).incrementCountsForNode( ANY_LABEL, 1 );
    }

    private Command.NodeCommand addNodeCommand()
    {
        final Command.NodeCommand command = new Command.NodeCommand();
        command.init( new NodeRecord( 1, false, 2, 3, false ), new NodeRecord( 1, false, 2, 3, true ) );
        return command;
    }
}
