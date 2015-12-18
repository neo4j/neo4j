/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.catchup.tx;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import org.neo4j.coreedge.catchup.CatchupClientProtocol;
import org.neo4j.coreedge.catchup.tx.edge.TxPullResponseDecoder;
import org.neo4j.coreedge.catchup.tx.edge.TxPullResponse;
import org.neo4j.coreedge.catchup.tx.core.TxPullResponseEncoder;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import static org.neo4j.coreedge.catchup.CatchupClientProtocol.NextMessage;

public class TxPullResponseEncodeDecodeTest
{
    @Test
    public void shouldEncodeAndDecodePullResponseMessage()
    {
        CatchupClientProtocol protocol = new CatchupClientProtocol();
        protocol.expect( NextMessage.TX_PULL_RESPONSE );

        EmbeddedChannel channel = new EmbeddedChannel(
                new TxPullResponseEncoder(),
                new TxPullResponseDecoder( protocol ) );

        // given
        TxPullResponse sent = new TxPullResponse( new StoreId(), newCommittedTransactionRepresentation() );

        // when
        channel.writeOutbound( sent );
        channel.writeInbound( channel.readOutbound() );

        // then
        TxPullResponse received = (TxPullResponse) channel.readInbound();
        assertNotSame( sent, received );
        assertEquals( sent, received );
    }

    private CommittedTransactionRepresentation newCommittedTransactionRepresentation()
    {
        final long arbitraryRecordId = 27l;
        Command.NodeCommand command =
                new Command.NodeCommand( new NodeRecord( arbitraryRecordId ), new NodeRecord( arbitraryRecordId ) );

        PhysicalTransactionRepresentation physicalTransactionRepresentation =
                new PhysicalTransactionRepresentation( asList( new LogEntryCommand( command ).getXaCommand() ) );
        physicalTransactionRepresentation.setHeader( new byte[]{}, 0, 0, 0, 0, 0, 0 );

        LogEntryStart startEntry = new LogEntryStart( 0, 0, 0l, 0l, new byte[]{}, LogPosition.UNSPECIFIED );
        OnePhaseCommit commitEntry = new OnePhaseCommit( 42, 0 );

        return new CommittedTransactionRepresentation( startEntry, physicalTransactionRepresentation, commitEntry );
    }

}
