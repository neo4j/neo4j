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
import org.neo4j.coreedge.catchup.tx.edge.TxStreamFinishedResponseDecoder;
import org.neo4j.coreedge.catchup.tx.edge.TxStreamFinishedResponseEncoder;
import org.neo4j.coreedge.catchup.tx.core.TxStreamFinishedResponse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import static org.neo4j.coreedge.catchup.CatchupClientProtocol.NextMessage;

public class TxStreamFinishedResponseEncodeDecodeTest
{
    @Test
    public void shouldEncodeAndDecodePullRequestMessage()
    {
        CatchupClientProtocol protocol = new CatchupClientProtocol();
        protocol.expect( NextMessage.TX_STREAM_FINISHED );

        EmbeddedChannel channel = new EmbeddedChannel( new TxStreamFinishedResponseEncoder(),
                new TxStreamFinishedResponseDecoder( protocol ) );

        // given
        final long arbitraryId = 23;
        TxStreamFinishedResponse sent = new TxStreamFinishedResponse( arbitraryId );

        // when
        channel.writeOutbound( sent );
        channel.writeInbound( channel.readOutbound() );

        // then
        TxStreamFinishedResponse received = (TxStreamFinishedResponse) channel.readInbound();
        assertNotSame( sent, received );
        assertEquals( sent, received );
    }

}
