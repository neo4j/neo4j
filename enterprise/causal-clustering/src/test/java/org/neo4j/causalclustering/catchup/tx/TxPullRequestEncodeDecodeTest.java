/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.causalclustering.catchup.tx;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import org.neo4j.causalclustering.identity.StoreId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class TxPullRequestEncodeDecodeTest
{
    @Test
    public void shouldEncodeAndDecodePullRequestMessage()
    {
        // given
        EmbeddedChannel channel = new EmbeddedChannel( new TxPullRequestEncoder(), new TxPullRequestDecoder() );
        final long arbitraryId = 23;
        TxPullRequest sent = new TxPullRequest( arbitraryId, new StoreId( 1, 2, 3, 4 ) );

        // when
        channel.writeOutbound( sent );
        channel.writeInbound( new Object[]{channel.readOutbound()} );

        // then
        TxPullRequest received = (TxPullRequest) channel.readInbound();
        assertNotSame( sent, received );
        assertEquals( sent, received );
    }

}
