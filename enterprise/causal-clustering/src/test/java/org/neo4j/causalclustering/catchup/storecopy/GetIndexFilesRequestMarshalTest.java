/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.causalclustering.identity.StoreId;

import static org.junit.Assert.assertEquals;

public class GetIndexFilesRequestMarshalTest
{
    private EmbeddedChannel embeddedChannel;

    @Before
    public void setup()
    {
        embeddedChannel = new EmbeddedChannel( new GetIndexFilesRequest.Encoder(), new GetIndexFilesRequest.Decoder() );
    }

    private static final StoreId expectedStore = new StoreId( 1, 2, 3, 4 );
    private static final long exepctedIndexId = 13;
    private static final Long expectedLastTransaction = 1234L;

    @Test
    public void getsTransmitted()
    {
        // given
        GetIndexFilesRequest expectedIndexSnapshotRequest = new GetIndexFilesRequest( expectedStore, exepctedIndexId, expectedLastTransaction );

        // when
        sendToChannel( expectedIndexSnapshotRequest, embeddedChannel );

        // then
        GetIndexFilesRequest actualIndexRequest = embeddedChannel.readInbound();
        assertEquals( expectedStore, actualIndexRequest.expectedStoreId() );
        assertEquals( exepctedIndexId, actualIndexRequest.indexId() );
        assertEquals( expectedLastTransaction.longValue(), actualIndexRequest.requiredTransactionId() );
    }

    private static void sendToChannel( GetIndexFilesRequest expectedIndexSnapshotRequest, EmbeddedChannel embeddedChannel )
    {
        embeddedChannel.writeOutbound( expectedIndexSnapshotRequest );

        ByteBuf object = embeddedChannel.readOutbound();
        embeddedChannel.writeInbound( object );
    }
}
