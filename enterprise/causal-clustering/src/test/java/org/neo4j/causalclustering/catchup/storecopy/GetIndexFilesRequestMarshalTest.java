/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
