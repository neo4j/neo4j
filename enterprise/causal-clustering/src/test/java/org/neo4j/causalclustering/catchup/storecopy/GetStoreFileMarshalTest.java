/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import org.neo4j.causalclustering.identity.StoreId;

import static org.junit.Assert.assertEquals;

public class GetStoreFileMarshalTest
{
    EmbeddedChannel embeddedChannel;

    @Before
    public void setup()
    {
        embeddedChannel = new EmbeddedChannel( new GetStoreFileRequest.Encoder(), new GetStoreFileRequest.Decoder() );
    }

    private static final StoreId expectedStore = new StoreId( 1, 2, 3, 4 );
    private static final File expectedFile = new File( "abc.123" );
    private static final Long expectedLastTransaction = 1234L;

    @Test
    public void getsTransmitted()
    {
        // given
        GetStoreFileRequest expectedStoreRequest = new GetStoreFileRequest( expectedStore, expectedFile, expectedLastTransaction );

        // when
        sendToChannel( expectedStoreRequest, embeddedChannel );

        // then
        GetStoreFileRequest actualStoreRequest = embeddedChannel.readInbound();
        assertEquals( expectedStore, actualStoreRequest.expectedStoreId() );
        assertEquals( expectedFile, actualStoreRequest.file() );
        assertEquals( expectedLastTransaction.longValue(), actualStoreRequest.requiredTransactionId() );
    }

    private static void sendToChannel( GetStoreFileRequest getStoreFileRequest, EmbeddedChannel embeddedChannel )
    {
        embeddedChannel.writeOutbound( getStoreFileRequest );

        ByteBuf object = embeddedChannel.readOutbound();
        embeddedChannel.writeInbound( object );
    }
}
