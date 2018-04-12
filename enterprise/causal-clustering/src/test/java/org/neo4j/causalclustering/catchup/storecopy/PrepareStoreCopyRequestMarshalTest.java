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

public class PrepareStoreCopyRequestMarshalTest
{
    EmbeddedChannel embeddedChannel;

    @Before
    public void setup()
    {
        embeddedChannel = new EmbeddedChannel( new PrepareStoreCopyRequestEncoder(), new PrepareStoreCopyRequestDecoder() );
    }

    @Test
    public void storeIdIsTransmitted()
    {
        // given store id requests transmit store id
        StoreId storeId = new StoreId( 1, 2, 3, 4 );
        PrepareStoreCopyRequest prepareStoreCopyRequest = new PrepareStoreCopyRequest( storeId );

        // when transmitted
        sendToChannel( prepareStoreCopyRequest, embeddedChannel );

        // then it can be received/deserialised
        PrepareStoreCopyRequest prepareStoreCopyRequestRead = embeddedChannel.readInbound();
        assertEquals( prepareStoreCopyRequest.getStoreId(), prepareStoreCopyRequestRead.getStoreId() );
    }

    public static <E> void sendToChannel( E e, EmbeddedChannel embeddedChannel )
    {
        embeddedChannel.writeOutbound( e );

        ByteBuf object = embeddedChannel.readOutbound();
        embeddedChannel.writeInbound( object );
    }
}
