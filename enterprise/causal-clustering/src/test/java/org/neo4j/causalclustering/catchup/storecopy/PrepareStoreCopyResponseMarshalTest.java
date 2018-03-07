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

import java.io.File;
import java.util.stream.Stream;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;

import static org.junit.Assert.assertEquals;

public class PrepareStoreCopyResponseMarshalTest
{
    private EmbeddedChannel embeddedChannel;

    @Before
    public void setup()
    {
        embeddedChannel = new EmbeddedChannel( new PrepareStoreCopyResponse.Encoder(), new PrepareStoreCopyResponse.Decoder() );
    }

    @Test
    public void transactionIdGetsTransmitted()
    {
        // given
        long transactionId = Long.MAX_VALUE;

        // when a transaction id is serialised
        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.success( new File[0], Primitive.longSet(), transactionId );
        sendToChannel( prepareStoreCopyResponse, embeddedChannel );

        // then it can be deserialised
        PrepareStoreCopyResponse readPrepareStoreCopyResponse = embeddedChannel.readInbound();
        assertEquals( prepareStoreCopyResponse.lastTransactionId(), readPrepareStoreCopyResponse.lastTransactionId() );
    }

    @Test
    public void fileListGetsTransmitted()
    {
        // given
        File[] files =
                new File[]{new File( "File a.txt" ), new File( "file-b" ), new File( "aoifnoasndfosidfoisndfoisnodainfsonidfaosiidfna" ), new File( "" )};

        // when
        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.success( files, Primitive.longSet(), 0L );
        sendToChannel( prepareStoreCopyResponse, embeddedChannel );

        // then it can be deserialised
        PrepareStoreCopyResponse readPrepareStoreCopyResponse = embeddedChannel.readInbound();
        assertEquals( prepareStoreCopyResponse.getFiles().length, readPrepareStoreCopyResponse.getFiles().length );
        for ( File file : files )
        {
            assertEquals( 1, Stream.of( readPrepareStoreCopyResponse.getFiles() ).map( File::getName ).filter( f -> f.equals( file.getName() ) ).count() );
        }
    }

    @Test
    public void descriptorsGetTransmitted()
    {
        // given
        File[] files =
                new File[]{new File( "File a.txt" ), new File( "file-b" ), new File( "aoifnoasndfosidfoisndfoisnodainfsonidfaosiidfna" ), new File( "" )};
        PrimitiveLongSet indexIds = Primitive.longSet();
        indexIds.add( 13 );

        // when
        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.success( files, indexIds, 1L );
        sendToChannel( prepareStoreCopyResponse, embeddedChannel );

        // then it can be deserialised
        PrepareStoreCopyResponse readPrepareStoreCopyResponse = embeddedChannel.readInbound();
        assertEquals( prepareStoreCopyResponse.getFiles().length, readPrepareStoreCopyResponse.getFiles().length );
        for ( File file : files )
        {
            assertEquals( 1, Stream.of( readPrepareStoreCopyResponse.getFiles() ).map( File::getName ).filter( f -> f.equals( file.getName() ) ).count() );
        }
        assertEquals( prepareStoreCopyResponse.getIndexIds(), readPrepareStoreCopyResponse.getIndexIds() );
    }

    private static void sendToChannel( PrepareStoreCopyResponse prepareStoreCopyResponse, EmbeddedChannel embeddedChannel )
    {
        embeddedChannel.writeOutbound( prepareStoreCopyResponse );

        ByteBuf object = embeddedChannel.readOutbound();
        embeddedChannel.writeInbound( object );
    }
}
