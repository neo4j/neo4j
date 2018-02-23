/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.bolt.v1.transport;

import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.bolt.v1.messaging.BoltRequestMessageRecorder;
import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v1.messaging.message.RunMessage;
import org.neo4j.bolt.v1.messaging.util.MessageMatchers;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.bolt.v1.messaging.message.RunMessage.run;

public class BoltV1DechunkerTest
{
    @Test
    public void shouldReadMessageWhenTheHeaderIsSplitAcrossChunks() throws Exception
    {
        Neo4jPack neo4jPack = new Neo4jPackV1();
        Random random = ThreadLocalRandom.current();
        for ( int len = 1; len <= 0x8000; len = len << 1 )
        {
            // given
            StringBuilder content = new StringBuilder( len );
            for ( int i = 0; i < len; i++ )
            {
                content.appendCodePoint( 'a' + random.nextInt( 'z' - 'a' ) );
            }
            RunMessage run = run( content.toString() );
            byte[] message = MessageMatchers.serialize( neo4jPack, run );
            byte head1 = (byte) (message.length >> 8);
            byte head2 = (byte) (message.length & 0xFF);
            byte[] chunk2 = new byte[message.length + 3];
            chunk2[0] = head2;
            System.arraycopy( message, 0, chunk2, 1, message.length );

            BoltRequestMessageRecorder messages = new BoltRequestMessageRecorder();
            BoltV1Dechunker dechunker = new BoltV1Dechunker( neo4jPack, messages, () ->
            {
            } );

            // when
            dechunker.handle( wrappedBuffer( new byte[]{head1} ) );
            assertTrue( messages.asList().isEmpty(), "content length " + len + ": should be waiting for second chunk" );
            dechunker.handle( wrappedBuffer( chunk2 ) );

            // then
            assertEquals( 1, messages.asList().size(), "content length " + len + ": should have received message" );
            assertEquals( run, messages.asList().get( 0 ) );
        }
    }
}
