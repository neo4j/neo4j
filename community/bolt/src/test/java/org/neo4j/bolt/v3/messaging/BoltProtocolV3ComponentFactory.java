/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.v3.messaging;

import java.io.IOException;

import org.neo4j.bolt.messaging.BoltRequestMessageReader;
import org.neo4j.bolt.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.messaging.Neo4jPack;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.messaging.ResponseMessage;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.runtime.SynchronousBoltConnection;
import org.neo4j.bolt.v1.messaging.BoltRequestMessageWriter;
import org.neo4j.bolt.v1.messaging.RecordingByteChannel;
import org.neo4j.bolt.v1.packstream.BufferedChannelOutput;
import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v2.messaging.Neo4jPackV2;
import org.neo4j.logging.internal.NullLogService;

import static org.mockito.Mockito.mock;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.serialize;

/**
 * A helper factory to generate boltV3 component in tests
 */
public class BoltProtocolV3ComponentFactory
{
    public static Neo4jPack newNeo4jPack()
    {
        return new Neo4jPackV2();
    }

    public static BoltRequestMessageWriter requestMessageWriter( Neo4jPack.Packer packer )
    {
        return new BoltRequestMessageWriterV3( packer );
    }

    public static BoltRequestMessageReader requestMessageReader( BoltStateMachine stateMachine )
    {
        return new BoltRequestMessageReaderV3( new SynchronousBoltConnection( stateMachine ), mock( BoltResponseMessageWriter.class ),
                NullLogService.getInstance() );
    }

    public static byte[] encode( Neo4jPack neo4jPack, RequestMessage... messages ) throws IOException
    {
        RecordingByteChannel rawData = new RecordingByteChannel();
        Neo4jPack.Packer packer = neo4jPack.newPacker( new BufferedChannelOutput( rawData ) );
        BoltRequestMessageWriter writer = requestMessageWriter( packer );

        for ( RequestMessage message : messages )
        {
            writer.write( message );
        }
        writer.flush();

        return rawData.getBytes();
    }

    public static TransportTestUtil.MessageEncoder newMessageEncoder()
    {
        return new TransportTestUtil.MessageEncoder()
        {
            @Override
            public byte[] encode( Neo4jPack neo4jPack, RequestMessage... messages ) throws IOException
            {
                return BoltProtocolV3ComponentFactory.encode( neo4jPack, messages );
            }

            @Override
            public byte[] encode( Neo4jPack neo4jPack, ResponseMessage... messages ) throws IOException
            {
                return serialize( neo4jPack, messages );
            }
        };
    }
}
