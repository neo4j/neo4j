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

import java.util.Arrays;
import java.util.List;

import org.neo4j.bolt.messaging.BoltRequestMessageReader;
import org.neo4j.bolt.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.messaging.RequestMessageDecoder;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.v1.messaging.MessageProcessingHandler;
import org.neo4j.bolt.v1.messaging.ResultHandler;
import org.neo4j.bolt.v1.messaging.decoder.DiscardAllMessageDecoder;
import org.neo4j.bolt.v1.messaging.decoder.PullAllMessageDecoder;
import org.neo4j.bolt.v1.messaging.decoder.ResetMessageDecoder;
import org.neo4j.bolt.v3.messaging.decoder.BeginMessageDecoder;
import org.neo4j.bolt.v3.messaging.decoder.CommitMessageDecoder;
import org.neo4j.bolt.v3.messaging.decoder.GoodbyeMessageDecoder;
import org.neo4j.bolt.v3.messaging.decoder.HelloMessageDecoder;
import org.neo4j.bolt.v3.messaging.decoder.RollbackMessageDecoder;
import org.neo4j.bolt.v3.messaging.decoder.RunMessageDecoder;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

public class BoltRequestMessageReaderV3 extends BoltRequestMessageReader
{
    public BoltRequestMessageReaderV3( BoltConnection connection, BoltResponseMessageWriter responseMessageWriter,
            LogService logService )
    {
        super( connection, newSimpleResponseHandler( responseMessageWriter, connection, logService ),
                buildDecoders( connection, responseMessageWriter, logService ) );
    }

    private static List<RequestMessageDecoder> buildDecoders( BoltConnection connection, BoltResponseMessageWriter responseMessageWriter,
            LogService logService )
    {
        BoltResponseHandler resultHandler = new ResultHandler( responseMessageWriter, connection, internalLog( logService ) );
        BoltResponseHandler defaultHandler = newSimpleResponseHandler( responseMessageWriter, connection, logService );

        return Arrays.asList(
                new HelloMessageDecoder( defaultHandler ),
                new RunMessageDecoder( defaultHandler ),
                new DiscardAllMessageDecoder( resultHandler ),
                new PullAllMessageDecoder( resultHandler ),
                new BeginMessageDecoder( defaultHandler ),
                new CommitMessageDecoder( resultHandler ),
                new RollbackMessageDecoder( resultHandler ),
                new ResetMessageDecoder( connection, defaultHandler ),
                new GoodbyeMessageDecoder( connection, defaultHandler )
        );
    }

    private static BoltResponseHandler newSimpleResponseHandler( BoltResponseMessageWriter responseMessageWriter, BoltConnection connection,
            LogService logService )
    {
        return new MessageProcessingHandler( responseMessageWriter, connection, internalLog( logService ) );
    }

    private static Log internalLog( LogService logService )
    {
        return logService.getInternalLog( BoltRequestMessageReaderV3.class );
    }
}
