/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.bolt.v1.messaging;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.neo4j.bolt.logging.BoltMessageLogger;
import org.neo4j.bolt.messaging.BoltRequestMessageReader;
import org.neo4j.bolt.messaging.RequestMessageDecoder;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.v1.messaging.decoder.AckFailureDecoder;
import org.neo4j.bolt.v1.messaging.decoder.DiscardAllDecoder;
import org.neo4j.bolt.v1.messaging.decoder.InitDecoder;
import org.neo4j.bolt.v1.messaging.decoder.PullAllDecoder;
import org.neo4j.bolt.v1.messaging.decoder.ResetDecoder;
import org.neo4j.bolt.v1.messaging.decoder.RunDecoder;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;

public class BoltRequestMessageReaderV1 extends BoltRequestMessageReader
{
    public BoltRequestMessageReaderV1( BoltConnection connection, BoltResponseMessageHandler<IOException> responseMessageHandler,
            BoltMessageLogger messageLogger, LogService logService )
    {
        super( connection,
                newSimpleResponseHandler( connection, responseMessageHandler, logService ),
                buildUnpackers( connection, responseMessageHandler, messageLogger, logService ),
                messageLogger );
    }

    private static List<RequestMessageDecoder> buildUnpackers( BoltConnection connection, BoltResponseMessageHandler<IOException> responseMessageHandler,
            BoltMessageLogger messageLogger, LogService logService )
    {
        BoltResponseHandler initHandler = newSimpleResponseHandler( connection, responseMessageHandler, logService );
        BoltResponseHandler runHandler = newSimpleResponseHandler( connection, responseMessageHandler, logService );
        BoltResponseHandler resultHandler = new ResultHandler( responseMessageHandler, connection, internalLog( logService ) );
        BoltResponseHandler defaultHandler = newSimpleResponseHandler( connection, responseMessageHandler, logService );

        return Arrays.asList(
                new InitDecoder( initHandler, messageLogger ),
                new AckFailureDecoder( defaultHandler, messageLogger ),
                new ResetDecoder( connection, defaultHandler, messageLogger ),
                new RunDecoder( runHandler, messageLogger ),
                new DiscardAllDecoder( resultHandler, messageLogger ),
                new PullAllDecoder( resultHandler, messageLogger )
        );
    }

    private static BoltResponseHandler newSimpleResponseHandler( BoltConnection connection,
            BoltResponseMessageHandler<IOException> responseMessageHandler, LogService logService )
    {
        return new MessageProcessingHandler( responseMessageHandler, connection, internalLog( logService ) );
    }

    private static Log internalLog( LogService logService )
    {
        return logService.getInternalLog( BoltRequestMessageReaderV1.class );
    }
}
