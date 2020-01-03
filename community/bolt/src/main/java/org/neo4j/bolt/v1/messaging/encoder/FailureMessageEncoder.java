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
package org.neo4j.bolt.v1.messaging.encoder;

import java.io.IOException;

import org.neo4j.bolt.messaging.Neo4jPack;
import org.neo4j.bolt.messaging.ResponseMessageEncoder;
import org.neo4j.bolt.v1.messaging.response.FailureMessage;
import org.neo4j.bolt.v1.messaging.response.FatalFailureMessage;
import org.neo4j.logging.Log;

public class FailureMessageEncoder implements ResponseMessageEncoder<FailureMessage>
{
    private final Log log;

    public FailureMessageEncoder( Log log )
    {
        this.log = log;
    }

    @Override
    public void encode( Neo4jPack.Packer packer, FailureMessage message ) throws IOException
    {
        if ( message instanceof FatalFailureMessage )
        {
            log.debug( "Encoding a fatal failure message to send. Message: %s", message );
        }
        encodeFailure( message, packer );
    }

    private void encodeFailure( FailureMessage message, Neo4jPack.Packer packer ) throws IOException
    {
        packer.packStructHeader( 1, message.signature() );
        packer.packMapHeader( 2 );

        packer.pack( "code" );
        packer.pack( message.status().code().serialize() );

        packer.pack( "message" );
        packer.pack( message.message() );
    }
}
