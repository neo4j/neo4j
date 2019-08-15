/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt.v4;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.messaging.BoltRequestMessageWriter;
import org.neo4j.bolt.v3.messaging.BoltRequestMessageWriterV3;
import org.neo4j.bolt.v4.messaging.AbstractStreamingMessage;
import org.neo4j.bolt.v4.messaging.DiscardMessage;
import org.neo4j.bolt.v4.messaging.PullMessage;

/**
 * This writer simulates the client.
 */
public class BoltRequestMessageWriterV4 extends BoltRequestMessageWriterV3
{
    public BoltRequestMessageWriterV4( Neo4jPack.Packer packer )
    {
        super( packer );
    }

    @Override
    public BoltRequestMessageWriter write( RequestMessage message ) throws IOException
    {
        if ( message instanceof PullMessage )
        {
            writeHandleN( (PullMessage) message, PullMessage.SIGNATURE );
        }
        else if ( message instanceof DiscardMessage )
        {
            writeHandleN( (DiscardMessage) message, DiscardMessage.SIGNATURE );
        }
        else
        {
            super.write( message );
        }
        return this;
    }

    private void writeHandleN( AbstractStreamingMessage message, byte signature )
    {
        try
        {
            packer.packStructHeader( 0, signature );
            packer.pack( message.meta() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
