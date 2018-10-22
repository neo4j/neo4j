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
package org.neo4j.bolt.v4;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.bolt.messaging.Neo4jPack;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.v1.messaging.BoltRequestMessageWriter;
import org.neo4j.bolt.v3.messaging.BoltRequestMessageWriterV3;
import org.neo4j.bolt.v4.messaging.PullNMessage;

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
        if ( message instanceof PullNMessage )
        {
            writePullN( (PullNMessage) message );
        }
        else
        {
            super.write( message );
        }
        return this;
    }

    private void writePullN( PullNMessage message )
    {
        try
        {
            packer.packStructHeader( 0, PullNMessage.SIGNATURE );
            packer.pack( message.meta() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
