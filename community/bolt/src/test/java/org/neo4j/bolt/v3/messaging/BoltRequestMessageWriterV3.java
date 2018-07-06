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
package org.neo4j.bolt.v3.messaging;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.bolt.messaging.Neo4jPack;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.v1.messaging.BoltRequestMessageWriter;
import org.neo4j.kernel.impl.util.ValueUtils;

/**
 * This writer simulates the client.
 */
public class BoltRequestMessageWriterV3 extends BoltRequestMessageWriter
{
    public BoltRequestMessageWriterV3( Neo4jPack.Packer packer )
    {
        super( packer );
    }

    @Override
    public BoltRequestMessageWriter write( RequestMessage message ) throws IOException
    {
        if ( message instanceof HelloMessage )
        {
            writeHello( (HelloMessage) message );
        }
        return super.write( message );
    }

    private void writeHello( HelloMessage message )
    {
        try
        {
            packer.packStructHeader( 0, HelloMessage.SIGNATURE );
            packer.pack( ValueUtils.asMapValue( message.meta() ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
