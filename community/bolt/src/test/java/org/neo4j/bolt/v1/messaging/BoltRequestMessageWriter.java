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
import java.io.UncheckedIOException;

import org.neo4j.bolt.messaging.Neo4jPack;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.v1.messaging.message.AckFailure;
import org.neo4j.bolt.v1.messaging.message.DiscardAll;
import org.neo4j.bolt.v1.messaging.message.Init;
import org.neo4j.bolt.v1.messaging.message.PullAll;
import org.neo4j.bolt.v1.messaging.message.Reset;
import org.neo4j.bolt.v1.messaging.message.Run;
import org.neo4j.kernel.impl.util.ValueUtils;

public class BoltRequestMessageWriter
{
    private final Neo4jPack.Packer packer;

    public BoltRequestMessageWriter( Neo4jPack.Packer packer )
    {
        this.packer = packer;
    }

    public BoltRequestMessageWriter write( RequestMessage message ) throws IOException
    {
        if ( message instanceof Init )
        {
            writeInit( (Init) message );
        }
        else if ( message instanceof AckFailure )
        {
            writeAckFailure();
        }
        else if ( message instanceof Reset )
        {
            writeReset();
        }
        else if ( message instanceof Run )
        {
            writeRun( (Run) message );
        }
        else if ( message instanceof DiscardAll )
        {
            writeDiscardAll();
        }
        else if ( message instanceof PullAll )
        {
            writePullAll();
        }
        else
        {
            throw new IllegalArgumentException( "Unknown message: " + message );
        }
        return this;
    }

    private void writeInit( Init message )
    {
        try
        {
            packer.packStructHeader( 2, Init.SIGNATURE );
            packer.pack( message.userAgent() );
            packer.pack( ValueUtils.asMapValue( message.authToken() ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void writeAckFailure()
    {
        try
        {
            packer.packStructHeader( 0, AckFailure.SIGNATURE );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void writeReset()
    {
        try
        {
            packer.packStructHeader( 0, Reset.SIGNATURE );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void writeRun( Run message )
    {
        try
        {
            packer.packStructHeader( 2, Run.SIGNATURE );
            packer.pack( message.statement() );
            packer.pack( message.params() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void writeDiscardAll()
    {
        try
        {
            packer.packStructHeader( 0, DiscardAll.SIGNATURE );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void writePullAll()
    {
        try
        {
            packer.packStructHeader( 0, PullAll.SIGNATURE );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    public void flush()
    {
        try
        {
            packer.flush();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
