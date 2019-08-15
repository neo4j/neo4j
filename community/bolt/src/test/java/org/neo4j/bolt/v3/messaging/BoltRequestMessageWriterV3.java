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
package org.neo4j.bolt.v3.messaging;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.messaging.BoltRequestMessageWriter;
import org.neo4j.bolt.v3.messaging.request.BeginMessage;
import org.neo4j.bolt.v3.messaging.request.CommitMessage;
import org.neo4j.bolt.v3.messaging.request.DiscardAllMessage;
import org.neo4j.bolt.v3.messaging.request.GoodbyeMessage;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.bolt.v3.messaging.request.PullAllMessage;
import org.neo4j.bolt.v3.messaging.request.ResetMessage;
import org.neo4j.bolt.v3.messaging.request.RollbackMessage;
import org.neo4j.bolt.v3.messaging.request.RunMessage;
import org.neo4j.kernel.impl.util.ValueUtils;

/**
 * This writer simulates the client.
 */
public class BoltRequestMessageWriterV3 implements BoltRequestMessageWriter
{
    protected final Neo4jPack.Packer packer;

    public BoltRequestMessageWriterV3( Neo4jPack.Packer packer )
    {
        this.packer = packer;
    }

    public BoltRequestMessageWriter write( RequestMessage message ) throws IOException
    {
        if ( message instanceof ResetMessage )
        {
            writeReset();
        }
        else if ( message instanceof DiscardAllMessage )
        {
            writeDiscardAll();
        }
        else if ( message instanceof PullAllMessage )
        {
            writePullAll();
        }
        else if ( message instanceof GoodbyeMessage )
        {
            writeGoodbye();
        }
        else if ( message instanceof HelloMessage )
        {
            writeHello( (HelloMessage) message );
        }
        else if ( message instanceof BeginMessage )
        {
            writeBegin( (BeginMessage) message );
        }
        else if ( message instanceof CommitMessage )
        {
            writeCommit();
        }
        else if ( message instanceof RollbackMessage )
        {
            writeRollback();
        }
        else if ( message instanceof RunMessage )
        {
            writeRun( (RunMessage) message );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown message: " + message );
        }
        return this;
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

    private void writeGoodbye()
    {
        try
        {
            packer.packStructHeader( 0, GoodbyeMessage.SIGNATURE );
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
            packer.packStructHeader( 0, ResetMessage.SIGNATURE );
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
            packer.packStructHeader( 0, DiscardAllMessage.SIGNATURE );
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
            packer.packStructHeader( 0, PullAllMessage.SIGNATURE );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void writeRun( RunMessage message )
    {
        try
        {
            packer.packStructHeader( 0, RunMessage.SIGNATURE );
            packer.pack( message.statement() );
            packer.pack( message.params() );
            packer.pack( message.meta() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void writeRollback()
    {
        writeSignatureOnlyMessage( RollbackMessage.SIGNATURE );
    }

    private void writeCommit()
    {
        writeSignatureOnlyMessage( CommitMessage.SIGNATURE );
    }

    private void writeBegin( BeginMessage message )
    {
        try
        {
            packer.packStructHeader( 0, BeginMessage.SIGNATURE );
            packer.pack( message.meta() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void writeSignatureOnlyMessage( byte signature )
    {
        try
        {
            packer.packStructHeader( 0, signature );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
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
