/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.messaging;

import org.neo4j.bolt.v1.messaging.message.RequestMessage;

import java.io.IOException;
import java.util.Map;

import static org.neo4j.bolt.v1.messaging.BoltRequestMessage.*;

public class BoltRequestMessageWriter implements BoltRequestMessageHandler<IOException>
{

    private final Neo4jPack.Packer packer;
    private final BoltResponseMessageBoundaryHook onMessageComplete;

    public BoltRequestMessageWriter( Neo4jPack.Packer packer, BoltResponseMessageBoundaryHook onMessageComplete )
    {
        this.packer = packer;
        this.onMessageComplete = onMessageComplete;
    }

    public BoltRequestMessageWriter write( RequestMessage message ) throws IOException
    {
        message.dispatch( this );
        return this;
    }

    @Override
    public void onInit( String clientName, Map<String, Object> credentials ) throws IOException
    {
        packer.packStructHeader( 1, INIT.signature() );
        packer.pack( clientName );
        packer.packRawMap( credentials );
        onMessageComplete.onMessageComplete();
    }

    @Override
    public void onAckFailure() throws IOException
    {
        packer.packStructHeader( 0, ACK_FAILURE.signature() );
        onMessageComplete.onMessageComplete();
    }

    @Override
    public void onReset() throws IOException
    {
        packer.packStructHeader( 0, RESET.signature() );
        onMessageComplete.onMessageComplete();
    }

    @Override
    public void onRun( String statement, Map<String, Object> params )
            throws IOException
    {
        packer.packStructHeader( 2, RUN.signature() );
        packer.pack( statement );
        packer.packRawMap( params );
        onMessageComplete.onMessageComplete();
    }

    @Override
    public void onDiscardAll()
            throws IOException
    {
        packer.packStructHeader( 0, DISCARD_ALL.signature() );
        onMessageComplete.onMessageComplete();
    }

    @Override
    public void onPullAll()
            throws IOException
    {
        packer.packStructHeader( 0, PULL_ALL.signature() );
        onMessageComplete.onMessageComplete();
    }

    public void flush() throws IOException
    {
        packer.flush();
    }

}
