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
package org.neo4j.bolt.runtime;

import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;

import java.net.SocketAddress;

import org.neo4j.bolt.transport.TransportThrottleGroup;
import org.neo4j.bolt.v1.packstream.PackOutput;
import org.neo4j.bolt.v1.runtime.BoltConnectionFatality;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.bolt.v1.runtime.Job;
import org.neo4j.bolt.v1.transport.ChunkedOutput;

public class SynchronousBoltConnection implements BoltConnection
{
    private final EmbeddedChannel channel;
    private final PackOutput output;
    private final BoltStateMachine machine;

    public SynchronousBoltConnection( BoltStateMachine machine )
    {
        this.channel = new EmbeddedChannel();
        this.output = new ChunkedOutput( this.channel, TransportThrottleGroup.NO_THROTTLE );
        this.machine = machine;
    }

    @Override
    public String id()
    {
        return channel.id().asLongText();
    }

    @Override
    public SocketAddress localAddress()
    {
        return channel.localAddress();
    }

    @Override
    public SocketAddress remoteAddress()
    {
        return channel.remoteAddress();
    }

    @Override
    public Channel channel()
    {
        return channel;
    }

    @Override
    public PackOutput output()
    {
        return output;
    }

    @Override
    public boolean hasPendingJobs()
    {
        return false;
    }

    @Override
    public void start()
    {

    }

    @Override
    public void enqueue( Job job )
    {
        try
        {
            job.perform( machine );
        }
        catch ( BoltConnectionFatality connectionFatality )
        {
            throw new RuntimeException( connectionFatality );
        }
    }

    @Override
    public boolean processNextBatch()
    {
        return true;
    }

    @Override
    public void handleSchedulingError( Throwable t )
    {

    }

    @Override
    public void interrupt()
    {
        machine.interrupt();
    }

    @Override
    public void stop()
    {
        channel.finishAndReleaseAll();
        machine.close();
    }
}
