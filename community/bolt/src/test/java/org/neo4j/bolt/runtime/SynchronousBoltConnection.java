/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.bolt.runtime;

import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;

import java.net.SocketAddress;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.v1.runtime.BoltConnectionFatality;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.bolt.v1.runtime.Job;

public class SynchronousBoltConnection implements BoltConnection
{
    private final Channel channel;
    private final BoltStateMachine machine;

    public SynchronousBoltConnection( BoltStateMachine machine )
    {
        this.channel = new EmbeddedChannel();
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
    public String principal()
    {
        return machine.owner();
    }

    @Override
    public boolean isOutOfBand()
    {
        return false;
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
    public void processNextBatch()
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
        machine.close();
    }
}
