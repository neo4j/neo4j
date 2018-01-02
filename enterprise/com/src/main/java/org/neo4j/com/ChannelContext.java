/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.com;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

public class ChannelContext
{
    private final Channel channel;
    private final ChannelBuffer output;
    private final ByteBuffer input;

    public ChannelContext( Channel channel, ChannelBuffer output, ByteBuffer input )
    {
        this.channel = requireNonNull( channel );
        this.output = requireNonNull( output );
        this.input = requireNonNull( input );
    }

    public Channel channel()
    {
        return channel;
    }

    public ChannelBuffer output()
    {
        return output;
    }

    public ByteBuffer input()
    {
        return input;
    }

    @Override
    public String toString()
    {
        return "ChannelContext{channel=" + channel + ", output=" + output + ", input=" + input + "}";
    }
}
