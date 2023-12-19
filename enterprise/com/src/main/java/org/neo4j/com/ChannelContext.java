/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
