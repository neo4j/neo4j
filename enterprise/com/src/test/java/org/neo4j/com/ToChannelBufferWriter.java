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

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import org.jboss.netty.buffer.ChannelBuffer;

import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;

public class ToChannelBufferWriter implements MadeUpWriter
{
    private final ChannelBuffer target;

    public ToChannelBufferWriter( ChannelBuffer target )
    {
        this.target = target;
    }

    @Override
    public void write( ReadableByteChannel data )
    {
        try ( BlockLogBuffer blockBuffer = new BlockLogBuffer( target, new Monitors().newMonitor( ByteCounterMonitor.class ) ) )
        {
            blockBuffer.write( data );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
