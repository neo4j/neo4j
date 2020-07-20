/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;

import org.neo4j.io.fs.ReadAheadChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.io.memory.ScopedBuffer;
import org.neo4j.memory.MemoryTracker;

/**
 * Basically a sequence of {@link StoreChannel channels} seamlessly seen as one.
 */
public class ReadAheadLogChannel extends ReadAheadChannel<LogVersionedStoreChannel> implements ReadableLogChannel
{
    private final LogVersionBridge bridge;

    public ReadAheadLogChannel( LogVersionedStoreChannel startingChannel, MemoryTracker memoryTracker )
    {
        this( startingChannel, LogVersionBridge.NO_MORE_CHANNELS, new NativeScopedBuffer( DEFAULT_READ_AHEAD_SIZE, memoryTracker ) );
    }

    public ReadAheadLogChannel( LogVersionedStoreChannel startingChannel, LogVersionBridge bridge, MemoryTracker memoryTracker )
    {
        this( startingChannel, bridge, new NativeScopedBuffer( DEFAULT_READ_AHEAD_SIZE, memoryTracker ) );
    }

    /**
     * This constructor is private to ensure that the given buffer always comes form one of our own constructors.
     */
    private ReadAheadLogChannel( LogVersionedStoreChannel startingChannel, LogVersionBridge bridge, ScopedBuffer scopedBuffer )
    {
        super( startingChannel, scopedBuffer );
        this.bridge = bridge;
    }

    @Override
    public long getVersion()
    {
        return channel.getVersion();
    }

    @Override
    public byte getLogFormatVersion()
    {
        return channel.getLogFormatVersion();
    }

    @Override
    public LogPositionMarker getCurrentPosition( LogPositionMarker positionMarker ) throws IOException
    {
        positionMarker.mark( channel.getVersion(), position() );
        return positionMarker;
    }

    @Override
    public LogPosition getCurrentPosition() throws IOException
    {
        return new LogPosition( channel.getVersion(),position() );
    }

    @Override
    protected LogVersionedStoreChannel next( LogVersionedStoreChannel channel ) throws IOException
    {
        return bridge.next( channel );
    }

    @Override
    public void close() throws IOException
    {
        super.close();
    }
}
