/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.messaging;

import java.io.IOException;

import io.netty.buffer.ByteBuf;

import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.storageengine.api.ReadPastEndException;

public class NetworkReadableClosableChannelNetty4 implements ReadableClosablePositionAwareChannel
{
    private final ByteBuf delegate;

    public NetworkReadableClosableChannelNetty4( ByteBuf input )
    {
        this.delegate = input;
    }

    @Override
    public byte get() throws IOException
    {
        ensureBytes( Byte.BYTES );
        return delegate.readByte();
    }

    @Override
    public short getShort() throws IOException
    {
        ensureBytes( Short.BYTES );
        return delegate.readShort();
    }

    @Override
    public int getInt() throws IOException
    {
        ensureBytes( Integer.BYTES );
        return delegate.readInt();
    }

    @Override
    public long getLong() throws IOException
    {
        ensureBytes( Long.BYTES );
        return delegate.readLong();
    }

    @Override
    public float getFloat() throws IOException
    {
        ensureBytes( Float.BYTES );
        return delegate.readFloat();
    }

    @Override
    public double getDouble() throws IOException
    {
        ensureBytes( Double.BYTES );
        return delegate.readDouble();
    }

    @Override
    public void get( byte[] bytes, int length ) throws IOException
    {
        ensureBytes( length );
        delegate.readBytes( bytes, 0, length );
    }

    private void ensureBytes( int byteCount ) throws ReadPastEndException
    {
        if ( delegate.readableBytes() < byteCount )
        {
            throw ReadPastEndException.INSTANCE;
        }
    }

    @Override
    public LogPositionMarker getCurrentPosition( LogPositionMarker positionMarker ) throws IOException
    {
        positionMarker.unspecified();
        return positionMarker;
    }

    @Override
    public void close() throws IOException
    {
        // no op
    }
}
