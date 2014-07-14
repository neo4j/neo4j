/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import org.neo4j.kernel.impl.transaction.xaframework.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.xaframework.ReadPastEndException;
import org.neo4j.kernel.impl.transaction.xaframework.ReadableLogChannel;

public class NetworkReadableLogChannel implements ReadableLogChannel
{
    private final ChannelBuffer delegate;

    public NetworkReadableLogChannel( ChannelBuffer input )
    {
        this.delegate = input;
    }

    @Override
    public boolean hasMoreData() throws IOException
    {
        return delegate.readableBytes() > 0;
    }

    @Override
    public byte get() throws IOException
    {
        if ( delegate.readableBytes() < 1 )
        {
            throw new ReadPastEndException();
        }
        byte value = delegate.readByte();
        return value;
    }

    @Override
    public short getShort() throws IOException
    {
        if ( delegate.readableBytes() < 2 )
        {
            throw new ReadPastEndException();
        }
        return delegate.readShort();
    }

    @Override
    public int getInt() throws IOException
    {
        if ( delegate.readableBytes() < 4 )
        {
            throw new ReadPastEndException();
        }
        return delegate.readInt();
    }

    @Override
    public long getLong() throws IOException
    {
        if ( delegate.readableBytes() < 8 )
        {
            throw new ReadPastEndException();
        }
        return delegate.readLong();
    }

    @Override
    public float getFloat() throws IOException
    {
        if ( delegate.readableBytes() < 4 )
        {
            throw new ReadPastEndException();
        }
        return delegate.readFloat();
    }

    @Override
    public double getDouble() throws IOException
    {
        if ( delegate.readableBytes() < 8 )
        {
            throw new ReadPastEndException();
        }
        return delegate.readDouble();
    }

    @Override
    public void get( byte[] bytes, int length ) throws IOException
    {
        if ( delegate.readableBytes() < length )
        {
            throw new ReadPastEndException();
        }
        delegate.readBytes( bytes, 0, length );
    }

    @Override
    public void getCurrentPosition( LogPositionMarker positionMarker ) throws IOException
    {
        positionMarker.unspecified();
    }

    @Override
    public void close() throws IOException
    {
        // no op
    }
}
