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

import org.jboss.netty.buffer.ChannelBuffer;

import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.storageengine.api.ReadPastEndException;

public class NetworkReadableClosableChannel implements ReadableClosablePositionAwareChannel
{
    private final ChannelBuffer delegate;

    public NetworkReadableClosableChannel( ChannelBuffer input )
    {
        this.delegate = input;
    }

    @Override
    public byte get() throws IOException
    {
        try
        {
            return delegate.readByte();
        }
        catch ( IndexOutOfBoundsException e )
        {
            throw ReadPastEndException.INSTANCE;
        }
    }

    @Override
    public short getShort() throws IOException
    {
        try
        {
            return delegate.readShort();
        }
        catch ( IndexOutOfBoundsException e )
        {
            throw ReadPastEndException.INSTANCE;
        }
    }

    @Override
    public int getInt() throws IOException
    {
        try
        {
            return delegate.readInt();
        }
        catch ( IndexOutOfBoundsException e )
        {
            throw ReadPastEndException.INSTANCE;
        }
    }

    @Override
    public long getLong() throws IOException
    {
        try
        {
            return delegate.readLong();
        }
        catch ( IndexOutOfBoundsException e )
        {
            throw ReadPastEndException.INSTANCE;
        }
    }

    @Override
    public float getFloat() throws IOException
    {
        try
        {
            return delegate.readFloat();
        }
        catch ( IndexOutOfBoundsException e )
        {
            throw ReadPastEndException.INSTANCE;
        }
    }

    @Override
    public double getDouble() throws IOException
    {
        try
        {
            return delegate.readDouble();
        }
        catch ( IndexOutOfBoundsException e )
        {
            throw ReadPastEndException.INSTANCE;
        }
    }

    @Override
    public void get( byte[] bytes, int length ) throws IOException
    {
        try
        {
            delegate.readBytes( bytes, 0, length );
        }
        catch ( IndexOutOfBoundsException e )
        {
            throw ReadPastEndException.INSTANCE;
        }
    }

    @Override
    public LogPositionMarker getCurrentPosition( LogPositionMarker positionMarker )
    {
        positionMarker.unspecified();
        return positionMarker;
    }

    @Override
    public void close()
    {
        // no op
    }
}
