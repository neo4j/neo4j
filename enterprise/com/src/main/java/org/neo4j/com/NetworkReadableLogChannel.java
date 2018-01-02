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

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.ReadPastEndException;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;

public class NetworkReadableLogChannel implements ReadableLogChannel
{
    private final ChannelBuffer delegate;

    public NetworkReadableLogChannel( ChannelBuffer input )
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
