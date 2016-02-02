/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.io.IOException;

import org.neo4j.storageengine.api.ReadPastEndException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class ChannelBufferToChannel implements ReadableChannel, WritableChannel
{
    private final ChannelBuffer blockLogReader;

    public ChannelBufferToChannel( ChannelBuffer blockLogReader )
    {
        this.blockLogReader = blockLogReader;
    }

    @Override
    public byte get() throws IOException, ReadPastEndException
    {
        return blockLogReader.readByte();
    }

    @Override
    public short getShort() throws IOException, ReadPastEndException
    {
        return blockLogReader.readShort();
    }

    @Override
    public int getInt() throws IOException, ReadPastEndException
    {
        return blockLogReader.readInt();
    }

    @Override
    public long getLong() throws IOException, ReadPastEndException
    {
        return blockLogReader.readLong();
    }

    @Override
    public float getFloat() throws IOException, ReadPastEndException
    {
        return blockLogReader.readFloat();
    }

    @Override
    public double getDouble() throws IOException, ReadPastEndException
    {
        return blockLogReader.readDouble();
    }

    @Override
    public void get( byte[] bytes, int length ) throws IOException, ReadPastEndException
    {
        blockLogReader.getBytes( 0, bytes, 0, length );
    }

    @Override
    public WritableChannel put( byte value ) throws IOException
    {
        blockLogReader.writeByte( value );
        return this;
    }

    @Override
    public WritableChannel putShort( short value ) throws IOException
    {
        blockLogReader.writeShort( value );
        return this;
    }

    @Override
    public WritableChannel putInt( int value ) throws IOException
    {
        blockLogReader.writeInt( value );
        return this;
    }

    @Override
    public WritableChannel putLong( long value ) throws IOException
    {
        blockLogReader.writeLong( value );
        return this;
    }

    @Override
    public WritableChannel putFloat( float value ) throws IOException
    {
        blockLogReader.writeFloat( value );
        return this;
    }

    @Override
    public WritableChannel putDouble( double value ) throws IOException
    {
        blockLogReader.writeDouble( value );
        return this;
    }

    @Override
    public WritableChannel put( byte[] value, int length ) throws IOException
    {
        blockLogReader.writeBytes( value, 0, length );
        return this;
    }
}
