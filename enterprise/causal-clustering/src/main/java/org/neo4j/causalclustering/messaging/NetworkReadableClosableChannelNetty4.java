/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
