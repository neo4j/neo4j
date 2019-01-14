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

import java.io.Flushable;
import java.io.IOException;

import io.netty.buffer.ByteBuf;

import org.neo4j.kernel.impl.transaction.log.FlushableChannel;

public class NetworkFlushableByteBuf implements FlushableChannel
{
    private final ByteBuf delegate;

    public NetworkFlushableByteBuf( ByteBuf byteBuf )
    {
        this.delegate = byteBuf;
    }

    @Override
    public FlushableChannel put( byte value )
    {
        delegate.writeByte( value );
        return this;
    }

    @Override
    public FlushableChannel putShort( short value )
    {
        delegate.writeShort( value );
        return this;
    }

    @Override
    public FlushableChannel putInt( int value )
    {
        delegate.writeInt( value );
        return this;
    }

    @Override
    public FlushableChannel putLong( long value )
    {
        delegate.writeLong( value );
        return this;
    }

    @Override
    public FlushableChannel putFloat( float value )
    {
        delegate.writeFloat( value );
        return this;
    }

    @Override
    public FlushableChannel putDouble( double value )
    {
        delegate.writeDouble( value );
        return this;
    }

    @Override
    public FlushableChannel put( byte[] value, int length )
    {
        delegate.writeBytes( value, 0, length );
        return this;
    }

    @Override
    public void close()
    {
    }

    @Override
    public Flushable prepareForFlush()
    {
        return null;
    }

    public ByteBuf buffer()
    {
        return delegate;
    }
}
