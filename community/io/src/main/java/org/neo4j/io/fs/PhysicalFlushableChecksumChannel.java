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
package org.neo4j.io.fs;

import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.Checksum;

import org.neo4j.io.memory.ScopedBuffer;
import org.neo4j.util.FeatureToggles;

/**
 * The main implementation of {@link FlushableChannel}. This class provides buffering over a simple {@link StoreChannel}
 * and, as a side effect, allows control of the flushing of that buffer to disk.
 */
public class PhysicalFlushableChecksumChannel extends PhysicalFlushableChannel implements FlushableChecksumChannel
{
    static boolean DISABLE_WAL_CHECKSUM = FeatureToggles.flag( ChecksumWriter.class, "disableChecksum", false );

    private final ByteBuffer checksumView;
    private final Checksum checksum;

    public PhysicalFlushableChecksumChannel( StoreChannel channel, ScopedBuffer scopedBuffer )
    {
        super( channel, scopedBuffer );
        this.checksumView = scopedBuffer.getBuffer().duplicate();
        checksum = CHECKSUM_FACTORY.get();
    }

    /**
     * External synchronization between this method and close is required so that they aren't called concurrently.
     * Currently that's done by acquiring the PhysicalLogFile monitor.
     */
    @Override
    public Flushable prepareForFlush() throws IOException
    {
        if ( !DISABLE_WAL_CHECKSUM )
        {
            // Consume remaining bytes
            checksumView.limit( buffer.position() );
            checksum.update( checksumView );
            checksumView.clear();
        }

        return super.prepareForFlush();
    }

    @Override
    public int putChecksum() throws IOException
    {
        // Make sure we can append checksum
        bufferWithGuaranteedSpace( 4 );

        if ( DISABLE_WAL_CHECKSUM )
        {
            buffer.putInt( 0xDEAD5EED );
            return 0xDEAD5EED;
        }

        // Consume remaining bytes
        checksumView.limit( buffer.position() );
        checksum.update( checksumView );
        int checksum = (int) this.checksum.getValue();

        // Append
        buffer.putInt( checksum );

        return checksum;
    }

    @Override
    public void beginChecksum()
    {
        if ( DISABLE_WAL_CHECKSUM )
        {
            return;
        }
        checksum.reset();
        checksumView.limit( checksumView.capacity() );
        checksumView.position( buffer.position() );
    }

    @Override
    public FlushableChecksumChannel put( byte value ) throws IOException
    {
        return (FlushableChecksumChannel) super.put( value );
    }

    @Override
    public FlushableChecksumChannel putShort( short value ) throws IOException
    {
        return (FlushableChecksumChannel) super.putShort( value );
    }

    @Override
    public FlushableChecksumChannel putInt( int value ) throws IOException
    {
        return (FlushableChecksumChannel) super.putInt( value );
    }

    @Override
    public FlushableChecksumChannel putLong( long value ) throws IOException
    {
        return (FlushableChecksumChannel) super.putLong( value );
    }

    @Override
    public FlushableChecksumChannel putFloat( float value ) throws IOException
    {
        return (FlushableChecksumChannel) super.putFloat( value );
    }

    @Override
    public FlushableChecksumChannel putDouble( double value ) throws IOException
    {
        return (FlushableChecksumChannel) super.putDouble( value );
    }

    @Override
    public FlushableChecksumChannel put( byte[] value, int length ) throws IOException
    {
        return (FlushableChecksumChannel) super.put( value, length );
    }
}
