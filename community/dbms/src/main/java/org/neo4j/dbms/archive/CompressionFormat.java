/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.dbms.archive;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.neo4j.function.ThrowingSupplier;
import org.neo4j.io.IOUtils;

public enum CompressionFormat
{
    GZIP
            {
                @Override
                public OutputStream compress( OutputStream stream ) throws IOException
                {
                    return new GZIPOutputStream( stream );
                }

                @Override
                public InputStream decompress( InputStream stream ) throws IOException
                {
                    return new GZIPInputStream( stream );
                }
            },
    ZSTD
            {
                // ZSTD does not check a magic header on initialisation, like GZIP does, so we have to do that ourselves.
                // We use this header for that purpose.
                private final byte[] HEADER = new byte[] {'z', 's', 't', 'd'};

                @Override
                public OutputStream compress( OutputStream stream ) throws IOException
                {
                    ZstdOutputStream zstdout = new ZstdOutputStream( stream );
                    zstdout.setChecksum( true );
                    if ( Runtime.getRuntime().availableProcessors() > 2 )
                    {
                        zstdout.setWorkers( Runtime.getRuntime().availableProcessors() );
                    }
                    zstdout.write( HEADER );
                    return zstdout;
                }

                @Override
                public InputStream decompress( InputStream stream ) throws IOException
                {
                    ZstdInputStream zstdin = new ZstdInputStream( stream );
                    byte[] header = new byte[HEADER.length];
                    if ( zstdin.read( header ) != HEADER.length || !Arrays.equals( header, HEADER ) )
                    {
                        throw new IOException( "Not in ZSTD format" );
                    }
                    return zstdin;
                }
            };

    public abstract OutputStream compress( OutputStream stream ) throws IOException;
    public abstract InputStream decompress( InputStream stream ) throws IOException;

    /**
     * @return {@code true} if the given {@link InputStream} is <em>directly</em> a compressed input stream of this format. With "directly" meaning that the
     * compressed stream is not wrapped in other streams, like buffered or filtering input streams.
     */
    public boolean isFormat( InputStream stream )
    {
        return (this == ZSTD && stream instanceof ZstdInputStream) || (this == GZIP && stream instanceof GZIPInputStream);
    }

    public static OutputStream compress( ThrowingSupplier<OutputStream, IOException> streamSupplier ) throws IOException
    {
        return compress( streamSupplier, ZSTD );
    }

    public static OutputStream compress( ThrowingSupplier<OutputStream, IOException> streamSupplier, CompressionFormat format ) throws IOException
    {
        OutputStream sink = streamSupplier.get();
        try
        {
            return new BufferedOutputStream( format.compress( sink ) );
        }
        catch ( IOException ioe )
        {
            IOUtils.closeAllSilently( sink );
            throw ioe;
        }
    }

    public static InputStream decompress( ThrowingSupplier<InputStream, IOException> streamSupplier ) throws IOException
    {
        InputStream source = streamSupplier.get();
        try
        {
            return ZSTD.decompress( source );
        }
        catch ( IOException ioe )
        {
            IOUtils.closeAllSilently( source );
            source = streamSupplier.get();
            try
            {
                return GZIP.decompress( source );
            }
            catch ( IOException e )
            {
                IOUtils.closeAllSilently( source );
                ioe.addSuppressed( e );
                throw ioe;
            }
        }
    }
}
