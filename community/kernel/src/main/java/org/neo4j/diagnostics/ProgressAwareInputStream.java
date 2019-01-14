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
package org.neo4j.diagnostics;

import java.io.IOException;
import java.io.InputStream;

/**
 * Implements an {@link InputStream} that keeps track of the progress. This assumes that the total size is available
 * before reading starts.
 */
public class ProgressAwareInputStream extends InputStream
{
    private final OnProgressListener listener;
    private final InputStream wrappedInputStream;
    private final long size;
    private long totalRead;
    private int lastReportedPercent;

    public ProgressAwareInputStream( InputStream wrappedInputStream, long size, OnProgressListener listener )
    {
        this.wrappedInputStream = wrappedInputStream;
        this.size = size;
        this.listener = listener;
    }

    @Override
    public int read() throws IOException
    {
        int data = wrappedInputStream.read();
        if ( data >= 0 )
        {
            totalRead += 1;
            recalculatePercent();
        }
        return data;
    }

    @Override
    public int read( byte[] b ) throws IOException
    {
        int n = wrappedInputStream.read( b );
        if ( n > 0 )
        {
            totalRead += n;
            recalculatePercent();
        }
        return n;
    }

    @Override
    public int read( byte[] b, int offset, int length ) throws IOException
    {
        int n = wrappedInputStream.read( b, offset, length );
        if ( n > 0 )
        {
            totalRead += n;
            recalculatePercent();
        }
        return n;
    }

    private void recalculatePercent()
    {
        int percent = (int) (totalRead * 100 / size);
        if ( percent > 100 )
        {
            percent = 100;
        }
        if ( percent < 0 )
        {
            percent = 0;
        }
        if ( percent > lastReportedPercent )
        {
            lastReportedPercent = percent;
            listener.onProgress( percent );
        }
    }

    @Override
    public long skip( long n ) throws IOException
    {
        return wrappedInputStream.skip( n );
    }

    @Override
    public int available() throws IOException
    {
        return wrappedInputStream.available();
    }

    @Override
    public void close() throws IOException
    {
        wrappedInputStream.close();
    }

    @Override
    public void mark( int readLimit )
    {
        wrappedInputStream.mark( readLimit );
    }

    @Override
    public void reset() throws IOException
    {
        wrappedInputStream.reset();
    }

    @Override
    public boolean markSupported()
    {
        return wrappedInputStream.markSupported();
    }

    /**
     * Interface for classes that want to monitor this input stream
     */
    public interface OnProgressListener
    {
        void onProgress( int percentage );
    }
}
