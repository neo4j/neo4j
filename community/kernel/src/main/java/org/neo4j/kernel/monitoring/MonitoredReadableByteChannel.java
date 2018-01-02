/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.monitoring;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class MonitoredReadableByteChannel implements ReadableByteChannel
{
    private final ReadableByteChannel delegate;
    private final ByteCounterMonitor monitor;

    public MonitoredReadableByteChannel( ReadableByteChannel delegate, ByteCounterMonitor monitor )
    {
        this.delegate = delegate;
        this.monitor = monitor;
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        int result = delegate.read( dst );
        if ( result >= 0 )
        {
            monitor.bytesRead( result );
        }
        return result;
    }

    @Override
    public boolean isOpen()
    {
        return delegate.isOpen();
    }

    @Override
    public void close() throws IOException
    {
        delegate.close();
    }
}
