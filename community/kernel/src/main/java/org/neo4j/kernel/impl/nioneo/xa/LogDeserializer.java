/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.kernel.impl.nioneo.xa.command.LogHandler;
import org.neo4j.kernel.impl.nioneo.xa.command.LogReader;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;

public class LogDeserializer implements LogReader<ReadableByteChannel>
{
    private final ByteCounterMonitor monitor;
    private LogHandler handler;
    private final ByteBuffer scratch;

    private XaCommandReader xaCommandReader;
    private int xidIdentifier;


    public LogDeserializer( ByteCounterMonitor monitor, ByteBuffer scratch )
    {
        this.monitor = monitor;
        this.scratch = scratch;
    }

    @Override
    public void setXaCommandReader( XaCommandReader xaCommandReader )
    {
        this.xaCommandReader = xaCommandReader;
    }

    @Override
    public void setXidIdentifier( int xidIdentifier )
    {
        this.xidIdentifier = xidIdentifier;
    }

    @Override
    public void setLogHandler( LogHandler handler )
    {
        this.handler = handler;
    }

    public void read( ReadableByteChannel channel ) throws IOException
    {
        try
        {
            long startedAtPosition = scratch.position();
            LogEntry entry = LogIoUtils.readEntry( scratch, channel, xaCommandReader );
            monitor.bytesRead( scratch.position() - startedAtPosition );

            if ( entry == null || !(entry instanceof LogEntry.Start ) )
            {
                throw new IOException( "Unable to find start entry" );
            }

            handler.startLog();

            while( entry != null )
            {
                entry.setIdentifier( xidIdentifier );
                entry.accept( handler );

                startedAtPosition = scratch.position();
                entry = LogIoUtils.readEntry( scratch, channel, xaCommandReader );
                monitor.bytesRead( scratch.position() - startedAtPosition );
            }
        }
        catch( IOException e )
        {
            handler.endLog( false );
            throw e;
        }

        /*
         * This is placed here instead of in the try because if one of the delegates in the handler chain
         * throws an exception in endLog(), some will have been called with endLog( true ) and will be called again
         * with endLog( false ) from the catch clause.
         * Having the endLog() here at least ensures that all of them will be called once with the same argument and
         * if that fails the exception will be thrown.
         */
        handler.endLog( true );
    }
}
