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
import java.nio.channels.FileChannel;

import org.neo4j.kernel.impl.nioneo.xa.command.LogHandler;
import org.neo4j.kernel.impl.nioneo.xa.command.LogReader;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;

public class RecoveryLogDeserializer implements LogReader<FileChannel>
{
    private LogHandler handler;
    private final ByteBuffer scratch;

    private XaCommandReader xaCommandReader;

    public RecoveryLogDeserializer( ByteBuffer scratch )
    {
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
        // this is recovery, the existing identifier will do nicely
    }

    @Override
    public void setLogHandler( LogHandler handler )
    {
        this.handler = handler;
    }

    public void read( FileChannel channel ) throws IOException
    {
        handler.startLog();
        long position = channel.position();
        LogEntry entry;
        while ( ( entry = LogIoUtils.readEntry( scratch, channel, xaCommandReader ) ) != null )
        {
            if ( entry instanceof LogEntry.Start )
            {
                ((LogEntry.Start) entry).setStartPosition( position );
            }
            entry.accept( handler );
            position = channel.position();
        }
        handler.endLog( true );

    }
}
