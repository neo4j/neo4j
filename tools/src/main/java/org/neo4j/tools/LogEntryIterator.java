/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.tools;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import javax.transaction.xa.Xid;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableVersionableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader;

public class LogEntryIterator implements Iterator<LogEntry>, Closeable
{
    private final StoreChannel storeChannel;
    private final IOCursor<LogEntry> cursor;

    private boolean hasNext;

    public LogEntryIterator( FileSystemAbstraction fs, File log ) throws IOException
    {
        this.storeChannel = fs.open( log, "r" );
        this.cursor = newLogEntryCursor( storeChannel );
        this.hasNext = cursor.next();
    }

    @Override
    public boolean hasNext()
    {
        return hasNext;
    }

    @Override
    public LogEntry next()
    {
        try
        {
            if ( !hasNext )
            {
                throw new NoSuchElementException();
            }
            LogEntry entry = cursor.get();
            hasNext = cursor.next();
            return entry;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
        Throwable error = null;
        try
        {
            cursor.close();
        }
        catch ( Throwable t )
        {
            error = t;
        }
        try
        {
            storeChannel.close();
        }
        catch ( Throwable t )
        {
            if ( error == null )
            {
                error = t;
            }
            else
            {
                error.addSuppressed( t );
            }
        }
        if ( error != null )
        {
            throw new RuntimeException( error );
        }
    }

    IOCursor<LogEntry> newLogEntryCursor( StoreChannel storeChannel ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect( 9 + Xid.MAXGTRIDSIZE + Xid.MAXBQUALSIZE * 10 );
        LogHeader header = LogHeaderReader.readLogHeader( buffer, storeChannel, false );
        Objects.requireNonNull( header );

        PhysicalLogVersionedStoreChannel versionedStoreChannel = new PhysicalLogVersionedStoreChannel( storeChannel,
                header.logVersion, header.logFormatVersion );
        ReadableVersionableLogChannel channel = new ReadAheadLogChannel( versionedStoreChannel,
                LogVersionBridge.NO_MORE_CHANNELS, 4096 );

        return new LogEntryCursor( channel );
    }
}
