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
package org.neo4j.test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Predicate;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;

import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;

/**
 * Utility for reading and filtering logical logs as well as tx logs.
 *
 * @author Mattias Persson
 */
public class LogTestUtils
{
    private LogTestUtils()
    {
    }

    public interface LogHook<RECORD> extends Predicate<RECORD>
    {
        void file( File file );

        void done( File file );
    }

    public abstract static class LogHookAdapter<RECORD> implements LogHook<RECORD>
    {
        @Override
        public void file( File file )
        {   // Do nothing
        }

        @Override
        public void done( File file )
        {   // Do nothing
        }
    }

    public static class CountingLogHook<RECORD> extends LogHookAdapter<RECORD>
    {
        private int count;

        @Override
        public boolean test( RECORD item )
        {
            count++;
            return true;
        }

        public int getCount()
        {
            return count;
        }
    }

    public static File[] filterNeostoreLogicalLog( LogFiles logFiles, FileSystemAbstraction fileSystem,
            LogHook<LogEntry> filter ) throws IOException
    {
        File[] files = logFiles.logFiles();
        for ( File file : files )
        {
            filterTransactionLogFile( fileSystem, file, filter );
        }

        return files;
    }

    static void filterTransactionLogFile( FileSystemAbstraction fileSystem, File file, final LogHook<LogEntry> filter )
            throws IOException
    {
        filter.file( file );
        try ( StoreChannel in = fileSystem.open( file, OpenMode.READ ) )
        {
            LogHeader logHeader = readLogHeader( ByteBuffer.allocate( LOG_HEADER_SIZE ), in, true, file );
            PhysicalLogVersionedStoreChannel inChannel =
                    new PhysicalLogVersionedStoreChannel( in, logHeader.logVersion, logHeader.logFormatVersion );
            ReadableLogChannel inBuffer = new ReadAheadLogChannel( inChannel );
            LogEntryReader<ReadableLogChannel> entryReader = new VersionAwareLogEntryReader<>();

            LogEntry entry;
            while ( (entry = entryReader.readLogEntry( inBuffer )) != null )
            {
                filter.test( entry );
            }
        }
    }
}
