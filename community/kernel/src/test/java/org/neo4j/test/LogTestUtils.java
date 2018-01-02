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
package org.neo4j.test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.function.Predicate;
import org.neo4j.helpers.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles.LogVersionVisitor;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalWritableLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableVersionableLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReaderLogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.WritableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel.DEFAULT_READ_AHEAD_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.writeLogHeader;

/**
 * Utility for reading and filtering logical logs as well as tx logs.
 *
 * @author Mattias Persson
 */
public class LogTestUtils
{
    public static interface LogHook<RECORD> extends Predicate<RECORD>
    {
        void file( File file );

        void done( File file );
    }

    public static abstract class LogHookAdapter<RECORD> implements LogHook<RECORD>
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

    public static final LogHook<Pair<Byte, List<byte[]>>> NO_FILTER = new LogHookAdapter<Pair<Byte,List<byte[]>>>()
    {
        @Override
        public boolean test( Pair<Byte, List<byte[]>> item )
        {
            return true;
        }
    };

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

    public static void assertLogContains( FileSystemAbstraction fileSystem, String logPath,
            LogEntry... expectedEntries ) throws IOException
    {
        try ( LogEntryCursor cursor = openLog( fileSystem, new File( logPath ) ) )
        {
            int entryNo = 0;
            while ( cursor.next() )
            {
                assertTrue( "The log contained more entries than we expected!", entryNo < expectedEntries.length );

                LogEntry expectedEntry = expectedEntries[entryNo];
                assertEquals( "Unexpected entry at entry number " + entryNo, cursor.get(), expectedEntry );
                entryNo++;
            }

            if ( entryNo < expectedEntries.length )
            {
                fail( "Log ended prematurely. Expected to find '" + expectedEntries[entryNo].toString() +
                      "' as log entry number " + entryNo + ", instead there were no more log entries." );
            }
        }
    }

    /**
     * Opens a {@link LogEntryCursor} over all log files found in the storeDirectory
     *
     * @param fs {@link FileSystemAbstraction} to find {@code storeDirectory} in.
     * @param storeDirectory the store directory where the log files are.
     * @param logVersionCallback will be kept up to date with which log version the cursor is at.
     */
    public static LogEntryCursor openLogs( final FileSystemAbstraction fs, File storeDirectory )
    {
        PhysicalLogFiles logFiles = new PhysicalLogFiles( storeDirectory, fs );
        File firstFile = logFiles.getLogFileForVersion( logFiles.getLowestLogVersion() );
        return openLogEntryCursor( fs, firstFile, new ReaderLogVersionBridge( fs, logFiles ) );
    }

    /**
     * Opens a {@link LogEntryCursor} over one log file
     */
    public static LogEntryCursor openLog( FileSystemAbstraction fs, File log )
    {
        return openLogEntryCursor( fs, log, LogVersionBridge.NO_MORE_CHANNELS );
    }

    private static LogEntryCursor openLogEntryCursor( FileSystemAbstraction fs, File firstFile,
            LogVersionBridge versionBridge )
    {
        StoreChannel channel = null;
        try
        {
            channel = fs.open( firstFile, "r" );
            ByteBuffer buffer = ByteBuffer.allocate( LogHeader.LOG_HEADER_SIZE );
            LogHeader header = LogHeaderReader.readLogHeader( buffer, channel, true );

            PhysicalLogVersionedStoreChannel logVersionedChannel = new PhysicalLogVersionedStoreChannel( channel,
                    header.logVersion, header.logFormatVersion );
            ReadableVersionableLogChannel logChannel = new ReadAheadLogChannel( logVersionedChannel,
                    versionBridge, ReadAheadLogChannel.DEFAULT_READ_AHEAD_SIZE );

            return new LogEntryCursor( logChannel );
        }
        catch ( Throwable t )
        {
            if ( channel != null )
            {
                try
                {
                    channel.close();
                }
                catch ( IOException e )
                {
                    t.addSuppressed( e );
                }
            }
            throw new RuntimeException( t );
        }
    }

    private static void replace( File tempFile, File file )
    {
        file.renameTo( new File( file.getAbsolutePath() + "." + System.currentTimeMillis() ) );
        tempFile.renameTo( file );
    }

    public static File[] filterNeostoreLogicalLog( FileSystemAbstraction fileSystem,
            String storeDir, LogHook<LogEntry> filter ) throws IOException
    {
        PhysicalLogFiles logFiles = new PhysicalLogFiles( new File( storeDir ), fileSystem );
        final List<File> files = new ArrayList<>();
        logFiles.accept( new LogVersionVisitor()
        {
            @Override
            public void visit( File file, long logVersion )
            {
                files.add( file );
            }
        } );
        for ( File file : files )
        {
            File filteredLog = filterNeostoreLogicalLog( fileSystem, file, filter );
            replace( filteredLog, file );
        }

        return files.toArray( new File[files.size()] );
    }

    public static File filterNeostoreLogicalLog( FileSystemAbstraction fileSystem, File file,
            final LogHook<LogEntry> filter )
            throws IOException
    {
        filter.file( file );
        File tempFile = new File( file.getAbsolutePath() + ".tmp" );
        fileSystem.deleteFile( tempFile );
        try ( StoreChannel in = fileSystem.open( file, "r" );
                StoreChannel out = fileSystem.open( tempFile, "rw" ) )
        {
            ByteBuffer buffer = ByteBuffer.allocate( 1024 * 1024 );
            LogHeader logHeader = transferLogicalLogHeader( in, out, buffer );
            PhysicalLogVersionedStoreChannel outChannel =
                    new PhysicalLogVersionedStoreChannel( out, logHeader.logVersion, logHeader.logFormatVersion );
            final WritableLogChannel outBuffer = new PhysicalWritableLogChannel( outChannel );

            PhysicalLogVersionedStoreChannel inChannel =
                    new PhysicalLogVersionedStoreChannel( in, logHeader.logVersion, logHeader.logFormatVersion );
            ReadableLogChannel inBuffer = new ReadAheadLogChannel( inChannel, LogVersionBridge.NO_MORE_CHANNELS,
                    DEFAULT_READ_AHEAD_SIZE );
            LogEntryReader<ReadableLogChannel> entryReader = new VersionAwareLogEntryReader<>();

            LogEntry entry;
            while ( (entry = entryReader.readLogEntry( inBuffer )) != null )
            {
                if ( filter.test( entry ) )
                {   // TODO allright, write to outBuffer
                }
            }
        }

        return tempFile;
    }

    private static LogHeader transferLogicalLogHeader( StoreChannel in, StoreChannel out, ByteBuffer buffer )
            throws IOException
    {
        LogHeader header = readLogHeader( buffer, in, true );
        writeLogHeader( buffer, header.logVersion, header.lastCommittedTxId );
        buffer.flip();
        out.write( buffer );
        return header;
    }

    public static NonCleanLogCopy copyLogicalLog( FileSystemAbstraction fileSystem,
            File logBaseFileName ) throws IOException
    {
        char active = '1';
        File activeLog = new File( logBaseFileName.getPath() + ".active" );
        ByteBuffer buffer = ByteBuffer.allocate( 1024 );
        File activeLogBackup;
        try ( StoreChannel af = fileSystem.open( activeLog, "r" ) )
        {
            af.read( buffer );
            buffer.flip();
            activeLogBackup = new File( logBaseFileName.getPath() + ".bak.active" );
            try ( StoreChannel activeCopy = fileSystem.open( activeLogBackup, "rw" ) )
            {
                activeCopy.write( buffer );
            }
        }
        buffer.flip();
        active = buffer.asCharBuffer().get();
        buffer.clear();
        File currentLog = new File( logBaseFileName.getPath() + "." + active );
        File currentLogBackup = new File( logBaseFileName.getPath() + ".bak." + active );
        try ( StoreChannel source = fileSystem.open( currentLog, "r" );
              StoreChannel dest = fileSystem.open( currentLogBackup, "rw" ) )
        {
            int read = -1;
            do
            {
                read = source.read( buffer );
                buffer.flip();
                dest.write( buffer );
                buffer.clear();
            }
            while ( read == 1024 );
        }
        return new NonCleanLogCopy(
                new FileBackup( activeLog, activeLogBackup, fileSystem ),
                new FileBackup( currentLog, currentLogBackup, fileSystem ) );
    }

    private static class FileBackup
    {
        private final File file, backup;
        private final FileSystemAbstraction fileSystem;

        FileBackup( File file, File backup, FileSystemAbstraction fileSystem )
        {
            this.file = file;
            this.backup = backup;
            this.fileSystem = fileSystem;
        }

        public void restore() throws IOException
        {
            fileSystem.deleteFile( file );
            fileSystem.renameFile( backup, file );
        }
    }

    public static class NonCleanLogCopy
    {
        private final FileBackup[] backups;

        NonCleanLogCopy( FileBackup... backups )
        {
            this.backups = backups;
        }

        public void reinstate() throws IOException
        {
            for ( FileBackup backup : backups )
            {
                backup.restore();
            }
        }
    }
}
