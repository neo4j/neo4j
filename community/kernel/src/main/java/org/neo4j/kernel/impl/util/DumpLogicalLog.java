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
package org.neo4j.kernel.impl.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.TimeZone;
import java.util.regex.Pattern;

import org.neo4j.helpers.Args;
import org.neo4j.helpers.Predicate;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.log.FilteringIOCursor;
import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableVersionableLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReaderLogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.TransactionLogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;

import static java.util.TimeZone.getTimeZone;

import static org.neo4j.helpers.Format.DEFAULT_TIME_ZONE;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel.DEFAULT_READ_AHEAD_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;

public class DumpLogicalLog
{
    private static final String TO_FILE = "tofile";
    private static final String TX_FILTER = "txfilter";

    private final FileSystemAbstraction fileSystem;

    public DumpLogicalLog( FileSystemAbstraction fileSystem )
    {
        this.fileSystem = fileSystem;
    }

    public void dump( String filenameOrDirectory, final PrintStream out, TimeZone timeZone, String regex )
            throws IOException
    {
        File file = new File( filenameOrDirectory );
        printFile( file, out );
        File firstFile;
        LogVersionBridge bridge;
        if ( file.isDirectory() )
        {
            // Use natural log version bridging if a directory is supplied
            final PhysicalLogFiles logFiles = new PhysicalLogFiles( file, fileSystem );
            bridge = new ReaderLogVersionBridge( fileSystem, logFiles )
            {
                @Override
                public LogVersionedStoreChannel next( LogVersionedStoreChannel channel ) throws IOException
                {
                    LogVersionedStoreChannel next = super.next( channel );
                    if ( next != channel )
                    {
                        printFile( logFiles.getLogFileForVersion( next.getVersion() ), out );
                    }
                    return next;
                }
            };
            firstFile = logFiles.getLogFileForVersion( logFiles.getLowestLogVersion() );
        }
        else
        {
            // Use no bridging, simple reading this single log file if a file is supplied
            firstFile = file;
            bridge = NO_MORE_CHANNELS;
        }

        StoreChannel fileChannel = fileSystem.open( firstFile, "r" );
        ByteBuffer buffer = ByteBuffer.allocateDirect( LOG_HEADER_SIZE );

        LogHeader logHeader;
        try
        {
            logHeader = readLogHeader( buffer, fileChannel, false );
        }
        catch ( IOException ex )
        {
            out.println( "Unable to read timestamp information, no records in logical log." );
            out.println( ex.getMessage() );
            fileChannel.close();
            throw ex;
        }
        out.println( "Logical log format: " + logHeader.logFormatVersion + " version: " + logHeader.logVersion +
                " with prev committed tx[" + logHeader.lastCommittedTxId + "]" );

        PhysicalLogVersionedStoreChannel channel = new PhysicalLogVersionedStoreChannel(
                fileChannel, logHeader.logVersion, logHeader.logFormatVersion );
        ReadableVersionableLogChannel logChannel = new ReadAheadLogChannel( channel, bridge, DEFAULT_READ_AHEAD_SIZE );
        LogEntryReader<ReadableLogChannel> entryReader = new VersionAwareLogEntryReader<>();

        IOCursor<LogEntry> entryCursor = new LogEntryCursor( entryReader, logChannel );
        TransactionLogEntryCursor transactionCursor = new TransactionLogEntryCursor( entryCursor );
        try ( IOCursor<LogEntry[]> cursor = regex == null ? transactionCursor :
                new FilteringIOCursor<>( transactionCursor, new TransactionRegexCriteria( regex, timeZone ) ) )
        {
            while ( cursor.next() )
            {
                for ( LogEntry entry : cursor.get() )
                {
                    out.println( entry.toString( timeZone ) );
                }
            }
        }
    }

    private static void printFile( File file, PrintStream out )
    {
        out.println( "=== " + file.getAbsolutePath() + " ===" );
    }

    protected class TransactionRegexCriteria implements Predicate<LogEntry[]>
    {
        protected Pattern pattern;
        private final TimeZone timeZone;

        TransactionRegexCriteria( String regex, TimeZone timeZone )
        {
            pattern = Pattern.compile( regex );
            this.timeZone = timeZone;
        }

        @Override
        public boolean accept( LogEntry[] transaction )
        {
            for ( LogEntry entry : transaction )
            {
                if ( pattern.matcher( entry.toString( timeZone ) ).find() )
                {
                    return true;
                }
            }
            return false;
        }
    }

    protected static boolean isAGraphDatabaseDirectory( String fileName )
    {
        File file = new File( fileName );
        return file.isDirectory() && new File( file, MetaDataStore.DEFAULT_NAME ).exists();
    }

    /**
     * Usage: [--txfilter "regex"] [--tofile] storeDirOrFile1 storeDirOrFile2 ...
     *
     * --txfilter
     * Will match regex against each {@link LogEntry} and if there is a match,
     * include transaction containing the LogEntry in the dump.
     * regex matching is done with {@link Pattern}
     *
     * --tofile
     * Redirects output to dump-logical-log.txt in the store directory
     */
    public static void main( String args[] ) throws IOException
    {
        Args arguments = Args.withFlags( TO_FILE ).parse( args );
        TimeZone timeZone = parseTimeZoneConfig( arguments );
        String regex = arguments.get( TX_FILTER );
        try ( Printer printer = getPrinter( arguments ) )
        {
            for ( String fileAsString : arguments.orphans() )
            {
                new DumpLogicalLog( new DefaultFileSystemAbstraction() )
                        .dump( fileAsString, printer.getFor( fileAsString ), timeZone, regex );
            }
        }
    }

    public static Printer getPrinter( Args args )
    {
        boolean toFile = args.getBoolean( TO_FILE, false, true );
        return toFile ? new FilePrinter() : SYSTEM_OUT_PRINTER;
    }

    public interface Printer extends AutoCloseable
    {
        PrintStream getFor( String file ) throws FileNotFoundException;

        @Override
        void close();
    }

    private static final Printer SYSTEM_OUT_PRINTER = new Printer()
    {
        @Override
        public PrintStream getFor( String file )
        {
            return System.out;
        }

        @Override
        public void close()
        {   // Don't close System.out
        }
    };

    private static class FilePrinter implements Printer
    {
        private File directory;
        private PrintStream out;

        @Override
        public PrintStream getFor( String file ) throws FileNotFoundException
        {
            File absoluteFile = new File( file ).getAbsoluteFile();
            File dir = absoluteFile.isDirectory() ? absoluteFile : absoluteFile.getParentFile();
            if ( !dir.equals( directory ) )
            {
                safeClose();
                File dumpFile = new File( dir, "dump-logical-log.txt" );
                System.out.println( "Redirecting the output to " + dumpFile.getPath() );
                out = new PrintStream( dumpFile );
                directory = dir;
            }
            return out;
        }

        private void safeClose()
        {
            if ( out != null )
            {
                out.close();
            }
        }

        @Override
        public void close()
        {
            safeClose();
        }
    }

    public static TimeZone parseTimeZoneConfig( Args arguments )
    {
        return getTimeZone( arguments.get( "timezone", DEFAULT_TIME_ZONE.getID() ) );
    }
}
