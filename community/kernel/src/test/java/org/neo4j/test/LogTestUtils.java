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
package org.neo4j.test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.Xid;

import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.nioneo.xa.CommandReaderFactory;
import org.neo4j.kernel.impl.nioneo.xa.LogDeserializer;
import org.neo4j.kernel.impl.transaction.xaframework.CommandWriter;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntryReader;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntryWriterv1;
import org.neo4j.kernel.impl.transaction.xaframework.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalLogFiles.LogVersionVisitor;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalWritableLogChannel;
import org.neo4j.kernel.impl.transaction.xaframework.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.xaframework.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.xaframework.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.xaframework.WritableLogChannel;
import org.neo4j.kernel.impl.util.Cursor;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.neo4j.kernel.impl.transaction.xaframework.ReadAheadLogChannel.DEFAULT_READ_AHEAD_SIZE;

/**
 * Utility for reading and filtering logical logs as well as tx logs.
 *
 * @author Mattias Persson
 */
// TODO 2.2-future rewrite this using the new APIs
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
        public boolean accept( Pair<Byte, List<byte[]>> item )
        {
            return true;
        }
    };

    public static class CountingLogHook<RECORD> extends LogHookAdapter<RECORD>
    {
        private int count;

        @Override
        public boolean accept( RECORD item )
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
            LogEntry ... expectedEntries ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect( 9 + Xid.MAXGTRIDSIZE
                + Xid.MAXBQUALSIZE * 10 );
        try ( StoreChannel fileChannel = fileSystem.open( new File( logPath ), "r" ) )
        {
            // Always a header
            VersionAwareLogEntryReader.readLogHeader( buffer, fileChannel, true );

            // Read all log entries
            final List<LogEntry> entries = new ArrayList<>();
            LogDeserializer deserializer = new LogDeserializer( CommandReaderFactory.DEFAULT );


            Visitor<LogEntry, IOException> visitor = new Visitor<LogEntry, IOException>()
            {
                @Override
                public boolean visit( LogEntry entry ) throws IOException
                {
                    entries.add( entry );
                    return true;
                }
            };

            ReadableLogChannel logChannel = new ReadAheadLogChannel(new PhysicalLogVersionedStoreChannel(fileChannel), LogVersionBridge.NO_MORE_CHANNELS, 4096);

            try ( Cursor<IOException> cursor = deserializer.cursor( logChannel, visitor ) )
            {
                cursor.next();
            }

            // Assert entries are what we expected
            for(int entryNo=0;entryNo < expectedEntries.length; entryNo++)
            {
                LogEntry expectedEntry = expectedEntries[entryNo];
                if(entries.size() <= entryNo)
                {
                    fail("Log ended prematurely. Expected to find '" + expectedEntry.toString() + "' as log entry number "+entryNo+", instead there were no more log entries." );
                }

                LogEntry actualEntry = entries.get( entryNo );

                assertThat( "Unexpected entry at entry number " + entryNo, actualEntry, is( expectedEntry ) );
            }

            // And assert log does not contain more entries
            assertThat( "The log contained more entries than we expected!", entries.size(), is( expectedEntries.length ) );
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
            final WritableLogChannel outBuffer = new PhysicalWritableLogChannel(
                    new PhysicalLogVersionedStoreChannel( out ) );
            ByteBuffer buffer = ByteBuffer.allocate( 1024*1024 );
            LogEntryWriter entryWriter = new LogEntryWriterv1( outBuffer, new CommandWriter( outBuffer ) );
            transferLogicalLogHeader( in, outBuffer, buffer );

            ReadableLogChannel inBuffer = new ReadAheadLogChannel( new PhysicalLogVersionedStoreChannel( in ),
                    LogVersionBridge.NO_MORE_CHANNELS, DEFAULT_READ_AHEAD_SIZE );
            LogEntryReader<ReadableLogChannel> entryReader = new VersionAwareLogEntryReader(
                    CommandReaderFactory.DEFAULT );
            LogEntry entry;
            while ( (entry = entryReader.readLogEntry( inBuffer )) != null )
            {
                if ( filter.accept( entry ) )
                {   // TODO allright, write to outBuffer
                }
            }
        }

        return tempFile;
    }

    private static void transferLogicalLogHeader( StoreChannel in, WritableLogChannel outBuffer,
            ByteBuffer buffer ) throws IOException
    {
        long[] header = VersionAwareLogEntryReader.readLogHeader( buffer, in, true );
        VersionAwareLogEntryReader.writeLogHeader( buffer, header[0], header[1] );
        byte[] headerBytes = new byte[buffer.limit()];
        buffer.get( headerBytes );
        outBuffer.put( headerBytes, headerBytes.length );
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
