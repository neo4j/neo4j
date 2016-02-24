/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.raft.log.debug;

import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles.getLogVersion;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.TreeSet;

import org.neo4j.coreedge.raft.log.PhysicalRaftLogEntryCursor;
import org.neo4j.coreedge.raft.log.RaftRecordCursor;
import org.neo4j.coreedge.raft.net.CoreReplicatedContentMarshal;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.helpers.Args;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;

public class DumpPhysicalRaftLog
{
    private final FileSystemAbstraction fileSystem;
    private static final String TO_FILE = "tofile";
    private ChannelMarshal<ReplicatedContent> marshal = new CoreReplicatedContentMarshal();


    public DumpPhysicalRaftLog( FileSystemAbstraction fileSystem, ChannelMarshal<ReplicatedContent> marshal )
    {
        this.fileSystem = fileSystem;
        this.marshal = marshal;
    }

    public int dump( String filenameOrDirectory, String logPrefix, PrintStream out ) throws IOException
    {
        int logsFound = 0;
        for ( String fileName : filenamesOf( filenameOrDirectory, logPrefix ) )
        {
            logsFound++;
            out.println( "=== " + fileName + " ===" );
            StoreChannel fileChannel = fileSystem.open( new File( fileName ), "r" );

            LogHeader logHeader;
            try
            {
                logHeader = readLogHeader( ByteBuffer.allocateDirect( LOG_HEADER_SIZE ), fileChannel, false );
            }
            catch ( IOException ex )
            {
                out.println( "Unable to read timestamp information, no records in logical log." );
                out.println( ex.getMessage() );
                fileChannel.close();
                throw ex;
            }
            out.println( "Logical log format:" + logHeader.logFormatVersion + "version: " + logHeader.logVersion +
                    " with prev committed tx[" + logHeader.lastCommittedTxId + "]" );

            PhysicalLogVersionedStoreChannel channel = new PhysicalLogVersionedStoreChannel(
                    fileChannel, logHeader.logVersion, logHeader.logFormatVersion );
            ReadableLogChannel logChannel = new ReadAheadLogChannel( channel, NO_MORE_CHANNELS );


            try ( PhysicalRaftLogEntryCursor cursor = new PhysicalRaftLogEntryCursor( new RaftRecordCursor<>( logChannel, marshal ) ) )
            {
                while ( cursor.next() )
                {
                    out.println( cursor.get().toString() );
                }
            }
        }
        return logsFound;
    }

    protected String[] filenamesOf( String filenameOrDirectory, final String prefix )
    {

        File file = new File( filenameOrDirectory );
        if ( fileSystem.isDirectory( file ) )
        {
            File[] files = fileSystem.listFiles( file, ( dir, name ) -> name.contains( prefix ) && !name.contains( "active" ) );
            Collection<String> result = new TreeSet<>( sequentialComparator() );
            for ( File file1 : files )
            {
                result.add( file1.getPath() );
            }
            return result.toArray( new String[result.size()] );
        }
        else
        {
            return new String[]{filenameOrDirectory};
        }
    }


    private static Comparator<? super String> sequentialComparator()

    {
        return new Comparator<String>()
        {
            @Override
            public int compare( String o1, String o2 )
            {
                return versionOf( o1 ).compareTo( versionOf( o2 ) );
            }

            private Long versionOf( String string )
            {
                try
                {
                    return getLogVersion( string );
                }
                catch ( RuntimeException ignored )
                {
                    return Long.MAX_VALUE;
                }
            }
        };
    }

    public static void main( String args[] ) throws IOException
    {
        Args arguments = Args.withFlags( TO_FILE ).parse( args );
        try ( Printer printer = getPrinter( arguments ) )
        {
            for ( String fileAsString : arguments.orphans() )
            {
                System.out.println("Reading file " + fileAsString );
                new DumpPhysicalRaftLog( new DefaultFileSystemAbstraction(), new CoreReplicatedContentMarshal() )
                        .dump( fileAsString, PhysicalLogFile.DEFAULT_NAME, printer.getFor( fileAsString ) );
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
}
