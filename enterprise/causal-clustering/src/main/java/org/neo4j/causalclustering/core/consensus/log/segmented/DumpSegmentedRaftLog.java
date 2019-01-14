/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;

import org.neo4j.causalclustering.core.consensus.log.EntryRecord;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.messaging.CoreReplicatedContentMarshal;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.cursor.IOCursor;
import org.neo4j.helpers.Args;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.Clocks;

class DumpSegmentedRaftLog
{
    private final FileSystemAbstraction fileSystem;
    private static final String TO_FILE = "tofile";
    private ChannelMarshal<ReplicatedContent> marshal = new CoreReplicatedContentMarshal();

    private DumpSegmentedRaftLog( FileSystemAbstraction fileSystem, ChannelMarshal<ReplicatedContent> marshal )
    {
        this.fileSystem = fileSystem;
        this.marshal = marshal;
    }

    private int dump( String filenameOrDirectory, PrintStream out )
            throws IOException, DamagedLogStorageException, DisposedException
    {
        LogProvider logProvider = NullLogProvider.getInstance();
        final int[] logsFound = {0};
        FileNames fileNames = new FileNames( new File( filenameOrDirectory ) );
        ReaderPool readerPool = new ReaderPool( 0, logProvider, fileNames, fileSystem, Clocks.systemClock() );
        RecoveryProtocol recoveryProtocol =
                new RecoveryProtocol( fileSystem, fileNames, readerPool, marshal, logProvider );
        Segments segments = recoveryProtocol.run().segments;

        segments.visit( segment -> {
                logsFound[0]++;
                out.println( "=== " + segment.getFilename() + " ===" );

                SegmentHeader header = segment.header();

                out.println( header.toString() );

                try ( IOCursor<EntryRecord> cursor = segment.getCursor( header.prevIndex() + 1 ) )
                {
                    while ( cursor.next() )
                    {
                        out.println( cursor.get().toString() );
                    }
                }
                catch ( DisposedException | IOException e )
                {
                    e.printStackTrace();
                    System.exit( -1 );
                    return true;
                }

            return false;
        } );

        return logsFound[0];
    }

    public static void main( String[] args )
    {
        Args arguments = Args.withFlags( TO_FILE ).parse( args );
        try ( Printer printer = getPrinter( arguments ) )
        {
            for ( String fileAsString : arguments.orphans() )
            {
                System.out.println( "Reading file " + fileAsString );

                try ( DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
                {
                    new DumpSegmentedRaftLog( fileSystem, new CoreReplicatedContentMarshal() )
                            .dump( fileAsString, printer.getFor( fileAsString ) );
                }
                catch ( IOException | DisposedException | DamagedLogStorageException e )
                {
                    e.printStackTrace();
                }
            }
        }
    }

    private static Printer getPrinter( Args args )
    {
        boolean toFile = args.getBoolean( TO_FILE, false, true );
        return toFile ? new DumpSegmentedRaftLog.FilePrinter() : SYSTEM_OUT_PRINTER;
    }

    interface Printer extends AutoCloseable
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
                close();
                File dumpFile = new File( dir, "dump-logical-log.txt" );
                System.out.println( "Redirecting the output to " + dumpFile.getPath() );
                out = new PrintStream( dumpFile );
                directory = dir;
            }
            return out;
        }

        @Override
        public void close()
        {
            if ( out != null )
            {
                out.close();
            }
        }
    }
}
