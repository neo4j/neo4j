package org.neo4j.coreedge.raft.log.debug;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ListIterator;

import org.neo4j.coreedge.raft.log.DamagedLogStorageException;
import org.neo4j.coreedge.raft.log.EntryRecord;
import org.neo4j.coreedge.raft.log.segmented.DisposedException;
import org.neo4j.coreedge.raft.log.segmented.FileNames;
import org.neo4j.coreedge.raft.log.segmented.RecoveryProtocol;
import org.neo4j.coreedge.raft.log.segmented.SegmentFile;
import org.neo4j.coreedge.raft.log.segmented.SegmentHeader;
import org.neo4j.coreedge.raft.log.segmented.Segments;
import org.neo4j.coreedge.raft.net.CoreReplicatedContentMarshal;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.cursor.IOCursor;
import org.neo4j.helpers.Args;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;


public class DumpSegmentedRaftLog
{
    private final FileSystemAbstraction fileSystem;
    private static final String TO_FILE = "tofile";
    private ChannelMarshal<ReplicatedContent> marshal = new CoreReplicatedContentMarshal();

    public DumpSegmentedRaftLog( FileSystemAbstraction fileSystem, ChannelMarshal<ReplicatedContent> marshal )
    {
        this.fileSystem = fileSystem;
        this.marshal = marshal;
    }

    public int dump( String filenameOrDirectory, PrintStream out )
            throws IOException, DamagedLogStorageException, DisposedException
    {
        LogProvider logProvider = NullLogProvider.getInstance();
        int logsFound = 0;
        RecoveryProtocol recoveryProtocol =
                new RecoveryProtocol( fileSystem, new FileNames( new File( filenameOrDirectory ) ), marshal,
                        logProvider );
        Segments segments = recoveryProtocol.run().getSegments();

        ListIterator<SegmentFile> segmentFileIterator = segments.getSegmentFileIteratorAtStart();

        SegmentFile currentSegmentFile;
        while (segmentFileIterator.hasNext())
        {
            currentSegmentFile = segmentFileIterator.next();
            logsFound++;
            out.println( "=== " + currentSegmentFile.getFilename() + " ===" );

            SegmentHeader header = currentSegmentFile.header();

            out.println( header.toString() );

            try ( IOCursor<EntryRecord> cursor = currentSegmentFile.getReader( header.prevIndex() + 1 ) )
            {
                while ( cursor.next() )
                {
                    out.println( cursor.get().toString() );
                }
            }
        }
        return logsFound;
    }

    public static void main( String[] args ) throws IOException, DisposedException, DamagedLogStorageException
    {
        Args arguments = Args.withFlags( TO_FILE ).parse( args );
        try ( Printer printer = getPrinter( arguments ) )
        {
            for ( String fileAsString : arguments.orphans() )
            {
                System.out.println( "Reading file " + fileAsString );
                new DumpSegmentedRaftLog( new DefaultFileSystemAbstraction(), new CoreReplicatedContentMarshal() )
                        .dump( fileAsString, printer.getFor( fileAsString ) );
            }
        }
    }

    public static Printer getPrinter( Args args )
    {
        boolean toFile = args.getBoolean( TO_FILE, false, true );
        return toFile ? new DumpSegmentedRaftLog.FilePrinter() : SYSTEM_OUT_PRINTER;
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
