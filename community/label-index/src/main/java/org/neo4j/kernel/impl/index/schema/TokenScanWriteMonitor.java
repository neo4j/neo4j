/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.index.schema;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.internal.helpers.Args;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FlushableChannel;
import org.neo4j.io.fs.PhysicalFlushableChannel;
import org.neo4j.io.fs.ReadAheadChannel;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static org.neo4j.io.fs.ReadAheadChannel.DEFAULT_READ_AHEAD_SIZE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

/**
 * A {@link NativeTokenScanWriter.WriteMonitor} which writes all interactions to a .writelog file, which has configurable rotation and pruning.
 * This class also has a {@link #main(String[])} method for dumping the contents of such write log to console or file, as text.
 */
public class TokenScanWriteMonitor implements NativeTokenScanWriter.WriteMonitor
{
    private static final byte TYPE_PREPARE_ADD = 0;
    private static final byte TYPE_PREPARE_REMOVE = 1;
    private static final byte TYPE_MERGE_ADD = 2;
    private static final byte TYPE_MERGE_REMOVE = 3;
    private static final byte TYPE_RANGE = 4;
    private static final byte TYPE_FLUSH = 5;
    private static final byte TYPE_SESSION_END = 6;

    private static final String ARG_TOFILE = "tofile";
    private static final String ARG_TXFILTER = "txfilter";

    private final FileSystemAbstraction fs;
    private final Monitor monitor;
    private final SystemNanoClock clock;
    private final Path storeDir;
    private final Path file;
    private FlushableChannel channel;
    private final Lock lock = new ReentrantLock();
    private final LongAdder position = new LongAdder();
    private final long rotationThreshold;
    private final long pruneThreshold;

    TokenScanWriteMonitor( FileSystemAbstraction fs, DatabaseLayout databaseLayout, EntityType entityType, Config config )
    {
        this( fs, databaseLayout, config.get( GraphDatabaseInternalSettings.token_scan_write_log_rotation_threshold ), ByteUnit.Byte,
                config.get( GraphDatabaseInternalSettings.token_scan_write_log_prune_threshold ).toMillis(), TimeUnit.MILLISECONDS, entityType, NO_MONITOR,
                Clocks.nanoClock() );
    }

    TokenScanWriteMonitor( FileSystemAbstraction fs, DatabaseLayout databaseLayout,
            long rotationThreshold, ByteUnit rotationThresholdUnit,
            long pruneThreshold, TimeUnit pruneThresholdUnit, EntityType entityType, Monitor monitor, SystemNanoClock clock )
    {
        this.fs = fs;
        this.monitor = monitor;
        this.clock = clock;
        this.rotationThreshold = rotationThresholdUnit.toBytes( rotationThreshold );
        this.pruneThreshold = pruneThresholdUnit.toMillis( pruneThreshold );
        this.storeDir = databaseLayout.databaseDirectory();
        this.file = writeLogBaseFile( databaseLayout, entityType );
        try
        {
            if ( fs.fileExists( file ) )
            {
                moveAwayFile( fs.getFileSize( file ) );
            }
            this.channel = instantiateChannel();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    static Path writeLogBaseFile( DatabaseLayout databaseLayout, EntityType entityType )
    {
        Path baseFile = entityType == EntityType.NODE ? databaseLayout.labelScanStore() : databaseLayout.relationshipTypeScanStore();
        return baseFile.resolveSibling( baseFile.getFileName() + ".writelog" );
    }

    private PhysicalFlushableChannel instantiateChannel() throws IOException
    {
        return new PhysicalFlushableChannel( fs.write( file ), INSTANCE );
    }

    @Override
    public void range( long range, int tokenId )
    {
        try
        {
            channel.put( TYPE_RANGE );
            channel.putLong( range );
            channel.putInt( tokenId );
            position.add( 1 + 8 + 4 );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void prepareAdd( long txId, int offset )
    {
        prepare( TYPE_PREPARE_ADD, txId, offset );
    }

    @Override
    public void prepareRemove( long txId, int offset )
    {
        prepare( TYPE_PREPARE_REMOVE, txId, offset );
    }

    private void prepare( byte type, long txId, int offset )
    {
        try
        {
            channel.put( type );
            channel.putLong( txId );
            channel.put( (byte) offset );
            position.add( 1 + 8 + 1 );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void mergeAdd( TokenScanValue existingValue, TokenScanValue newValue )
    {
        merge( TYPE_MERGE_ADD, existingValue, newValue );
    }

    @Override
    public void mergeRemove( TokenScanValue existingValue, TokenScanValue newValue )
    {
        merge( TYPE_MERGE_REMOVE, existingValue, newValue );
    }

    private void merge( byte type, TokenScanValue existingValue, TokenScanValue newValue )
    {
        try
        {
            channel.put( type );
            channel.putLong( existingValue.bits );
            channel.putLong( newValue.bits );
            position.add( 1 + 8 + 8 );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void flushPendingUpdates()
    {
        try
        {
            channel.put( TYPE_FLUSH );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void writeSessionEnded()
    {
        try
        {
            channel.put( TYPE_SESSION_END );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }

        position.add( 1 );
        long fileSize = position.sum();
        if ( fileSize > rotationThreshold )
        {
            // Rotate
            lock.lock();
            try
            {
                channel.prepareForFlush().flush();
                channel.close();
                moveAwayFile( fileSize );
                position.reset();
                channel = instantiateChannel();
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
            finally
            {
                lock.unlock();
            }

            // Prune
            long time = clock.millis();
            long threshold = time - pruneThreshold;
            for ( Path file : fs.listFiles( storeDir, name -> name.getFileName().toString().startsWith( file.getFileName() + "-" ) ) )
            {
                long timestamp = millisOf( file );
                if ( timestamp < threshold )
                {
                    fs.deleteFile( file );
                    monitor.pruned( file, timestamp );
                }
            }
        }
    }

    static long millisOf( Path file )
    {
        String name = file.getFileName().toString();
        int dashIndex = name.lastIndexOf( '-' );
        if ( dashIndex == -1 )
        {
            return 0;
        }
        return Long.parseLong( name.substring( dashIndex + 1 ) );
    }

    @Override
    public void force()
    {
        // checkpoint does this
        lock.lock();
        try
        {
            channel.prepareForFlush().flush();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public void close()
    {
        try
        {
            channel.close();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void moveAwayFile( long fileSize ) throws IOException
    {
        Path to;
        do
        {
            to = timestampedFile();
        }
        while ( fs.fileExists( to ) );
        fs.renameFile( file, to );
        monitor.rotated( to, millisOf( to ), fileSize );
    }

    private Path timestampedFile()
    {
        return storeDir.resolve( file.getFileName() + "-" + clock.millis() );
    }

    /**
     * Dumps a token scan write log as plain text. Arguments:
     * <ul>
     *     <li>{@value #ARG_TOFILE}: dumps to a .txt file next to the writelog</li>
     *     <li>{@value #ARG_TXFILTER}: filter for which tx ids to include in the dump.
     *     <p>
     *     Consists of one or more groups separated by comma.
     *     <p>
     *     Each group is either a txId, or a txId range, e.g. 123-456
     *     </li>
     * </ul>
     * <p>
     * How to interpret the dump, e.g:
     * <pre>
     * === ..../neostore.labelscanstore.db.writelog ===
     * [1,1]+tx:6,entity:0,token:0
     * [1,1]+tx:3,entity:20,token:0
     * [1,1]+tx:4,entity:40,token:0
     * [1,1]+tx:5,entity:60,token:0
     * [2,1]+tx:8,entity:80,token:1
     * [3,1]+tx:10,entity:41,token:1
     * [4,1]+tx:9,entity:21,token:1
     * [4,1]+tx:11,entity:61,token:1
     * [4,1]+range:0,tokenId:1
     *  [00000000 00000000 00000010 00000000 00000000 00000000 00000000 00000000]
     *  [00100000 00000000 00000000 00000000 00000000 00100000 00000000 00000000]
     * [5,1]+tx:12,entity:81,token:1
     * [5,1]+range:1,tokenId:1
     *  [00000000 00000000 00000000 00000000 00000000 00000001 00000000 00000000]
     *  [00000000 00000000 00000000 00000000 00000000 00000010 00000000 00000000]
     * [6,1]+tx:13,entity:1,token:1
     * [6,1]+range:0,tokenId:1
     *  [00100000 00000000 00000010 00000000 00000000 00100000 00000000 00000000]
     *  [00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000010]
     * [7,1]+tx:14,entity:62,token:1
     * [7,1]+range:0,tokenId:1
     * </pre>
     * How to interpret a message like:
     * <pre>
     * [1,1]+tx:6,entity:0,token:0
     *  ▲ ▲ ▲   ▲        ▲       ▲
     *  │ │ │   │        │       └── token id of the change
     *  │ │ │   │        └────────── entity id of the change
     *  │ │ │   └─────────────────── id of transaction making this particular change
     *  │ │ └─────────────────────── addition, a minus means removal
     *  │ └───────────────────────── flush, local to each write session, incremented when a batch of changes is flushed internally in a writer session
     *  └─────────────────────────── write session, incremented for each {@link TokenScanStore#newWriter(PageCursorTracer)}
     * </pre>
     * How to interpret a message like:
     * <pre>
     * [4,1]+range:0,tokenId:1
     *  [00000000 00000000 00000010 00000000 00000000 00000000 00000000 00000000]
     *  [00100000 00000000 00000000 00000000 00000000 00100000 00000000 00000000]
     * </pre>
     * First the first line (parts within bracket same as above):
     * <pre>
     * [4,1]+range:0,tokenId:1
     *             ▲         ▲
     *             │         └── token id of the changed bitset to apply
     *             └──────────── range, i.e. which bitset to apply this change for
     * </pre>
     * Then the bitsets are printed
     * <pre>
     *  [00000000 00000000 00000010 00000000 00000000 00000000 00000000 00000000] : state of the bitset for this token id before the change
     *  [00100000 00000000 00000000 00000000 00000000 00100000 00000000 00000000] : bits that applied to this bitset
     *                                                                              for addition the 1-bits denotes bits to be added
     *                                                                              for removal the 1-bits denotes bits to be removed
     * </pre>
     */
    public static void main( String[] args ) throws IOException
    {
        Args arguments = Args.withFlags( ARG_TOFILE ).parse( args );
        if ( arguments.orphans().size() == 0 )
        {
            System.err.println( "Please supply database directory" );
            return;
        }

        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat( Path.of( arguments.orphans().get( 0 ) ) );
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        TxFilter txFilter = parseTxFilter( arguments.get( ARG_TXFILTER, null ) );
        PrintStream out = System.out;
        boolean redirectsToFile = arguments.getBoolean( ARG_TOFILE );

        for ( EntityType entityType : EntityType.values() )
        {
            if ( redirectsToFile )
            {
                Path outFile = Path.of( writeLogBaseFile( databaseLayout, entityType ).toAbsolutePath() + ".txt" );
                System.out.println( "Redirecting output to " + outFile );
                out = new PrintStream( new BufferedOutputStream( Files.newOutputStream( outFile ) ) );
            }
            Dumper dumper = new PrintStreamDumper( out );
            dump( fs, databaseLayout, dumper, txFilter, entityType );
            if ( redirectsToFile )
            {
                out.close();
            }
        }
    }

    public static void dump( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Dumper dumper, TxFilter txFilter, EntityType entityType )
            throws IOException
    {
        Path writeLogFile = writeLogBaseFile( databaseLayout, entityType );
        String writeLogFileBaseName = writeLogFile.getFileName().toString();
        Path[] files = fs.listFiles( databaseLayout.databaseDirectory(), name -> name.getFileName().toString().startsWith( writeLogFileBaseName ) );
        Arrays.sort( files, comparing( file -> file.getFileName().toString().equals( writeLogFileBaseName ) ? 0 : millisOf( file ) ) );
        long session = 0;
        for ( Path file : files )
        {
            dumper.file( file );
            session = dumpFile( fs, file, dumper, txFilter, session );
        }
    }

    private static long dumpFile( FileSystemAbstraction fs, Path file, Dumper dumper, TxFilter txFilter, long session ) throws IOException
    {
        try ( ReadableChannel channel = new ReadAheadChannel<>( fs.read( file ), new NativeScopedBuffer( DEFAULT_READ_AHEAD_SIZE, INSTANCE ) ) )
        {
            long range = -1;
            int tokenId = -1;
            long flush = 0;
            //noinspection InfiniteLoopStatement
            while ( true )
            {
                byte type = channel.get();
                switch ( type )
                {
                case TYPE_RANGE:
                    range = channel.getLong();
                    tokenId = channel.getInt();
                    if ( txFilter != null )
                    {
                        txFilter.clear();
                    }
                    break;
                case TYPE_PREPARE_ADD:
                case TYPE_PREPARE_REMOVE:
                    dumpPrepare( dumper, type, channel, range, tokenId, txFilter, session, flush );
                    break;
                case TYPE_MERGE_ADD:
                case TYPE_MERGE_REMOVE:
                    dumpMerge( dumper, type, channel, range, tokenId, txFilter, session, flush );
                    break;
                case TYPE_FLUSH:
                    flush++;
                    break;
                case TYPE_SESSION_END:
                    session++;
                    flush = 0;
                    break;
                default:
                    System.out.println( "Unknown type " + type + " at " + ((ReadAheadChannel) channel).position() );
                    break;
                }
            }
        }
        catch ( ReadPastEndException e )
        {
            // This is OK. we're done with this file
        }
        return session;
    }

    private static void dumpMerge( Dumper dumper, byte type, ReadableChannel channel, long range, int tokenId, TxFilter txFilter,
            long session, long flush ) throws IOException
    {
        long existingBits = channel.getLong();
        long newBits = channel.getLong();
        if ( txFilter == null || txFilter.contains() )
        {
            dumper.merge( type == TYPE_MERGE_ADD, session, flush, range, tokenId, existingBits, newBits );
        }
    }

    private static void dumpPrepare( Dumper dumper, byte type, ReadableChannel channel, long range, int tokenId, TxFilter txFilter, long session, long flush )
            throws IOException
    {
        long txId = channel.getLong();
        int offset = channel.get();
        long entityId = range * 64 + offset;
        if ( txFilter == null || txFilter.contains( txId ) )
        {
            // I.e. if the txId this update comes from is within the txFilter
            dumper.prepare( type == TYPE_PREPARE_ADD, session, flush, txId, entityId, tokenId );
        }
    }

    static TxFilter parseTxFilter( String txFilter )
    {
        if ( txFilter == null )
        {
            return null;
        }

        String[] tokens = txFilter.split( "," );
        long[][] filters = new long[tokens.length][];
        for ( int i = 0; i < tokens.length; i++ )
        {
            String token = tokens[i];
            int index = token.lastIndexOf( '-' );
            long low, high;
            if ( index == -1 )
            {
                low = high = Long.parseLong( token );
            }
            else
            {
                low = Long.parseLong( token.substring( 0, index ) );
                high = Long.parseLong( token.substring( index + 1 ) );
            }
            filters[i] = new long[]{low, high};
        }
        return new TxFilter( filters );
    }

    static class TxFilter
    {
        private final long[][] lowsAndHighs;
        private boolean contains;

        TxFilter( long[]... lowsAndHighs )
        {
            this.lowsAndHighs = lowsAndHighs;
        }

        void clear()
        {
            contains = false;
        }

        boolean contains( long txId )
        {
            for ( long[] filter : lowsAndHighs )
            {
                if ( txId >= filter[0] && txId <= filter[1] )
                {
                    contains = true;
                    return true;
                }
            }
            return false;
        }

        boolean contains()
        {
            return contains;
        }
    }

    public interface Dumper
    {
        void file( Path file );

        void prepare( boolean add, long session, long flush, long txId, long entityId, int tokenId );

        void merge( boolean add, long session, long flush, long range, int tokenId, long existingBits, long newBits );
    }

    public static class PrintStreamDumper implements Dumper
    {
        private final PrintStream out;
        private final char[] bitsAsChars = new char[64 + 7/*separators*/];

        PrintStreamDumper( PrintStream out )
        {
            this.out = out;
            Arrays.fill( bitsAsChars, ' ' );
        }

        @Override
        public void file( Path file )
        {
            out.println( "=== " + file.toAbsolutePath() + " ===" );
        }

        @Override
        public void prepare( boolean add, long session, long flush, long txId, long entityId, int tokenId )
        {
            out.println( format( "[%d,%d]%stx:%d,entity:%d,token:%d", session, flush, add ? '+' : '-', txId, entityId, tokenId ) );
        }

        @Override
        public void merge( boolean add, long session, long flush, long range, int tokenId, long existingBits, long newBits )
        {
            out.println( format( "[%d,%d]%srange:%d,tokenId:%d%n [%s]%n [%s]", session, flush, add ? '+' : '-', range, tokenId,
                    bits( existingBits, bitsAsChars ), bits( newBits, bitsAsChars ) ) );
        }

        private static String bits( long bits, char[] bitsAsChars )
        {
            long mask = 1;
            for ( int i = 0, c = 0; i < 64; i++, c++ )
            {
                if ( i % 8 == 0 )
                {
                    c++;
                }
                boolean set = (bits & mask) != 0;
                bitsAsChars[bitsAsChars.length - c] = set ? '1' : '0';
                mask <<= 1;
            }
            return String.valueOf( bitsAsChars );
        }
    }

    public interface Monitor
    {
        void rotated( Path file, long timestamp, long size );

        void pruned( Path file, long timestamp );
    }

    public static final Monitor NO_MONITOR = new Monitor()
    {
        @Override
        public void rotated( Path file, long timestamp, long size )
        {
        }

        @Override
        public void pruned( Path file, long timestamp )
        {
        }
    };
}
