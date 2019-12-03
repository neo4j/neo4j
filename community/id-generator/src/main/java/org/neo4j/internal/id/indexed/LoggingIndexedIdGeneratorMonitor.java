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
package org.neo4j.internal.id.indexed;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongPredicate;

import org.neo4j.internal.helpers.Args;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FlushableChannel;
import org.neo4j.io.fs.PhysicalFlushableChannel;
import org.neo4j.io.fs.ReadAheadChannel;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.util.FeatureToggles;

import static java.util.Comparator.comparing;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.neo4j.internal.helpers.Format.date;
import static org.neo4j.internal.id.indexed.IndexedIdGenerator.NO_MONITOR;
import static org.neo4j.io.ByteUnit.mebiBytes;

/**
 * Logs all monitor calls into a {@link FlushableChannel}.
 */
public class LoggingIndexedIdGeneratorMonitor implements IndexedIdGenerator.Monitor, Closeable
{
    private static final boolean LOG_ENABLED = FeatureToggles.flag( LoggingIndexedIdGeneratorMonitor.class, "enabled", false );
    private static final long LOG_ROTATION_SIZE_THRESHOLD =
            FeatureToggles.getLong( LoggingIndexedIdGeneratorMonitor.class, "rotationThreshold", mebiBytes( 200 ) );
    private static final long LOG_PRUNE_THRESHOLD = FeatureToggles.getLong( LoggingIndexedIdGeneratorMonitor.class, "pruneThreshold", DAYS.toMillis( 2 ) );

    private static final String ARG_TOFILE = "tofile";
    private static final String ARG_FILTER = "filter";

    private static final LongPredicate NO_FILTER = id -> true;
    private static final Type[] TYPES = Type.values();
    static final int HEADER_SIZE = Byte.BYTES + Long.BYTES;

    private final FileSystemAbstraction fs;
    private final File file;
    private final SystemNanoClock clock;
    private FlushableChannel channel;
    private AtomicLong position = new AtomicLong();
    private long rotationThreshold;
    private long pruneThreshold;

    /**
     * Looks at feature toggle and instantiates a LoggingMonitor if enabled, otherwise a no-op monitor.
     */
    public static IndexedIdGenerator.Monitor defaultIdMonitor( FileSystemAbstraction fs, File idFile )
    {
        if ( LOG_ENABLED )
        {
            return new LoggingIndexedIdGeneratorMonitor( fs, new File( idFile.getAbsolutePath() + ".log" ), Clocks.nanoClock(),
                    LOG_ROTATION_SIZE_THRESHOLD, ByteUnit.Byte, LOG_PRUNE_THRESHOLD, TimeUnit.MILLISECONDS );
        }
        return NO_MONITOR;
    }

    LoggingIndexedIdGeneratorMonitor( FileSystemAbstraction fs, File file, SystemNanoClock clock,
            long rotationThreshold, ByteUnit rotationThresholdUnit,
            long pruneThreshold, TimeUnit pruneThresholdUnit )
    {
        this.fs = fs;
        this.file = file;
        this.clock = clock;
        this.rotationThreshold = rotationThresholdUnit.toBytes( rotationThreshold );
        this.pruneThreshold = pruneThresholdUnit.toMillis( pruneThreshold );
        try
        {
            if ( fs.fileExists( file ) )
            {
                moveAwayFile();
            }
            this.channel = instantiateChannel();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public synchronized void opened( long highestWrittenId, long highId )
    {
        putTypeAndTwoIds( Type.OPENED, highestWrittenId, highId );
    }

    @Override
    public synchronized void allocatedFromHigh( long allocatedId )
    {
        putTypeAndId( Type.ALLOCATE_HIGH, allocatedId );
    }

    @Override
    public synchronized void allocatedFromReused( long allocatedId )
    {
        putTypeAndId( Type.ALLOCATE_REUSED, allocatedId );
    }

    @Override
    public synchronized void cached( long cachedId )
    {
        putTypeAndId( Type.CACHED, cachedId );
    }

    @Override
    public synchronized void markedAsUsed( long markedId )
    {
        putTypeAndId( Type.MARK_USED, markedId );
    }

    @Override
    public synchronized void markedAsDeleted( long markedId )
    {
        putTypeAndId( Type.MARK_DELETED, markedId );
    }

    @Override
    public synchronized void markedAsFree( long markedId )
    {
        putTypeAndId( Type.MARK_FREE, markedId );
    }

    @Override
    public synchronized void markedAsReserved( long markedId )
    {
        putTypeAndId( Type.MARK_RESERVED, markedId );
    }

    @Override
    public synchronized void markedAsUnreserved( long markedId )
    {
        putTypeAndId( Type.MARK_UNRESERVED, markedId );
    }

    @Override
    public synchronized void markedAsDeletedAndFree( long markedId )
    {
        putTypeAndId( Type.MARK_DELETED_AND_FREE, markedId );
    }

    @Override
    public synchronized void markSessionDone()
    {
        flushBuffer();
        checkRotateAndPrune();
    }

    @Override
    public synchronized void normalized( long idRange )
    {
        putTypeAndId( Type.NORMALIZED, idRange );
    }

    @Override
    public synchronized void bridged( long bridgedId )
    {
        putTypeAndId( Type.BRIDGED, bridgedId );
    }

    @Override
    public synchronized void checkpoint( long highestWrittenId, long highId )
    {
        putTypeAndTwoIds( Type.CHECKPOINT, highestWrittenId, highId );

        // Take the opportunity to also flush this log
        flushBuffer();
    }

    @Override
    public synchronized void clearingCache()
    {
        putTypeOnly( Type.CLEARING_CACHE );
    }

    @Override
    public synchronized void clearedCache()
    {
        putTypeOnly( Type.CLEARED_CACHE );
    }

    @Override
    public synchronized void close()
    {
        putTypeOnly( Type.CLOSED );
        IOUtils.closeAllUnchecked( channel );
    }

    private void flushBuffer()
    {
        try
        {
            channel.prepareForFlush().flush();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void putEntryHeader( Type type ) throws IOException
    {
        channel.put( type.id );
        channel.putLong( clock.millis() );
    }

    private void checkRotateAndPrune()
    {
        if ( position.longValue() >= rotationThreshold )
        {
            // Rotate
            try
            {
                flushBuffer();
                channel.close();
                moveAwayFile();
                position.set( 0 );
                channel = instantiateChannel();
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }

            // Prune
            long time = clock.millis();
            long threshold = time - pruneThreshold;
            for ( File file : fs.listFiles( file.getParentFile(), ( dir, name ) -> name.startsWith( file.getName() + "-" ) ) )
            {
                if ( millisOf( file ) < threshold )
                {
                    fs.deleteFile( file );
                }
            }
        }
    }

    private void putTypeOnly( Type type )
    {
        try
        {
            putEntryHeader( type );
            position.addAndGet( HEADER_SIZE );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void putTypeAndId( Type type, long id )
    {
        try
        {
            putEntryHeader( type );
            channel.putLong( id );
            position.addAndGet( HEADER_SIZE + Long.BYTES );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void putTypeAndTwoIds( Type type, long id1, long id2 )
    {
        try
        {
            putEntryHeader( type );
            channel.putLong( id1 );
            channel.putLong( id2 );
            position.addAndGet( HEADER_SIZE + Long.BYTES * 2 );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void moveAwayFile() throws IOException
    {
        File to;
        do
        {

            to = timestampedFile();
        }
        while ( fs.fileExists( to ) );
        fs.renameFile( file, to );
    }

    private PhysicalFlushableChannel instantiateChannel() throws IOException
    {
        return new PhysicalFlushableChannel( fs.write( file ) );
    }

    private File timestampedFile()
    {
        return new File( file.getParentFile(), file.getName() + "-" + clock.millis() );
    }

    static long millisOf( File file )
    {
        String name = file.getName();
        int dashIndex = name.lastIndexOf( '-' );
        if ( dashIndex == -1 )
        {
            return Long.MAX_VALUE;
        }
        return Long.parseLong( name.substring( dashIndex + 1 ) );
    }

    /**
     * Used for dumping contents of a log as text
     */
    public static void main( String[] args ) throws IOException
    {
        Args arguments = Args.withFlags( ARG_TOFILE ).parse( args );
        if ( arguments.orphans().size() == 0 )
        {
            System.err.println( "Please supply base name of log file" );
            return;
        }

        File file = new File( arguments.orphans().get( 0 ) );
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        String filterArg = arguments.get( ARG_FILTER, null );
        LongPredicate filter = filterArg != null ? parseFilter( filterArg ) : NO_FILTER;
        PrintStream out = System.out;
        boolean redirectsToFile = arguments.getBoolean( ARG_TOFILE );
        if ( redirectsToFile )
        {
            File outFile = new File( file.getAbsolutePath() + ".txt" );
            System.out.println( "Redirecting output to " + outFile );
            out = new PrintStream( new BufferedOutputStream( new FileOutputStream( outFile ) ) );
        }
        dump( fs, file, new Printer( out, filter ) );
        if ( redirectsToFile )
        {
            out.close();
        }
    }

    static void dump( FileSystemAbstraction fs, File baseFile, Dumper dumper ) throws IOException
    {
        File[] files = fs.listFiles( baseFile.getParentFile(),
                ( dir, name ) -> name.startsWith( baseFile.getName() ) && !name.endsWith( ".txt" ) );
        Arrays.sort( files, comparing( LoggingIndexedIdGeneratorMonitor::millisOf ) );
        for ( File file : files )
        {
            dumpFile( fs, file, dumper );
        }
    }

    private static void dumpFile( FileSystemAbstraction fs, File file, Dumper dumper ) throws IOException
    {
        dumper.file( file );
        try ( ReadableChannel channel = new ReadAheadChannel<>( fs.read( file ) ) )
        {
            while ( true )
            {
                byte typeByte = channel.get();
                if ( typeByte < 0 || typeByte >= TYPES.length )
                {
                    System.out.println( "Unknown type " + typeByte + " at " + ((ReadAheadChannel) channel).position() );
                    continue;
                }

                Type type = TYPES[typeByte];
                long time = channel.getLong();
                switch ( type )
                {
                case CLEARING_CACHE:
                case CLEARED_CACHE:
                case CLOSED:
                    dumper.type( type, time );
                    break;
                case ALLOCATE_HIGH:
                case ALLOCATE_REUSED:
                case CACHED:
                case MARK_USED:
                case MARK_DELETED:
                case MARK_FREE:
                case MARK_RESERVED:
                case MARK_UNRESERVED:
                case MARK_DELETED_AND_FREE:
                case NORMALIZED:
                case BRIDGED:
                    dumper.typeAndId( type, time, channel.getLong() );
                    break;
                case OPENED:
                case CHECKPOINT:
                    dumper.typeAndTwoIds( type, time, channel.getLong(), channel.getLong() );
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
    }

    private static Filter parseFilter( String arg )
    {
        String[] ids = arg.split( "," );
        long[] result = new long[ids.length];
        for ( int i = 0; i < ids.length; i++ )
        {
            result[i] = Long.parseLong( ids[i] );
        }
        return new Filter( result );
    }

    private static class Filter implements LongPredicate
    {
        private final long[] ids;

        Filter( long... ids )
        {
            this.ids = ids;
        }

        @Override
        public boolean test( long value )
        {
            for ( long id : ids )
            {
                if ( id == value )
                {
                    return true;
                }
            }
            return false;
        }
    }

    interface Dumper
    {
        void file( File file );

        void type( Type type, long time );

        void typeAndId( Type type, long time, long id );

        void typeAndTwoIds( Type type, long time, long id1, long id2 );
    }

    private static class Printer implements Dumper
    {
        private final PrintStream out;
        private final LongPredicate filter;

        Printer( PrintStream out, LongPredicate filter )
        {
            this.out = out;
            this.filter = filter;
        }

        @Override
        public void file( File file )
        {
            out.printf( "=== %s ===%n", file.getAbsolutePath() );
        }

        @Override
        public void type( Type type, long time )
        {
            out.printf( "%s %s%n", date( time ), type.shortName );
        }

        @Override
        public void typeAndId( Type type, long time, long id )
        {
            if ( filter.test( id ) )
            {
                out.printf( "%s %s [%d]%n", date( time ), type.shortName, id );
            }
        }

        @Override
        public void typeAndTwoIds( Type type, long time, long id1, long id2 )
        {
            out.printf( "%s %s %d/%d%n", date( time ), type.shortName, id1, id2 );
        }
    }

    enum Type
    {
        OPENED( "Opened" ),
        CLOSED( "Closed" ),
        ALLOCATE_HIGH( "AH" ),
        ALLOCATE_REUSED( "AR" ),
        CACHED( "CA" ),
        MARK_USED( "MI" ),
        MARK_DELETED( "MD" ),
        MARK_FREE( "MF" ),
        MARK_RESERVED( "MR" ),
        MARK_UNRESERVED( "MX" ),
        MARK_DELETED_AND_FREE( "MA" ),
        NORMALIZED( "NO" ),
        BRIDGED( "BR" ),
        CHECKPOINT( "Checkpoint" ),
        CLEARING_CACHE( "ClearCacheStart" ),
        CLEARED_CACHE( "ClearCacheEnd" );

        byte id;
        String shortName;

        Type( String shortName )
        {
            this.id = (byte) ordinal();
            this.shortName = shortName;
        }
    }
}
