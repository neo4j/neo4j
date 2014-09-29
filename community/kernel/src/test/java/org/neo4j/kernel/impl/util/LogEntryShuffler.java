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
package org.neo4j.kernel.impl.util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import javax.transaction.xa.Xid;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.nioneo.xa.Command;
import org.neo4j.kernel.impl.transaction.xaframework.DirectMappedLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommandFactory;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;

/*
 * This tool reads a log file and it randomly  shuffle the log entries inside the file, but it guarantees that the
 * order of the entries belonging to the same transaction are still in order. I.e., grepping on a txId you will get
 * log entries in the right order.  This tool is useful for testing upgrades from older Neo4j versions, indeed we have
 * to make sure we can properly migrate logs containing unordered entries.
 */
public class LogEntryShuffler
{
    public static class CommandFactory extends XaCommandFactory
    {
        @Override
        public XaCommand readCommand( ReadableByteChannel byteChannel, ByteBuffer buffer ) throws IOException
        {
            return Command.readCommand( null, byteChannel, buffer );
        }
    }

    private final FileSystemAbstraction fileSystem;

    public LogEntryShuffler( FileSystemAbstraction fileSystem )
    {
        this.fileSystem = fileSystem;
    }

    public static void main( String[] args ) throws IOException
    {
        if ( args.length != 2 )
        {
            System.out.println( "params: input-file output-file" );
            return;
        }

        final File input = new File( args[0] );
        final File output = new File( args[1] );
        new LogEntryShuffler( new DefaultFileSystemAbstraction() ).shuffle( input, output );
    }

    public void shuffle( File input, File output ) throws IOException
    {
        if ( fileSystem.fileExists( output ) )
        {
            throw new IOException( "Output file: " + output + " already exists" );
        }

        // read entries
        final Pair<long[], List<LogEntry>> pair = readHeaderAndEntries( input );
        final long[] header = pair.first();
        List<LogEntry> readEntries = pair.other();

        // group them by transaction id
        final Map<Long, List<LogEntry>> groupedEntries = groupByTxId( readEntries );

        // shuffle them
        final List<LogEntry> shuffledEntries = doShuffle( groupedEntries );

        assert readEntries.size() == shuffledEntries.size();

        // write shuffled entries in the output file
        final int writtenEntries = writeHeaderAndEntries( output, header, shuffledEntries );

        assert shuffledEntries.size() == writtenEntries;
    }

    private Pair<long[], List<LogEntry>> readHeaderAndEntries( File input ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect( 9 + Xid.MAXGTRIDSIZE + Xid.MAXBQUALSIZE * 10 );
        StoreChannel fileChannel = fileSystem.open( input, "r" );
        try
        {
            long[] header = LogIoUtils.readLogHeader( buffer, fileChannel, true );

            final List<LogEntry> entries = new ArrayList<LogEntry>();
            final CommandFactory cf = new CommandFactory();
            LogEntry entry;
            while ( (entry = LogIoUtils.readEntry( buffer, fileChannel, cf )) != null )
            {
                entries.add( entry );
            }

            return Pair.of( header, entries );
        }
        finally
        {
            fileChannel.close();
        }
    }

    private Map<Long, List<LogEntry>> groupByTxId( List<LogEntry> entries )
    {
        final Map<Integer, List<LogEntry>> idToEntry = new HashMap<Integer, List<LogEntry>>();
        final Map<Long, Integer> txIdToId = new HashMap<Long, Integer>();
        for ( LogEntry entry : entries )
        {
            int identifier = entry.getIdentifier();
            provideList( idToEntry, identifier ).add( entry );

            if ( entry instanceof LogEntry.Commit )
            {
                txIdToId.put( ((LogEntry.Commit) entry).getTxId(), identifier );
            }
        }

        final Map<Long, List<LogEntry>> txIdToEntry = new HashMap<Long, List<LogEntry>>();
        for ( Map.Entry<Long, Integer> e : txIdToId.entrySet() )
        {
            txIdToEntry.put( e.getKey(), idToEntry.get( e.getValue() ) );
        }

        return txIdToEntry;
    }

    private List<LogEntry> doShuffle( Map<Long, List<LogEntry>> txIdGroupedEntries )
    {
        final Set<Long> txIds = txIdGroupedEntries.keySet();
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for ( long i : txIds )
        {
            min = Math.min( i, min );
            max = Math.max( i, max );
        }
        long nextCommittableTx = min;

        final InRangeRandomPicker picker = new InRangeRandomPicker( min, max );
        final List<LogEntry> mixedUp = new LinkedList<LogEntry>();
        while ( !txIdGroupedEntries.isEmpty() )
        {
            long randTxId = picker.random();
            List<LogEntry> logEntries;

            while ( (logEntries = fetchNext( txIdGroupedEntries, randTxId, nextCommittableTx )) == null )
            {
                randTxId = picker.inRange( randTxId + 1 );
            }

            final LogEntry entry = logEntries.remove( 0 );
            mixedUp.add( entry );

            if ( logEntries.isEmpty() )
            {
                txIdGroupedEntries.remove( randTxId );
                // the last in the list is a commit so we can commit the next tx
                nextCommittableTx++;
            }
        }

        return mixedUp;
    }

    private List<LogEntry> fetchNext( Map<Long, List<LogEntry>> txIdGroupedEntries, long txId, long nextCommittableTx )
    {
        final List<LogEntry> logEntries = txIdGroupedEntries.get( txId );
        if ( logEntries == null )
        {
            return null;
        }

        final LogEntry logEntry = logEntries.get( 0 );
        if ( logEntry instanceof LogEntry.Commit && nextCommittableTx != ((LogEntry.Commit) logEntry).getTxId() )
        {
            return null;
        }

        return logEntries;
    }

    private int writeHeaderAndEntries( File output, long[] header, List<LogEntry> entries ) throws IOException
    {
        StoreChannel fileChannel = fileSystem.open( output, "rw" );
        try
        {
            fileChannel.write( LogIoUtils.writeLogHeader( ByteBuffer.allocateDirect( 16 ), header[0], header[1] ) );

            final LogBuffer buffer = new DirectMappedLogBuffer( fileChannel, NO_COUNT );
            int written = 0;
            for ( LogEntry entry : entries )
            {
                LogIoUtils.writeLogEntry( entry, buffer );
                written++;

            }
            buffer.force();
            return written;
        }
        finally
        {
            fileChannel.close();
        }
    }

    private List<LogEntry> provideList( Map<Integer, List<LogEntry>> idToEntries, int identifier )
    {
        List<LogEntry> map = idToEntries.get( identifier );
        if ( map == null )
        {
            map = new ArrayList<LogEntry>();
            idToEntries.put( identifier, map );
        }
        return map;
    }

    private static class InRangeRandomPicker
    {
        private final long min;
        private final long max;
        private final Random random = new Random( System.currentTimeMillis() );

        public InRangeRandomPicker( long min, long max )
        {
            this.min = min;
            this.max = max;
        }

        public long random()
        {
            return inRange( random.nextLong() );
        }

        public long inRange( long num )
        {
            if ( num >= min && num <= max )
            {
                return num;
            }
            return min + (Math.abs( num ) % (max - min));
        }
    }

    private static ByteCounterMonitor NO_COUNT = new ByteCounterMonitor()
    {
        @Override
        public void bytesWritten( long numberOfBytes )
        {

        }

        @Override
        public void bytesRead( long numberOfBytes )
        {

        }
    };
}
