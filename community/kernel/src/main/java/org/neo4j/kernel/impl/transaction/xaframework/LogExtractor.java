/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import static java.lang.Math.max;
import static org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource.LOGICAL_LOG_DEFAULT_NAME;
import static org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog.getHighestHistoryLogVersion;
import static org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog.readAndAssertLogHeader;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.Xid;

import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.CommonFactories;
import org.neo4j.kernel.impl.cache.LruCache;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.xa.Command;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry.Start;
import org.neo4j.kernel.impl.util.BufferedFileChannel;

public class LogExtractor
{
    private static final String[] ACTIVE_POSTFIXES = { ".1", ".2" };
    
    /**
     * If tx range is smaller than this threshold ask the position cache for the
     * start position farthest back. Otherwise jump to right log and scan.
     */
    private static final int CACHE_FIND_THRESHOLD = 10000;

    private final ByteBuffer localBuffer = newLogReaderBuffer();
    private ReadableByteChannel source;
    private final LogEntryCollector collector;
    private long version;
    private LogEntry.Commit lastCommitEntry;
    private LogEntry.Commit previousCommitEntry;
    private final long startTxId;
    private long nextExpectedTxId;
    private int counter;

    private final LogPositionCache cache;
    private final LogLoader logLoader;
    private final XaCommandFactory commandFactory;
    
    public static class LogPositionCache
    {
        private final LruCache<Long, TxPosition> txStartPositionCache =
                new LruCache<Long, TxPosition>( "Tx start position cache", 10000 );
        private final LruCache<Long /*log version*/, Long /*last committed tx*/> logHeaderCache =
                new LruCache<Long, Long>( "Log header cache", 1000 );
        
        public void clear()
        {
            logHeaderCache.clear();
            txStartPositionCache.clear();
        }
        
        public TxPosition positionOf( long txId )
        {
            return txStartPositionCache.get( txId );
        }
        
        public void putHeader( long logVersion, long previousLogLastCommittedTx )
        {
            logHeaderCache.put( logVersion, previousLogLastCommittedTx );
        }
        
        public Long getHeader( long logVersion )
        {
            return logHeaderCache.get( logVersion );
        }
        
        public void putStartPosition( long txId, TxPosition position )
        {
            txStartPositionCache.put( txId, position );
        }
        
        public TxPosition getStartPosition( long txId )
        {
            return txStartPositionCache.get( txId );
        }
    }
    
    public static interface LogLoader
    {
        ReadableByteChannel getLogicalLogOrMyselfCommitted( long version, long position ) throws IOException;
        
        long getHighestLogVersion();
    }
    
    static ByteBuffer newLogReaderBuffer()
    {
        return ByteBuffer.allocate( 9 + Xid.MAXGTRIDSIZE + Xid.MAXBQUALSIZE * 10 );
    }

    public LogExtractor( LogPositionCache cache, LogLoader logLoader,
            XaCommandFactory commandFactory, long startTxId, long endTxIdHint ) throws IOException
    {
        this.cache = cache;
        this.logLoader = logLoader;
        this.commandFactory = commandFactory;
        this.startTxId = startTxId;
        this.nextExpectedTxId = startTxId;
        long diff = endTxIdHint-startTxId + 1/*since they are inclusive*/;
        if ( diff < CACHE_FIND_THRESHOLD )
        {   // Find it from cache, we must check with all the requested transactions
            // because the first committed transaction doesn't necessarily have its
            // start record before the others.
            TxPosition earliestPosition = getEarliestStartPosition( startTxId, endTxIdHint );
            if ( earliestPosition != null )
            {
                this.version = earliestPosition.version;
                this.source = logLoader.getLogicalLogOrMyselfCommitted( version, earliestPosition.position );
            }
        }

        if ( source == null )
        {   // Find the start position by jumping to the right log and scan linearly.
            // for consecutive transaction there's no scan needed, only the first one.
            this.version = findLogContainingTxId( startTxId )[0];
            this.source = logLoader.getLogicalLogOrMyselfCommitted( version, 0 );
            // To get to the right position to start reading entries from
            readAndAssertLogHeader( localBuffer, source, version );
        }
        this.collector = new KnownTxIdCollector( startTxId );
    }

    private TxPosition getEarliestStartPosition( long startTxId, long endTxIdHint )
    {
        TxPosition earliest = null;
        for ( long txId = startTxId; txId <= endTxIdHint; txId++ )
        {
            TxPosition position = cache.positionOf( txId );
            if ( position == null ) return null;
            if ( earliest == null || position.earlierThan( earliest ) )
            {
                earliest = position;
            }
        }
        return earliest;
    }

    /**
     * @return the txId for the extracted tx. Or -1 if end-of-stream was reached.
     * @throws RuntimeException if there was something unexpected with the stream.
     */
    public long extractNext( LogBuffer target ) throws IOException
    {
        try
        {
            while ( this.version <= logLoader.getHighestLogVersion() )
            {
                long result = collectNextFromCurrentSource( target );
                if ( result != -1 )
                {
                    // TODO Should be assertions?
                    if ( previousCommitEntry != null && result == previousCommitEntry.getTxId() ) continue;
                    if ( result != nextExpectedTxId )
                    {
                        throw new RuntimeException( "Expected txId " + nextExpectedTxId + ", but got " + result + " (starting from " + startTxId + ")" + " " + counter + ", " + previousCommitEntry + ", " + lastCommitEntry );
                    }
                    nextExpectedTxId++;
                    counter++;
                    return result;
                }

                if ( this.version < logLoader.getHighestLogVersion() )
                {
                    continueInNextLog();
                }
                else break;
            }
            return -1;
        }
        catch ( Exception e )
        {
            // Something is wrong with the cached tx start position for this (expected) tx,
            // remove it from cache so that next request will have to bypass the cache
            cache.clear();
            if ( e instanceof IOException ) throw (IOException) e;
            else throw Exceptions.launderedException( e );
        }
    }

    private void continueInNextLog() throws IOException
    {
        ensureSourceIsClosed();
        this.source = logLoader.getLogicalLogOrMyselfCommitted( ++version, 0 );
        readAndAssertLogHeader( localBuffer, source, version ); // To get to the right position to start reading entries from
    }

    private long collectNextFromCurrentSource( LogBuffer target ) throws IOException
    {
        LogEntry entry = null;
        while ( collector.hasInFutureQueue() || // if something in queue then don't read next entry
                (entry = LogIoUtils.readEntry( localBuffer, source, commandFactory )) != null )
        {
            LogEntry foundEntry = collector.collect( entry, target );
            if ( foundEntry != null )
            {   // It just wrote the transaction, w/o the done record though. Add it
                previousCommitEntry = lastCommitEntry;
                LogIoUtils.writeLogEntry( new LogEntry.Done( collector.getIdentifier() ), target );
                lastCommitEntry = (LogEntry.Commit)foundEntry;
                return lastCommitEntry.getTxId();
            }
        }
        return -1;
    }

    public void close()
    {
        ensureSourceIsClosed();
    }

    @Override
    protected void finalize() throws Throwable
    {
        ensureSourceIsClosed();
    }

    private void ensureSourceIsClosed()
    {
        try
        {
            if ( source != null )
            {
                source.close();
                source = null;
            }
        }
        catch ( IOException e )
        { // OK?
            System.out.println( "Couldn't close logical after extracting transactions from it" );
            e.printStackTrace();
        }
    }

    public LogEntry.Commit getLastCommitEntry()
    {
        return lastCommitEntry;
    }
    
    public long getLastTxChecksum()
    {
        return getLastStartEntry().getChecksum();
    }

    public Start getLastStartEntry()
    {
        return collector.getLastStartEntry();
    }

    private long[] findLogContainingTxId( long txId ) throws IOException
    {
        long version = logLoader.getHighestLogVersion();
        long committedTx = 1;
        while ( version >= 0 )
        {
            Long cachedLastTx = cache.getHeader( version );
            if ( cachedLastTx != null )
            {
                committedTx = cachedLastTx.longValue();
            }
            else
            {
                ReadableByteChannel logChannel = logLoader.getLogicalLogOrMyselfCommitted( version, 0 );
                try
                {
                    ByteBuffer buf = ByteBuffer.allocate( 16 );
                    long[] header = readAndAssertLogHeader( buf, logChannel, version );
                    committedTx = header[1];
                    cache.putHeader( version, committedTx );
                }
                finally
                {
                    logChannel.close();
                }
            }
            if ( committedTx < txId )
            {
                break;
            }
            version--;
        }
        if ( version == -1 )
        {
            throw new RuntimeException( "txId:" + txId + " not found in any logical log "
                                        + "(starting at " + logLoader.getHighestLogVersion()
                                        + " and searching backwards" );
        }
        return new long[] { version, committedTx };
    }
    
    private static interface LogEntryCollector
    {
        LogEntry collect( LogEntry entry, LogBuffer target ) throws IOException;

        LogEntry.Start getLastStartEntry();

        boolean hasInFutureQueue();

        int getIdentifier();
    }

    private static class KnownIdentifierCollector implements LogEntryCollector
    {
        private final int identifier;
        private LogEntry.Start startEntry;

        KnownIdentifierCollector( int identifier )
        {
            this.identifier = identifier;
        }

        public int getIdentifier()
        {
            return identifier;
        }

        public LogEntry collect( LogEntry entry, LogBuffer target ) throws IOException
        {
            if ( entry.getIdentifier() == identifier )
            {
                if ( entry instanceof LogEntry.Start )
                {
                    startEntry = (Start) entry;
                }
                if ( target != null )
                {
                    LogIoUtils.writeLogEntry( entry, target );
                }
                return entry;
            }
            return null;
        }

        @Override
        public boolean hasInFutureQueue()
        {
            return false;
        }

        @Override
        public LogEntry.Start getLastStartEntry()
        {
            return startEntry;
        }
    }

    private static class KnownTxIdCollector implements LogEntryCollector
    {
        private final Map<Integer,List<LogEntry>> transactions = new HashMap<Integer,List<LogEntry>>();
        private final long startTxId;
        private int identifier;
        private final Map<Long, List<LogEntry>> futureQueue = new HashMap<Long, List<LogEntry>>();
        private long nextExpectedTxId;
        private LogEntry.Start lastStartEntry;

        KnownTxIdCollector( long startTxId )
        {
            this.startTxId = startTxId;
            this.nextExpectedTxId = startTxId;
        }

        public int getIdentifier()
        {
            return identifier;
        }

        @Override
        public boolean hasInFutureQueue()
        {
            return futureQueue.containsKey( nextExpectedTxId );
        }

        @Override
        public LogEntry.Start getLastStartEntry()
        {
            return lastStartEntry;
        }

        public LogEntry collect( LogEntry entry, LogBuffer target ) throws IOException
        {
            if ( futureQueue.containsKey( nextExpectedTxId ) )
            {
                List<LogEntry> list = futureQueue.remove( nextExpectedTxId++ );
                lastStartEntry = (LogEntry.Start)list.get( 0 );
                writeToBuffer( list, target );
                return commitEntryOf( list );
            }

            if ( entry instanceof LogEntry.Start )
            {
                List<LogEntry> list = new LinkedList<LogEntry>();
                list.add( entry );
                transactions.put( entry.getIdentifier(), list );
            }
            else if ( entry instanceof LogEntry.Commit )
            {
                long commitTxId = ((LogEntry.Commit) entry).getTxId();
                if ( commitTxId < startTxId ) return null;
                identifier = entry.getIdentifier();
                List<LogEntry> entries = transactions.get( identifier );
                if ( entries == null ) return null;
                entries.add( entry );
                if ( nextExpectedTxId != startTxId )
                {   // Have returned some previous tx
                    // If we encounter an already extracted tx in the middle of the stream
                    // then just ignore it. This can happen when we do log rotation,
                    // where records are copied over from the active log to the new.
                    if ( commitTxId < nextExpectedTxId ) return null;
                }

                if ( commitTxId != nextExpectedTxId )
                {   // There seems to be a hole in the tx stream, or out-of-ordering
                    futureQueue.put( commitTxId, entries );
                    return null;
                }

                writeToBuffer( entries, target );
                nextExpectedTxId = commitTxId+1;
                lastStartEntry = (LogEntry.Start)entries.get( 0 );
                return entry;
            }
            else if ( entry instanceof LogEntry.Command || entry instanceof LogEntry.Prepare )
            {
                List<LogEntry> list = transactions.get( entry.getIdentifier() );

                // Since we can start reading at any position in the log it might be the case
                // that we come across a record which corresponding start record resides
                // before the position we started reading from. If that should be the case
                // then skip it since it isn't an important record for us here.
                if ( list != null )
                {
                    list.add( entry );
                }
            }
            else if ( entry instanceof LogEntry.Done )
            {
                transactions.remove( entry.getIdentifier() );
            }
            else
            {
                throw new RuntimeException( "Unknown entry: " + entry );
            }
            return null;
        }

        private LogEntry commitEntryOf( List<LogEntry> list )
        {
            for ( LogEntry entry : list )
            {
                if ( entry instanceof LogEntry.Commit ) return entry;
            }
            throw new RuntimeException( "No commit entry in " + list );
        }

        private void writeToBuffer( List<LogEntry> entries, LogBuffer target ) throws IOException
        {
            if ( target != null )
            {
                for ( LogEntry entry : entries )
                {
                    LogIoUtils.writeLogEntry( entry, target );
                }
            }
        }
    }

    public static class TxPosition
    {
        final long version;
        final int masterId;
        final int identifier;
        final long position;
        final long checksum;

        public TxPosition( long version, int masterId, int identifier, long position, long checksum )
        {
            this.version = version;
            this.masterId = masterId;
            this.identifier = identifier;
            this.position = position;
            this.checksum = checksum;
        }

        public boolean earlierThan( TxPosition other )
        {
            if ( version < other.version ) return true;
            if ( version > other.version ) return false;
            return position < other.position;
        }

        @Override
        public String toString()
        {
            return "TxPosition[version:" + version + ", pos:" + position + "]";
        }
    }
    
    public static LogExtractor from( final String storeDir ) throws IOException
    {
        // 2 is a "magic" first tx :)
        return from( storeDir, 2 );
    }
    
    public static LogExtractor from( final String storeDir, long startTxId ) throws IOException
    {
        LogLoader loader = new LogLoader()
        {
            private final FileSystemAbstraction fileSystem = CommonFactories.defaultFileSystemAbstraction();
            private final Map<Long, String> activeLogFiles = getActiveLogs( storeDir );
            private final long highestLogVersion = max( getHighestHistoryLogVersion( new File( storeDir ),
                    LOGICAL_LOG_DEFAULT_NAME ), maxKey( activeLogFiles ) );
            
            @Override
            public ReadableByteChannel getLogicalLogOrMyselfCommitted( long version, long position )
                    throws IOException
            {
                String name = new File( storeDir, LOGICAL_LOG_DEFAULT_NAME + ".v" + version ).getAbsolutePath();
                if ( !fileSystem.fileExists( name ) )
                {
                    name = activeLogFiles.get( version );
                    if ( name == null ) throw new NoSuchLogVersionException( version );
                }
                FileChannel channel = fileSystem.open( name, "r" );
                channel.position( position );
                return new BufferedFileChannel( channel );
            }
            
            private long maxKey( Map<Long, String> activeLogFiles )
            {
                long max = 0;
                for ( Long key : activeLogFiles.keySet() ) max = max( max, key );
                return max;
            }

            private Map<Long, String> getActiveLogs( String storeDir ) throws IOException
            {
                Map<Long, String> result = new HashMap<Long, String>();
                for ( String postfix : ACTIVE_POSTFIXES )
                {
                    File candidateFile = new File( storeDir, LOGICAL_LOG_DEFAULT_NAME + postfix );
                    if ( !candidateFile.exists() ) continue;
                    long[] header = LogIoUtils.readLogHeader( fileSystem, candidateFile );
                    result.put( header[0], candidateFile.getAbsolutePath() );
                }
                return result;
            }

            @Override
            public long getHighestLogVersion()
            {
                return highestLogVersion;
            }
        };
        
        XaCommandFactory commandFactory = new XaCommandFactory()
        {
            @Override
            public XaCommand readCommand( ReadableByteChannel byteChannel,
                    ByteBuffer buffer ) throws IOException
            {
                return Command.readCommand( null, byteChannel, buffer );
            }
        };
        
        return new LogExtractor( new LogPositionCache(), loader, commandFactory, startTxId, Long.MAX_VALUE );
    }
}
