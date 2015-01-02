/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.transaction.XidImpl;
import org.neo4j.kernel.impl.transaction.xaframework.LogExtractor.LogLoader;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.neo4j.kernel.impl.transaction.XidImpl.DEFAULT_SEED;
import static org.neo4j.kernel.impl.transaction.XidImpl.getNewGlobalId;

public class TestLogPruneStrategy
{
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private EphemeralFileSystemAbstraction FS;
    private File directory;
    
    @Before
    public void before()
    {
        FS = fs.get();
        directory = TargetDirectory.forTest( FS, getClass() ).cleanDirectory( "prune" );
    }
    
    @Test
    public void noPruning() throws Exception
    {
        MockedLogLoader log = new MockedLogLoader( LogPruneStrategies.NO_PRUNING );
        for ( int i = 0; i < 100; i++ )
        {
            log.addTransactionsUntilRotationHappens();
            assertLogsRangeExists( log, 0, log.getHighestLogVersion()-1 );
        }
        assertEquals( 100, log.getHighestLogVersion() );
    }
    
    @Test
    public void pruneByFileCountWhereAllContainsFilesTransactions() throws Exception
    {
        int fileCount = 5;
        MockedLogLoader log = new MockedLogLoader( LogPruneStrategies.nonEmptyFileCount( FS, fileCount ) );
        for ( int i = 0; i < 100; i++ )
        {
            log.addTransactionsUntilRotationHappens();
            long from = Math.max( 0, log.getHighestLogVersion()-fileCount );
            assertLogsRangeExists( log, from, log.getHighestLogVersion()-1 );
        }
    }
    
    @Test
    public void pruneByFileCountWhereSomeAreEmpty() throws Exception
    {
        MockedLogLoader log = new MockedLogLoader( LogPruneStrategies.nonEmptyFileCount( FS, 3 ) );
        
        // v0 with transactions in it
        log.addTransactionsUntilRotationHappens();
        assertLogsRangeExists( log, 0, 0 );
        
        // v1 empty
        log.rotate();
        assertLogsRangeExists( log, 0, 1, empty( 1 ) );
        
        // v2 empty
        log.rotate();
        assertLogsRangeExists( log, 0, 2, empty( 1, 2 ) );
        
        // v3 with transactions in it
        log.addTransactionsUntilRotationHappens();
        assertLogsRangeExists( log, 0, 3, empty( 1, 2 ) );

        // v4 with transactions in it
        log.addTransactionsUntilRotationHappens();
        assertLogsRangeExists( log, 0, 4, empty( 1, 2 ) );

        // v5 empty
        log.rotate();
        assertLogsRangeExists( log, 0, 5, empty( 1, 2, 5 ) );

        // v6 with transactions in it
        log.addTransactionsUntilRotationHappens();
        assertLogsRangeExists( log, 3, 6, empty( 5 ) );
    }
    
    @Test
    public void pruneByFileSize() throws Exception
    {
        int maxSize = MAX_LOG_SIZE*5;
        MockedLogLoader log = new MockedLogLoader( LogPruneStrategies.totalFileSize( FS, maxSize ) );
        
        for ( int i = 0; i < 100; i++ )
        {
            log.addTransactionsUntilRotationHappens();
            assertTrue( log.getTotalSizeOfAllExistingLogFiles() < (maxSize+MAX_LOG_SIZE) );
        }
    }
    
    @Test
    public void pruneByTransactionCount() throws Exception
    {
        MockedLogLoader log = new MockedLogLoader( 10000, LogPruneStrategies.transactionCount( FS, 1000 ) );
        
        for ( int i = 1; i < 100; i++ )
        {
            log.addTransactionsUntilRotationHappens();
            int avg = (int)(log.getLastCommittedTxId()/i);
            assertTrue( log.getTotalTransactionCountOfAllExistingLogFiles() < avg*(i+1 /*+1 here is because the whole log which a transaction spills over in is kept also*/) );
        }
    }
    
    @Test
    public void pruneByTransactionTimeSpan() throws Exception
    {
        /*
         * T:    ----------------------------------------->
         * 0     =======
         * 1            ========
         * 2                    ============
         * A                                =======
         * KEEP            <---------------------->
         * 
         * Prune 0 in the example above
         */
        
        int seconds = 1;
        int millisToKeep = (int) (SECONDS.toMillis( seconds ) / 10);
        MockedLogLoader log = new MockedLogLoader( LogPruneStrategies.transactionTimeSpan( FS, millisToKeep, MILLISECONDS ) );
        
        long end = System.currentTimeMillis() + SECONDS.toMillis( seconds );
        long lastTimestamp = System.currentTimeMillis();
        while ( true )
        {
            lastTimestamp = System.currentTimeMillis();
            if ( log.addTransaction( 15, lastTimestamp ) )
            {
                assertLogRangeByTimestampExists( log, millisToKeep, lastTimestamp );
                if ( System.currentTimeMillis() > end )
                {
                    break;
                }
            }
        }
    }
    
    @Test
    public void makeSureOneLogStaysEvenWhenZeroFilesIsConfigured() throws Exception
    {
        makeSureOneLogStaysEvenWhenZeroConfigured( new MockedLogLoader( LogPruneStrategies.nonEmptyFileCount( FS, 0 ) ) );
    }

    @Test
    public void makeSureOneLogStaysEvenWhenZeroSpaceIsConfigured() throws Exception
    {
        makeSureOneLogStaysEvenWhenZeroConfigured( new MockedLogLoader( LogPruneStrategies.totalFileSize( FS, 0 ) ) );
    }
    
    @Test
    public void makeSureOneLogStaysEvenWhenZeroTransactionsIsConfigured() throws Exception
    {
        makeSureOneLogStaysEvenWhenZeroConfigured( new MockedLogLoader( LogPruneStrategies.transactionCount( FS, 0 ) ) );
    }

    @Test
    public void makeSureOneLogStaysEvenWhenZeroTimeIsConfigured() throws Exception
    {
        makeSureOneLogStaysEvenWhenZeroConfigured( new MockedLogLoader( LogPruneStrategies.transactionTimeSpan( FS, 0, SECONDS ) ) );
    }
    
    private void makeSureOneLogStaysEvenWhenZeroConfigured( MockedLogLoader mockedLogLoader ) throws Exception
    {
        MockedLogLoader log = new MockedLogLoader( LogPruneStrategies.nonEmptyFileCount( FS, 0 ) );
        log.rotate();
        assertLogsRangeExists( log, 0, 0, empty( 0 ) );
        log.rotate();
        assertLogsRangeExists( log, 0, 1, empty( 0, 1 ) );
        log.addTransactionsUntilRotationHappens();
        assertLogsRangeExists( log, 2, 2, empty() );
        log.addTransactionsUntilRotationHappens();
        assertLogsRangeExists( log, 3, 3, empty() );
    }
    
    private void assertLogRangeByTimestampExists( MockedLogLoader log, int millisToKeep,
            long lastTimestamp )
    {
        long lowerLimit = lastTimestamp - millisToKeep;
        for ( long version = log.getHighestLogVersion() - 1; version >= 0; version-- )
        {
            Long firstTimestamp = log.getFirstStartRecordTimestamp( version+1 );
            if ( firstTimestamp == null )
            {
                break;
            }
            assertTrue( "Log " + version + " should've been deleted by now. first of " + (version+1) + ":" + firstTimestamp + ", highestVersion:" +
                    log.getHighestLogVersion() + ", lowerLimit:" + lowerLimit + ", timestamp:" + lastTimestamp,
                    firstTimestamp >= lowerLimit );
        }
    }
    
    private void assertLogsRangeExists( MockedLogLoader log, long from, long to )
    {
        assertLogsRangeExists( log, from, to, empty() );
    }
    
    private void assertLogsRangeExists( MockedLogLoader log, long from, long to, Set<Long> empty )
    {
        assertTrue( log.getHighestLogVersion() >= to );
        for ( long i = 0; i < from; i++ )
        {
            assertFalse( "Log v" + i + " shouldn't exist when highest version is " + log.getHighestLogVersion() +
                    " and prune strategy " + log.pruning, FS.fileExists( log.getFileName( i ) ) );
        }
        
        for ( long i = from; i <= to; i++ )
        {
            File file = log.getFileName( i );
            assertTrue( "Log v" + i + " should exist when highest version is " + log.getHighestLogVersion() +
                    " and prune strategy " + log.pruning, FS.fileExists( file ) );
            if ( empty.contains( i ) )
            {
                assertEquals( "Log v" + i + " should be empty", LogIoUtils.LOG_HEADER_SIZE, FS.getFileSize( file ) );
                empty.remove( i );
            }
            else
            {
                assertTrue( "Log v" + i + " should be at least size " + log.getLogSize(),
                        FS.getFileSize( file ) >= log.getLogSize() );
            }
        }
        assertTrue( "Expected to find empty logs: " + empty, empty.isEmpty() );
    }

    private Set<Long> empty( long... items )
    {
        Set<Long> result = new HashSet<Long>();
        for ( long item : items )
        {
            result.add( item );
        }
        return result;
    }

    private static final int MAX_LOG_SIZE = 1000;
    private static final byte[] RESOURCE_XID = new byte[] { 5,6,7,8,9 };
    
    /**
     * A subset of what XaLogicaLog is. It's a LogLoader, add transactions to it's active log file,
     * and also rotate when max file size is reached. It uses the real xa command
     * serialization/deserialization so it's only the {@link LogLoader} aspect that is mocked.
     */
    private class MockedLogLoader implements LogLoader
    {
        private long version;
        private long tx;
        private final File baseFile;
        private final ByteBuffer activeBuffer;
        private final int identifier = 1;
        private final LogPruneStrategy pruning;
        private final Map<Long, Long> lastCommittedTxs = new HashMap<Long, Long>();
        private final Map<Long, Long> timestamps = new HashMap<Long, Long>();
        private final int logSize;
        
        MockedLogLoader( LogPruneStrategy pruning )
        {
            this( MAX_LOG_SIZE, pruning );
        }
        
        MockedLogLoader( int logSize, LogPruneStrategy pruning )
        {
            this.logSize = logSize;
            this.pruning = pruning;
            activeBuffer = ByteBuffer.allocate( logSize*10 );
            baseFile = new File( directory, "log" );
            clearAndWriteHeader();
        }
        
        public int getLogSize()
        {
            return logSize;
        }

        private void clearAndWriteHeader()
        {
            activeBuffer.clear();
            LogIoUtils.writeLogHeader( activeBuffer, version, tx );
            
            // Because writeLogHeader does flip()
            activeBuffer.limit( activeBuffer.capacity() );
            activeBuffer.position( LogIoUtils.LOG_HEADER_SIZE );
        }
        
        @Override
        public ReadableByteChannel getLogicalLogOrMyselfCommitted( long version, long position )
                throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getHighestLogVersion()
        {
            return version;
        }

        @Override
        public File getFileName( long version )
        {
            File file = new File( baseFile + ".v" + version );
            return file;
        }
        
        /**
         * @param date start record date.
         * @return whether or not this caused a rotation to happen.
         * @throws IOException 
         */
        public boolean addTransaction( int commandSize, long date ) throws IOException
        {
            InMemoryLogBuffer tempLogBuffer = new InMemoryLogBuffer();
            XidImpl xid = new XidImpl( getNewGlobalId( DEFAULT_SEED, 0 ), RESOURCE_XID );
            LogIoUtils.writeStart( tempLogBuffer, identifier, xid, -1, -1, date, Long.MAX_VALUE );
            LogIoUtils.writeCommand( tempLogBuffer, identifier, new TestXaCommand( commandSize ) );
            LogIoUtils.writeCommit( false, tempLogBuffer, identifier, ++tx, date );
            LogIoUtils.writeDone( tempLogBuffer, identifier );
            tempLogBuffer.read( activeBuffer );
            if ( !timestamps.containsKey( version ) )
             {
                timestamps.put( version, date ); // The first tx timestamp for this version
            }
            boolean rotate = (activeBuffer.capacity() - activeBuffer.remaining()) >= logSize;
            if ( rotate )
            {
                rotate();
            }
            return rotate;
        }
        
        /**
         * @return the total size of the previous log (currently {@link #getHighestLogVersion()}-1
         */
        public void addTransactionsUntilRotationHappens() throws IOException
        {
            int size = 10;
            while ( true )
            {
                if ( addTransaction( size, System.currentTimeMillis() ) )
                {
                    return;
                }
                size = Math.max( 10, (size + 7)%100 );
            }
        }
        
        public void rotate() throws IOException
        {
            lastCommittedTxs.put( version, tx );
            writeBufferToFile( activeBuffer, getFileName( version++ ) );
            pruning.prune( this );
            clearAndWriteHeader();
        }

        private void writeBufferToFile( ByteBuffer buffer, File fileName ) throws IOException
        {
            StoreChannel channel = null;
            try
            {
                buffer.flip();
                channel = FS.open( fileName, "rw" );
                channel.write( buffer );
            }
            finally
            {
                if ( channel != null )
                {
                    channel.close();
                }
            }
        }
        
        public int getTotalSizeOfAllExistingLogFiles()
        {
            int size = 0;
            for ( long version = getHighestLogVersion()-1; version >= 0; version-- )
            {
                File file = getFileName( version );
                if ( FS.fileExists( file ) )
                {
                    size += FS.getFileSize( file );
                }
                else
                {
                    break;
                }
            }
            return size;
        }
        
        public int getTotalTransactionCountOfAllExistingLogFiles()
        {
            if ( getHighestLogVersion() == 0 )
            {
                return 0;
            }
            long upper = getHighestLogVersion()-1;
            long lower = upper;
            while ( lower >= 0 )
            {
                File file = getFileName( lower-1 );
                if ( !FS.fileExists( file ) )
                {
                    break;
                }
                else
                {
                    lower--;
                }
            }
            return (int) (getLastCommittedTxId() - getFirstCommittedTxId( lower ));
        }
        
        @Override
        public Long getFirstCommittedTxId( long version )
        {
            return lastCommittedTxs.get( version );
        }
        
        @Override
        public Long getFirstStartRecordTimestamp( long version )
        {
            return timestamps.get( version );
        }
        
        @Override
        public long getLastCommittedTxId()
        {
            return tx;
        }
    }
    
    private static class TestXaCommand extends XaCommand
    {
        private final int totalSize;

        public TestXaCommand( int totalSize )
        {
            this.totalSize = totalSize;
        }
        
        @Override
        public void execute()
        {   // Do nothing
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            buffer.putInt( totalSize );
            buffer.put( new byte[totalSize-4/*size of the totalSize integer*/] );
        }
    }
}
