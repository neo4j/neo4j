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
package org.neo4j.kernel.impl.transaction.log.pruning;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogFileRecoverer;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableVersionableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReaderFactory;
import org.neo4j.test.ImpermanentGraphDatabase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.keep_logical_logs;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logical_log_rotation_threshold;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_VERSION;

public class TestLogPruning
{
    private GraphDatabaseAPI db;
    private FileSystemAbstraction fs;
    private PhysicalLogFiles files;

    // From the last measurement by figureOutSampleTransactionSizeBytes
    private final int transactionLogSize = 358;

    @After
    public void after() throws Exception
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }

    @Test
    public void noPruning() throws Exception
    {
        newDb( "true", transactionLogSize*2 );

        for ( int i = 0; i < 100; i++ )
        {
            doTransaction();
        }

        long currentVersion = files.getHighestLogVersion();
        for ( long version = 0; version < currentVersion; version++ )
        {
            assertTrue( "Version " + version + " has been unexpectedly pruned",
                    fs.fileExists( files.getLogFileForVersion( version ) ) );
        }
    }

    @Test
    public void pruneByFileSize() throws Exception
    {
        // Given
        int size = 1050;
        int logThreshold = transactionLogSize*3;
        newDb( size + " size", logThreshold );

        // When
        for ( int i = 0; i < 100; i++ )
        {
            doTransaction();
        }

        int logFileSize = logFileSize();
        assertTrue( logFileSize >= size - logThreshold && logFileSize <= size + logThreshold );
    }

    @Test
    public void pruneByFileCount() throws Exception
    {
        int logsToKeep = 5;
        newDb( logsToKeep + " files", transactionLogSize * 3 );

        for ( int i = 0; i < 100; i++ )
        {
            doTransaction();
        }

        assertEquals( logsToKeep, logCount() );
        // TODO we could verify, after the db has been shut down, that the file count is n.
    }

    @Test
    public void pruneByTransactionCount() throws Exception
    {
        int transactionsToKeep = 100;

        int transactionsPerLog = 3;
        newDb( transactionsToKeep + " txs", transactionsToKeep*transactionsPerLog );

        for ( int i = 0; i < 100; i++ )
        {
            doTransaction();
        }

        int transactionCount = transactionCount();
        assertTrue( "Transaction count expected to be within " + transactionsToKeep + " <= txs <= " +
                (transactionsToKeep+transactionsPerLog) + ", but was " + transactionCount,

                transactionCount >= transactionsToKeep &&
                transactionCount <= (transactionsToKeep+transactionsPerLog) );
    }

    private GraphDatabaseAPI newDb( String logPruning, int rotateThreshold )
    {
        fs = new EphemeralFileSystemAbstraction();
        GraphDatabaseAPI db = new ImpermanentGraphDatabase( stringMap(
                keep_logical_logs.name(), logPruning,
                logical_log_rotation_threshold.name(), "" + rotateThreshold
                ) )
        {
            @Override
            protected FileSystemAbstraction createFileSystemAbstraction()
            {
                return fs;
            }
        };
        this.db = db;
        files = new PhysicalLogFiles( new File(db.getStoreDir()), PhysicalLogFile.DEFAULT_NAME, fs );
        return db;
    }

    private void doTransaction()
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( "name", "a somewhat lengthy string of some sort, right?" );
            tx.success();
        }
    }

    @Test
    @Ignore( "Here as a helper to figure out the transaction size of the sample transaction on disk" )
    public void figureOutSampleTransactionSizeBytes()
    {
        db = newDb( "true", transactionLogSize*2 );
        doTransaction();
        db.shutdown();
        System.out.println( fs.getFileSize( files.getLogFileForVersion( 0 ) ) );
    }

    private int aggregateLogData( Extractor extractor ) throws IOException
    {
        int total = 0;
        for ( long i = files.getHighestLogVersion(); i >= 0; i-- )
        {
            File versionFileName = files.getLogFileForVersion( i );
            if ( fs.fileExists( versionFileName ) )
            {
                total += extractor.extract( versionFileName );
            }
            else
            {
                break;
            }
        }
        return total;
    }

    private interface Extractor
    {
        int extract( File from ) throws IOException;
    }

    private int logCount() throws IOException
    {
        return aggregateLogData( new Extractor()
        {
            @Override
            public int extract( File from )
            {
                return 1;
            }
        } );
    }

    private int logFileSize() throws IOException
    {
        return aggregateLogData( new Extractor()
        {
            @Override
            public int extract( File from )
            {
                return (int) fs.getFileSize( from );
            }
        } );
    }

    private int transactionCount() throws IOException
    {
        return aggregateLogData( new Extractor()
        {
            @Override
            public int extract( File from ) throws IOException
            {
                final AtomicInteger counter = new AtomicInteger();
                LogFileRecoverer reader = new LogFileRecoverer(
                        new LogEntryReaderFactory().versionable(),
                        new Visitor<CommittedTransactionRepresentation, IOException>()
                        {
                            @Override
                            public boolean visit( CommittedTransactionRepresentation element ) throws IOException
                            {
                                counter.incrementAndGet();
                                return true;
                            }
                        } );
                LogVersionBridge bridge = new LogVersionBridge()
                {
                    @Override
                    public LogVersionedStoreChannel next( LogVersionedStoreChannel channel ) throws IOException
                    {
                        return channel;
                    }
                };
                StoreChannel storeChannel = fs.open( from, "r" );
                PhysicalLogVersionedStoreChannel versionedStoreChannel =
                        new PhysicalLogVersionedStoreChannel( storeChannel, -1 /* ignored */, CURRENT_LOG_VERSION );
                versionedStoreChannel.position( LOG_HEADER_SIZE );
                try ( ReadableVersionableLogChannel channel =
                              new ReadAheadLogChannel( versionedStoreChannel, bridge, 1000 ) )
                {
                    reader.visit( channel );
                }
                return counter.get();
            }
        } );
    }
}
