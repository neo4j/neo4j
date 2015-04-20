/*
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
package org.neo4j.kernel.impl.transaction.log.pruning;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.log.LogRotation;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableVersionableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReaderFactory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.keep_logical_logs;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_VERSION;

public class TestLogPruning
{
    private interface Extractor
    {
        int extract( File from ) throws IOException;
    }

    private GraphDatabaseAPI db;
    private FileSystemAbstraction fs;
    private PhysicalLogFiles files;
    private int rotateEveryNTransactions, performedTransactions;

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
        newDb( "true", 2 );

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
        int transactionByteSize = figureOutSampleTransactionSizeBytes();
        int transactionsPerFile = 3;
        int logThreshold = transactionByteSize * transactionsPerFile;
        newDb( logThreshold + " size", 1 );

        // When
        for ( int i = 0; i < 100; i++ )
        {
            doTransaction();
        }

        int totalLogFileSize = logFileSize();
        double totalTransactions = (double) totalLogFileSize / transactionByteSize;
        assertTrue( totalTransactions >= 3 && totalTransactions < 4 );
    }

    @Test
    public void pruneByFileCount() throws Exception
    {
        int logsToKeep = 5;
        newDb( logsToKeep + " files", 3 );

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
        newDb( transactionsToKeep + " txs", 3 );

        for ( int i = 0; i < 100; i++ )
        {
            doTransaction();
        }

        int transactionCount = transactionCount();
        assertTrue( "Transaction count expected to be within " + transactionsToKeep + " <= txs <= " +
                    (transactionsToKeep + transactionsPerLog) + ", but was " + transactionCount,

                transactionCount >= transactionsToKeep &&
                transactionCount <= (transactionsToKeep + transactionsPerLog) );
    }

    private GraphDatabaseAPI newDb( String logPruning, int rotateEveryNTransactions )
    {
        this.rotateEveryNTransactions = rotateEveryNTransactions;
        fs = new EphemeralFileSystemAbstraction();
        TestGraphDatabaseFactory gdf = new TestGraphDatabaseFactory();
        gdf.setFileSystem( fs );
        GraphDatabaseBuilder builder = gdf.newImpermanentDatabaseBuilder();
        builder.setConfig( keep_logical_logs, logPruning );
        this.db = (GraphDatabaseAPI) builder.newGraphDatabase();
        files = new PhysicalLogFiles( new File( db.getStoreDir() ), PhysicalLogFile.DEFAULT_NAME, fs );
        return db;
    }

    private void doTransaction() throws IOException
    {
        if ( ++performedTransactions >= rotateEveryNTransactions )
        {
            db.getDependencyResolver().resolveDependency( LogRotation.class ).rotateLogFile();
            performedTransactions = 0;
        }

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( "name", "a somewhat lengthy string of some sort, right?" );
            tx.success();
        }
    }

    private int figureOutSampleTransactionSizeBytes() throws IOException
    {
        db = newDb( "true", 5 );
        doTransaction();
        db.shutdown();
        return (int) fs.getFileSize( files.getLogFileForVersion( 0 ) );
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
                int counter = 0;
                LogVersionBridge bridge = new LogVersionBridge()
                {
                    @Override
                    public LogVersionedStoreChannel next( LogVersionedStoreChannel channel ) throws IOException
                    {
                        return channel;
                    }
                };
                StoreChannel storeChannel = fs.open( from, "r" );
                LogVersionedStoreChannel versionedStoreChannel = PhysicalLogFile.openForVersion( files, fs, CURRENT_LOG_VERSION );
                        new PhysicalLogVersionedStoreChannel( storeChannel, -1 /* ignored */, CURRENT_LOG_VERSION );
                try ( ReadableVersionableLogChannel channel =
                              new ReadAheadLogChannel( versionedStoreChannel, bridge, 1000 ) )
                {
                    try (PhysicalTransactionCursor<ReadableVersionableLogChannel> physicalTransactionCursor =
                            new PhysicalTransactionCursor<>( channel, new LogEntryReaderFactory().versionable() ))
                    {
                        while ( physicalTransactionCursor.next())
                        {
                            counter++;
                        }
                    }
                }
                return counter;
            }
        } );
    }
}
