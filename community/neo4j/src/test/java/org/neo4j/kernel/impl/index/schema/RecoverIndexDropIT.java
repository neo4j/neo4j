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
package org.neo4j.kernel.impl.index.schema;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.api.index.IndexMap;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.recovery.RecoveryMonitor;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.test.TestLabels.LABEL_ONE;

/**
 * Issue came up when observing that recovering an INDEX DROP command didn't actually call {@link IndexProxy#drop()},
 * and actually did nothing to that {@link IndexProxy} except removing it from its {@link IndexMap}.
 * This would have {@link IndexingService} forget about that index and at shutdown not call {@link IndexProxy#close()},
 * resulting in open page cache files, for any page cache mapped native index files.
 *
 * This would be a problem if the INDEX DROP command was present in the transaction log, but the db had been killed
 * before the command had been applied and so the files would still remain, and not be dropped either when that command
 * was recovered.
 */
public class RecoverIndexDropIT
{
    private static final String KEY = "key";

    @Rule
    public final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory( fs );

    @Test
    public void shouldDropIndexOnRecovery() throws IOException
    {
        // given a transaction stream ending in an INDEX DROP command.
        CommittedTransactionRepresentation dropTransaction = prepareDropTransaction();
        File storeDir = directory.graphDbDir();
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        createIndex( db );
        db.shutdown();
        appendDropTransactionToTransactionLog( storeDir, dropTransaction );

        // when recovering this (the drop transaction with the index file intact)
        Monitors monitors = new Monitors();
        AssertRecoveryIsPerformed recoveryMonitor = new AssertRecoveryIsPerformed();
        monitors.addMonitorListener( recoveryMonitor );
        db = new TestGraphDatabaseFactory().setMonitors( monitors ).newEmbeddedDatabase( storeDir );
        try
        {
            assertTrue( recoveryMonitor.recoveryWasPerformed );

            // then
            try ( Transaction tx = db.beginTx() )
            {
                assertEquals( 0, count( db.schema().getIndexes() ) );
                tx.success();
            }
        }
        finally
        {
            // and the ability to shut down w/o failing on still open files
            db.shutdown();
        }
    }

    private IndexDefinition createIndex( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = db.schema().indexFor( LABEL_ONE ).on( KEY ).create();
            tx.success();
            return index;
        }
    }

    private void appendDropTransactionToTransactionLog( File storeDir, CommittedTransactionRepresentation dropTransaction ) throws IOException
    {
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( storeDir, fs ).build();
        File logFile = logFiles.getLogFileForVersion( logFiles.getHighestLogVersion() );
        StoreChannel writeStoreChannel = fs.open( logFile, OpenMode.READ_WRITE );
        writeStoreChannel.position( writeStoreChannel.size() );
        try ( PhysicalFlushableChannel writeChannel = new PhysicalFlushableChannel( writeStoreChannel ) )
        {
            new LogEntryWriter( writeChannel ).serialize( dropTransaction );
        }
    }

    private CommittedTransactionRepresentation prepareDropTransaction() throws IOException
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newEmbeddedDatabase( directory.directory( "preparation" ) );
        try
        {
            // Create index
            IndexDefinition index;
            index = createIndex( db );
            try ( Transaction tx = db.beginTx() )
            {
                index.drop();
                tx.success();
            }
            return extractLastTransaction( db );
        }
        finally
        {
            db.shutdown();
        }
    }

    private CommittedTransactionRepresentation extractLastTransaction( GraphDatabaseAPI db ) throws IOException
    {
        LogicalTransactionStore txStore = db.getDependencyResolver().resolveDependency( LogicalTransactionStore.class );
        CommittedTransactionRepresentation transaction = null;
        try ( TransactionCursor cursor = txStore.getTransactions( TransactionIdStore.BASE_TX_ID + 1 ) )
        {
            while ( cursor.next() )
            {
                transaction = cursor.get();
            }
        }
        return transaction;
    }

    private static class AssertRecoveryIsPerformed implements RecoveryMonitor
    {
        boolean recoveryWasPerformed;

        @Override
        public void recoveryRequired( LogPosition recoveryPosition )
        {
            recoveryWasPerformed = true;
        }
    }
}
