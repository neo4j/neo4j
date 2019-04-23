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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;

import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.PhysicalFlushableChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.api.index.IndexMap;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.recovery.RecoveryMonitor;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
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
@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class RecoverIndexDropIT
{
    private static final String KEY = "key";

    @Inject
    private DefaultFileSystemAbstraction fs;
    @Inject
    private TestDirectory directory;

    @Test
    void shouldDropIndexOnRecovery() throws IOException
    {
        // given a transaction stream ending in an INDEX DROP command.
        CommittedTransactionRepresentation dropTransaction = prepareDropTransaction();
        var databaseLayout = directory.databaseLayout();
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( directory.storeDir() ).build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        createIndex( db );
        managementService.shutdown();
        appendDropTransactionToTransactionLog( directory.databaseLayout().getTransactionLogsDirectory(), dropTransaction );

        // when recovering this (the drop transaction with the index file intact)
        Monitors monitors = new Monitors();
        AssertRecoveryIsPerformed recoveryMonitor = new AssertRecoveryIsPerformed();
        monitors.addMonitorListener( recoveryMonitor );
        managementService = new TestDatabaseManagementServiceBuilder( directory.storeDir() ).setMonitors( monitors )
                .build();
        db = managementService.database( DEFAULT_DATABASE_NAME );
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
            managementService.shutdown();
        }
    }

    private static IndexDefinition createIndex( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = db.schema().indexFor( LABEL_ONE ).on( KEY ).create();
            tx.success();
            return index;
        }
    }

    private void appendDropTransactionToTransactionLog( File transactionLogsDirectory, CommittedTransactionRepresentation dropTransaction ) throws IOException
    {
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( transactionLogsDirectory, fs ).build();
        File logFile = logFiles.getLogFileForVersion( logFiles.getHighestLogVersion() );
        StoreChannel writeStoreChannel = fs.write( logFile );
        writeStoreChannel.position( writeStoreChannel.size() );
        try ( PhysicalFlushableChannel writeChannel = new PhysicalFlushableChannel( writeStoreChannel ) )
        {
            new LogEntryWriter( writeChannel ).serialize( dropTransaction );
        }
    }

    private CommittedTransactionRepresentation prepareDropTransaction() throws IOException
    {
        DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder( directory.directory( "preparation" ) ).build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
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
            managementService.shutdown();
        }
    }

    private static CommittedTransactionRepresentation extractLastTransaction( GraphDatabaseAPI db ) throws IOException
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
