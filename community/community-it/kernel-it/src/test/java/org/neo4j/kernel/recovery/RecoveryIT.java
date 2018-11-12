/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.recovery;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.valueOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.kernel.configuration.Config.defaults;
import static org.neo4j.kernel.recovery.Recovery.isRecoveryRequired;
import static org.neo4j.kernel.recovery.Recovery.performRecovery;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class RecoveryIT
{
    @Inject
    public DefaultFileSystemAbstraction fileSystem;
    @Inject
    public TestDirectory directory;

    @Test
    void recoveryRequiredOnDatabaseWithoutCorrectCheckpoints() throws Exception
    {
        GraphDatabaseService database = createDatabase();
        generateSomeData( database );
        database.shutdown();
        removeLastCheckpointRecordFromLastLogFile();

        assertTrue( isRecoveryRequired( fileSystem, directory.databaseLayout(), defaults() ) );
    }

    @Test
    void recoveryNotRequiredWhenDatabaseNotFound() throws Exception
    {
        DatabaseLayout absentDatabase = directory.databaseLayout( "absent" );
        assertFalse( isRecoveryRequired( fileSystem, absentDatabase, defaults() ) );
    }

    @Test
    void recoverEmptyDatabase() throws Exception
    {
        GraphDatabaseService database = createDatabase();
        database.shutdown();
        removeLastCheckpointRecordFromLastLogFile();

        recoverDatabase();
    }

    @Test
    void recoverDatabaseWithNodes() throws Exception
    {
        GraphDatabaseService database = createDatabase();

        int numberOfNodes = 10;
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                database.createNode();
                transaction.success();
            }
        }
        database.shutdown();
        removeLastCheckpointRecordFromLastLogFile();

        recoverDatabase();

        GraphDatabaseService recoveredDatabase = createDatabase();
        try ( Transaction transaction = recoveredDatabase.beginTx() )
        {
            assertEquals( numberOfNodes, count( recoveredDatabase.getAllNodes() ) );
        }
        finally
        {
            recoveredDatabase.shutdown();
        }
    }

    @Test
    void recoverDatabaseWithNodesAndRelationshipsAndRelationshipTypes() throws Exception
    {
        GraphDatabaseService database = createDatabase();

        int numberOfRelationships = 10;
        int numberOfNodes = numberOfRelationships * 2;
        for ( int i = 0; i < numberOfRelationships; i++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node start = database.createNode();
                Node stop = database.createNode();
                start.createRelationshipTo( stop, withName( valueOf( i ) ) );
                transaction.success();
            }
        }
        database.shutdown();
        removeLastCheckpointRecordFromLastLogFile();

        recoverDatabase();

        GraphDatabaseService recoveredDatabase = createDatabase();
        try ( Transaction transaction = recoveredDatabase.beginTx() )
        {
            assertEquals( numberOfNodes, count( recoveredDatabase.getAllNodes() ) );
            assertEquals( numberOfRelationships, count( recoveredDatabase.getAllRelationships() ) );
            assertEquals( numberOfRelationships, count( recoveredDatabase.getAllRelationshipTypesInUse() ) );
        }
        finally
        {
            recoveredDatabase.shutdown();
        }
    }

    @Test
    void recoverDatabaseWithProperties() throws Exception
    {
        GraphDatabaseService database = createDatabase();

        int numberOfRelationships = 10;
        int numberOfNodes = numberOfRelationships * 2;
        for ( int i = 0; i < numberOfRelationships; i++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node start = database.createNode();
                Node stop = database.createNode();
                start.setProperty( "start" + i, i );
                stop.setProperty( "stop" + i, i );
                start.createRelationshipTo( stop, withName( valueOf( i ) ) );
                transaction.success();
            }
        }
        database.shutdown();
        removeLastCheckpointRecordFromLastLogFile();

        recoverDatabase();

        GraphDatabaseService recoveredDatabase = createDatabase();
        try ( Transaction transaction = recoveredDatabase.beginTx() )
        {
            assertEquals( numberOfNodes, count( recoveredDatabase.getAllNodes() ) );
            assertEquals( numberOfRelationships, count( recoveredDatabase.getAllRelationships() ) );
            assertEquals( numberOfRelationships, count( recoveredDatabase.getAllRelationshipTypesInUse() ) );
            assertEquals( numberOfNodes, count( recoveredDatabase.getAllPropertyKeys() ) );
        }
        finally
        {
            recoveredDatabase.shutdown();
        }
    }

    @Test
    void recoverDatabaseWithIndex() throws Exception
    {
        GraphDatabaseService database = createDatabase();

        int numberOfRelationships = 10;
        int numberOfNodes = numberOfRelationships * 2;
        String startProperty = "start";
        String stopProperty = "stop";
        Label startMarker = Label.label( "start" );
        Label stopMarker = Label.label( "stop" );

        try ( Transaction transaction = database.beginTx() )
        {
            database.schema().indexFor( startMarker ).on( startProperty ).create();
            database.schema().constraintFor( stopMarker ).assertPropertyIsUnique( stopProperty ).create();
            transaction.success();
        }
        try ( Transaction transaction = database.beginTx() )
        {
            database.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            transaction.success();
        }

        for ( int i = 0; i < numberOfRelationships; i++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node start = database.createNode( startMarker );
                Node stop = database.createNode( stopMarker );

                start.setProperty( startProperty, i );
                stop.setProperty( stopProperty, i );
                start.createRelationshipTo( stop, withName( valueOf( i ) ) );
                transaction.success();
            }
        }
        database.shutdown();
        removeLastCheckpointRecordFromLastLogFile();

        recoverDatabase();

        GraphDatabaseService recoveredDatabase = createDatabase();
        try ( Transaction transaction = recoveredDatabase.beginTx() )
        {
            assertEquals( numberOfNodes, count( recoveredDatabase.getAllNodes() ) );
            assertEquals( numberOfRelationships, count( recoveredDatabase.getAllRelationships() ) );
            assertEquals( numberOfRelationships, count( recoveredDatabase.getAllRelationshipTypesInUse() ) );
            assertEquals( 2, count( recoveredDatabase.getAllPropertyKeys() ) );
        }
        finally
        {
            recoveredDatabase.shutdown();
        }
    }

    private void recoverDatabase() throws Exception
    {
        DatabaseLayout databaseLayout = directory.databaseLayout();
        assertTrue( isRecoveryRequired( databaseLayout, defaults() ) );
        performRecovery( databaseLayout );
        assertFalse( isRecoveryRequired( databaseLayout, defaults() ) );
    }

    private void removeLastCheckpointRecordFromLastLogFile() throws IOException
    {
        LogPosition checkpointPosition = null;

        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( directory.databaseDir(), fileSystem ).build();
        LogFile transactionLogFile = logFiles.getLogFile();
        VersionAwareLogEntryReader<ReadableLogChannel> entryReader = new VersionAwareLogEntryReader<>();
        LogPosition startPosition = LogPosition.start( logFiles.getHighestLogVersion() );
        try ( ReadableLogChannel reader = transactionLogFile.getReader( startPosition ) )
        {
            LogEntry logEntry;
            do
            {
                logEntry = entryReader.readLogEntry( reader );
                if ( logEntry instanceof CheckPoint )
                {
                    checkpointPosition = ((CheckPoint) logEntry).getLogPosition();
                }
            }
            while ( logEntry != null );
        }
        if ( checkpointPosition != null )
        {
            try ( StoreChannel storeChannel = fileSystem.open( logFiles.getHighestLogFile(), OpenMode.READ_WRITE ) )
            {
                storeChannel.truncate( checkpointPosition.getByteOffset() );
            }
        }
    }

    private static void generateSomeData( GraphDatabaseService database )
    {
        for ( int i = 0; i < 10; i++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node node1 = database.createNode();
                Node node2 = database.createNode();
                node1.createRelationshipTo( node2, withName( "Type" + i ) );
                node2.setProperty( "a", "b" );
                transaction.success();
            }
        }
    }

    private GraphDatabaseService createDatabase()
    {
        return new TestGraphDatabaseFactory().newEmbeddedDatabase( directory.databaseDir() );
    }
}
