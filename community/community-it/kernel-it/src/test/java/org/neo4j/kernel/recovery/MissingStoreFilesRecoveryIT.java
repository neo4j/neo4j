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
package org.neo4j.kernel.recovery;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.valueOf;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.RelationshipType.withName;

@TestDirectoryExtension
class MissingStoreFilesRecoveryIT
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;
    private DatabaseManagementService managementService;
    private DatabaseLayout databaseLayout;
    private DatabaseManagementServiceBuilder serviceBuilder;
    private NamedDatabaseId defaultNamedDatabaseId;
    private static final Label testNodes = Label.label( "testNodes" );

    @BeforeEach
    void setUp() throws IOException
    {
        serviceBuilder = new DatabaseManagementServiceBuilder( testDirectory.homeDir() );
        managementService = serviceBuilder.build();
        var databaseApi = defaultDatabase( managementService );
        createSomeData( databaseApi );
        databaseLayout = databaseApi.databaseLayout();

        defaultNamedDatabaseId = getDatabaseManager().databaseIdRepository().getByName( DEFAULT_DATABASE_NAME ).get();

        managementService.shutdown();
    }

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void databaseStartFailingOnMissingFilesAndMissedTxLogs() throws IOException
    {
        fileSystem.deleteFile( databaseLayout.nodeStore() );
        fileSystem.deleteRecursively( databaseLayout.getTransactionLogsDirectory() );

        managementService = serviceBuilder.build();
        var dbStateService = getDatabaseStateService();
        assertTrue( dbStateService.causeOfFailure( defaultNamedDatabaseId ).isPresent() );
    }

    @Test
    void failToStartOnMissingFilesAndPartialTransactionLogs() throws IOException
    {
        LogFiles logFiles = prepareDatabaseWithTwoTxLogFiles();

        fileSystem.deleteFile( logFiles.getLogFileForVersion( 0 ) );
        fileSystem.deleteFile( databaseLayout.nodeStore() );

        var dbStateService = getDatabaseStateService();
        var failure = dbStateService.causeOfFailure( defaultNamedDatabaseId );
        assertFalse( failure.isPresent() );
        assertFalse( fileSystem.fileExists( databaseLayout.nodeStore() ) );
    }

    private LogFiles prepareDatabaseWithTwoTxLogFiles() throws IOException
    {
        managementService = serviceBuilder.build();
        var databaseApi = defaultDatabase( managementService );
        LogFiles logFiles = rotateTransactionLogs( databaseApi );
        assertNotNull( logFiles.getLogFileForVersion( 1 ) );
        createSomeData( databaseApi );
        managementService.shutdown();
        return logFiles;
    }

    private DatabaseManager getDatabaseManager()
    {
        return defaultDatabase( managementService ).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }

    private DatabaseStateService getDatabaseStateService()
    {
        return defaultDatabase( managementService ).getDependencyResolver().resolveDependency( DatabaseStateService.class );
    }

    private GraphDatabaseAPI defaultDatabase( DatabaseManagementService managementService )
    {
        return (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    private static LogFiles rotateTransactionLogs( GraphDatabaseAPI databaseApi ) throws IOException
    {
        LogFiles logFiles = databaseApi.getDependencyResolver().resolveDependency( LogFiles.class );
        LogFile logFile = logFiles.getLogFile();
        logFile.rotate();
        return logFiles;
    }

    private static void createSomeData( GraphDatabaseAPI databaseApi ) throws IOException
    {
        insertData( databaseApi );
        CheckPointer checkPointer = databaseApi.getDependencyResolver().resolveDependency( CheckPointer.class );
        checkPointer.forceCheckPoint( new SimpleTriggerInfo( "forcedCheckpointInTheMiddle" ) );
        insertData( databaseApi );
    }

    private static void insertData( GraphDatabaseAPI databaseApi )
    {
        for ( int i = 0; i < 100; i++ )
        {
            try ( Transaction transaction = databaseApi.beginTx() )
            {
                Node nodeA = transaction.createNode( testNodes );
                Node nodeB = transaction.createNode( testNodes );
                nodeA.createRelationshipTo( nodeB, withName( valueOf( i ) ) );
                transaction.commit();
            }
        }
    }
}
