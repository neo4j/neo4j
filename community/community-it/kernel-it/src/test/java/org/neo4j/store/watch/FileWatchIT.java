/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.store.watch;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.File;
import java.io.IOException;
import java.nio.file.WatchKey;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.fs.watcher.FileWatchEventListener;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.impl.util.watcher.DefaultFileDeletionEventListener;
import org.neo4j.kernel.impl.util.watcher.FileSystemWatcherService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.LogAssertions.assertThat;

@Neo4jLayoutExtension
@EnabledOnOs( OS.LINUX )
class FileWatchIT
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DatabaseLayout databaseLayout;

    private AssertableLogProvider logProvider;
    private GraphDatabaseService database;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        logProvider = new AssertableLogProvider();
        managementService = new TestDatabaseManagementServiceBuilder( databaseLayout ).setInternalLogProvider( logProvider ).build();
        database = managementService.database( DEFAULT_DATABASE_NAME );
    }

    @AfterEach
    void tearDown()
    {
        shutdownDatabaseSilently( managementService );
    }

    @Test
    void notifyAboutStoreFileDeletion() throws IOException, InterruptedException
    {
        String fileName = databaseLayout.metadataStore().getName();
        FileWatcher fileWatcher = getFileWatcher( database );
        CheckPointer checkpointer = getCheckpointer( database );
        DeletionLatchEventListener deletionListener = new DeletionLatchEventListener( fileName );
        fileWatcher.addFileWatchEventListener( deletionListener );

        do
        {
            createNode( database );
            forceCheckpoint( checkpointer );
        }
        while ( !deletionListener.awaitModificationNotification() );

        deleteFile( databaseLayout.databaseDirectory(), fileName );
        deletionListener.awaitDeletionNotification();

        assertThat( logProvider ).containsMessages( '\'' + fileName + "' which belongs to the '" + databaseLayout.databaseDirectory().getName() +
                "' database was deleted while it was running." );
    }

    @Test
    void notifyWhenFileWatchingFailToStart()
    {
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        DatabaseManagementService service = null;
        try
        {
            service = new TestDatabaseManagementServiceBuilder( testDirectory.homeDir( "failed-start-db" ) )
                    .setInternalLogProvider( logProvider )
                    .setFileSystem( new NonWatchableFileSystemAbstraction() )
                    .build();
            assertNotNull( managementService.database( DEFAULT_DATABASE_NAME ) );

            assertThat( logProvider ).containsMessages(
                    "Can not create file watcher for current file system. " + "File monitoring capabilities for store files will be disabled." );
        }
        finally
        {
            shutdownDatabaseSilently( service );
        }
    }

    @Test
    void doNotNotifyAboutLuceneIndexFilesDeletion() throws IOException, InterruptedException
    {
        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) database).getDependencyResolver();
        FileWatcher fileWatcher = getFileWatcher( database );
        CheckPointer checkPointer = dependencyResolver.resolveDependency( CheckPointer.class );

        String propertyStoreName = databaseLayout.propertyStore().getName();
        AccumulativeDeletionEventListener accumulativeListener = new AccumulativeDeletionEventListener();
        ModificationEventListener modificationListener = new ModificationEventListener( propertyStoreName );
        fileWatcher.addFileWatchEventListener( modificationListener );
        fileWatcher.addFileWatchEventListener( accumulativeListener );

        String labelName = "labelName";
        String propertyName = "propertyName";
        Label testLabel = Label.label( labelName );
        createIndexes( database, propertyName, testLabel );
        do
        {
            createNode( database, propertyName, testLabel );
            forceCheckpoint( checkPointer );
        }
        while ( !modificationListener.awaitModificationNotification() );

        fileWatcher.removeFileWatchEventListener( modificationListener );
        ModificationEventListener afterRemovalListener = new ModificationEventListener( propertyStoreName );
        fileWatcher.addFileWatchEventListener( afterRemovalListener );

        dropAllIndexes( database );
        do
        {
            createNode( database, propertyName, testLabel );
            forceCheckpoint( checkPointer );
        }
        while ( !afterRemovalListener.awaitModificationNotification() );

        accumulativeListener.assertDoesNotHaveAnyDeletions();
    }

    @Test
    void doNotMonitorTransactionLogFiles() throws IOException, InterruptedException
    {
        FileWatcher fileWatcher = getFileWatcher( database );
        CheckPointer checkpointer = getCheckpointer( database );
        String metadataStore = databaseLayout.metadataStore().getName();
        ModificationEventListener modificationEventListener = new ModificationEventListener( metadataStore );
        fileWatcher.addFileWatchEventListener( modificationEventListener );

        do
        {
            createNode( database );
            forceCheckpoint( checkpointer );
        }
        while ( !modificationEventListener.awaitModificationNotification() );

        String fileName = TransactionLogFilesHelper.DEFAULT_NAME + ".0";
        DeletionLatchEventListener deletionListener = new DeletionLatchEventListener( fileName );
        fileWatcher.addFileWatchEventListener( deletionListener );
        deleteFile( databaseLayout.getTransactionLogsDirectory(), fileName );
        deletionListener.awaitDeletionNotification();

        assertThat( logProvider ).forClass( DefaultFileDeletionEventListener.class ).forLevel( INFO ).doesNotContainMessage( fileName );
    }

    @Test
    void notifyWhenWholeStoreDirectoryRemoved() throws IOException, InterruptedException
    {
        String fileName = databaseLayout.metadataStore().getName();
        FileWatcher fileWatcher = getFileWatcher( database );
        CheckPointer checkpointer = getCheckpointer( database );

        ModificationEventListener modificationListener = new ModificationEventListener( fileName );
        fileWatcher.addFileWatchEventListener( modificationListener );
        do
        {
            createNode( database );
            forceCheckpoint( checkpointer );
        }
        while ( !modificationListener.awaitModificationNotification() );
        fileWatcher.removeFileWatchEventListener( modificationListener );

        String storeDirectoryName = databaseLayout.databaseDirectory().getName();
        DeletionLatchEventListener eventListener = new DeletionLatchEventListener( storeDirectoryName );
        fileWatcher.addFileWatchEventListener( eventListener );
        FileUtils.deleteRecursively( databaseLayout.databaseDirectory() );

        eventListener.awaitDeletionNotification();

        assertThat( logProvider ).containsMessages( '\'' + storeDirectoryName + "' which belongs to the '" +
                databaseLayout.databaseDirectory().getName() + "' database was deleted while it was running." );
    }

    @Test
    void shouldLogWhenDisabled()
    {
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        DatabaseManagementService service = null;
        try
        {
            service = new TestDatabaseManagementServiceBuilder( testDirectory.homeDir( "failed-start-db" ) )
                    .setInternalLogProvider( logProvider )
                    .setFileSystem( new NonWatchableFileSystemAbstraction() )
                    .setConfig( GraphDatabaseSettings.filewatcher_enabled, false )
                    .build();
            assertNotNull( managementService.database( DEFAULT_DATABASE_NAME ) );

            assertThat( logProvider ).containsMessages( "File watcher disabled by configuration." );
        }
        finally
        {
            shutdownDatabaseSilently( service );
        }
    }

    private static void shutdownDatabaseSilently( DatabaseManagementService managementService )
    {
        if ( managementService != null )
        {
            try
            {
                managementService.shutdown();
            }
            catch ( Exception expected )
            {
                // ignored
            }
        }
    }

    private static void dropAllIndexes( GraphDatabaseService database )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            for ( IndexDefinition definition : transaction.schema().getIndexes() )
            {
                definition.drop();
            }
            transaction.commit();
        }
    }

    private static void createIndexes( GraphDatabaseService database, String propertyName, Label testLabel )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.schema().indexFor( testLabel ).on( propertyName ).create();
            transaction.commit();
        }

        try ( Transaction tx = database.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
        }
    }

    private static void forceCheckpoint( CheckPointer checkPointer ) throws IOException
    {
        checkPointer.forceCheckPoint( new SimpleTriggerInfo( "testForceCheckPoint" ) );
    }

    private static void createNode( GraphDatabaseService database, String propertyName, Label testLabel )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = transaction.createNode( testLabel );
            node.setProperty( propertyName, "value" );
            transaction.commit();
        }
    }

    private static CheckPointer getCheckpointer( GraphDatabaseService database )
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( CheckPointer.class );
    }

    private static FileWatcher getFileWatcher( GraphDatabaseService database )
    {
        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) database).getDependencyResolver();
        return dependencyResolver.resolveDependency( FileSystemWatcherService.class ).getFileWatcher();
    }

    private static void deleteFile( File storeDir, String fileName )
    {
        File metadataStore = new File( storeDir, fileName );
        FileUtils.deleteFile( metadataStore );
    }

    private static void createNode( GraphDatabaseService database )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.createNode();
            transaction.commit();
        }
    }

    private static class NonWatchableFileSystemAbstraction extends DefaultFileSystemAbstraction
    {
        @Override
        public FileWatcher fileWatcher() throws IOException
        {
            throw new IOException( "You can't watch me!" );
        }
    }

    private static class AccumulativeDeletionEventListener implements FileWatchEventListener
    {
        private final List<String> deletedFiles = new ArrayList<>();

        @Override
        public void fileDeleted( WatchKey key, String fileName )
        {
            deletedFiles.add( fileName );
        }

        void assertDoesNotHaveAnyDeletions()
        {
            assertThat( deletedFiles ).as( "Should not have any deletions registered" ).isEmpty();
        }
    }

    private static class ModificationEventListener implements FileWatchEventListener
    {
        final String expectedFileName;
        private final CountDownLatch modificationLatch = new CountDownLatch( 1 );

        ModificationEventListener( String expectedFileName )
        {
            this.expectedFileName = expectedFileName;
        }

        @Override
        public void fileModified( WatchKey key, String fileName )
        {
            if ( expectedFileName.equals( fileName ) )
            {
                modificationLatch.countDown();
            }
        }

        boolean awaitModificationNotification() throws InterruptedException
        {
            return modificationLatch.await(1, TimeUnit.SECONDS);
        }
    }

    private static class DeletionLatchEventListener extends ModificationEventListener
    {
        private final CountDownLatch deletionLatch = new CountDownLatch( 1 );

        DeletionLatchEventListener( String expectedFileName )
        {
            super( expectedFileName );
        }

        @Override
        public void fileDeleted( WatchKey key, String fileName )
        {
            if ( fileName.endsWith( expectedFileName ) )
            {
                deletionLatch.countDown();
            }
        }

        void awaitDeletionNotification() throws InterruptedException
        {
            deletionLatch.await();
        }

    }
}
