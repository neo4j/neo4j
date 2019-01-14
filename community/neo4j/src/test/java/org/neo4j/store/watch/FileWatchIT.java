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
package org.neo4j.store.watch;

import org.apache.commons.lang3.SystemUtils;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.index.impl.lucene.explicit.LuceneDataSource;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.fs.watcher.FileWatchEventListener;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.impl.util.watcher.DefaultFileDeletionEventListener;
import org.neo4j.kernel.impl.util.watcher.FileSystemWatcherService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeFalse;

public class FileWatchIT
{
    private static final long TEST_TIMEOUT = 600_000;

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    private File storeDir;
    private AssertableLogProvider logProvider;
    private GraphDatabaseService database;

    @Before
    public void setUp()
    {
        storeDir = testDirectory.graphDbDir();
        logProvider = new AssertableLogProvider();
        database = new TestGraphDatabaseFactory().setInternalLogProvider( logProvider ).newEmbeddedDatabase( storeDir );
    }

    @After
    public void tearDown()
    {
        shutdownDatabaseSilently( database );
    }

    @Test( timeout = TEST_TIMEOUT )
    public void notifyAboutStoreFileDeletion() throws Exception
    {
        assumeFalse( SystemUtils.IS_OS_WINDOWS );

        String fileName = MetaDataStore.DEFAULT_NAME;
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

        deleteFile( storeDir, fileName );
        deletionListener.awaitDeletionNotification();

        logProvider.assertContainsMessageContaining(
                "'" + fileName + "' which belongs to the store was deleted while database was running." );
    }

    @Test( timeout = TEST_TIMEOUT )
    public void notifyWhenFileWatchingFailToStart()
    {
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        GraphDatabaseService db = null;
        try
        {
            db = new TestGraphDatabaseFactory().setInternalLogProvider( logProvider )
                    .setFileSystem( new NonWatchableFileSystemAbstraction() )
                    .newEmbeddedDatabase( testDirectory.directory( "failed-start-db" ) );

            logProvider.assertContainsMessageContaining( "Can not create file watcher for current file system. " +
                    "File monitoring capabilities for store files will be disabled." );
        }
        finally
        {
            shutdownDatabaseSilently( db );
        }
    }

    @Test( timeout = TEST_TIMEOUT )
    public void notifyAboutExplicitIndexFolderRemoval() throws InterruptedException, IOException
    {
        String monitoredDirectory = getExplicitIndexDirectory( storeDir );

        FileWatcher fileWatcher = getFileWatcher( database );
        CheckPointer checkPointer = getCheckpointer( database );
        DeletionLatchEventListener deletionListener = new DeletionLatchEventListener( monitoredDirectory );
        ModificationEventListener modificationEventListener = new ModificationEventListener( MetaDataStore.DEFAULT_NAME );
        fileWatcher.addFileWatchEventListener( deletionListener );
        fileWatcher.addFileWatchEventListener( modificationEventListener );

        do
        {
            createNode( database );
            forceCheckpoint( checkPointer );
        }
        while ( !modificationEventListener.awaitModificationNotification() );

        deleteStoreDirectory( storeDir, monitoredDirectory );
        deletionListener.awaitDeletionNotification();

        logProvider.assertContainsMessageContaining(
                "'" + monitoredDirectory + "' which belongs to the store was deleted while database was running." );
    }

    @Test( timeout = TEST_TIMEOUT )
    public void doNotNotifyAboutLuceneIndexFilesDeletion() throws InterruptedException, IOException
    {
        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) database).getDependencyResolver();
        FileWatcher fileWatcher = getFileWatcher( database );
        CheckPointer checkPointer = dependencyResolver.resolveDependency( CheckPointer.class );

        String propertyStoreName = MetaDataStore.DEFAULT_NAME + StoreFactory.PROPERTY_STORE_NAME;
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

    @Test( timeout = TEST_TIMEOUT )
    public void doNotMonitorTransactionLogFiles() throws InterruptedException, IOException
    {
        assumeFalse( SystemUtils.IS_OS_WINDOWS );

        FileWatcher fileWatcher = getFileWatcher( database );
        CheckPointer checkpointer = getCheckpointer( database );
        ModificationEventListener modificationEventListener =
                new ModificationEventListener( MetaDataStore.DEFAULT_NAME );
        fileWatcher.addFileWatchEventListener( modificationEventListener );

        do
        {
            createNode( database );
            forceCheckpoint( checkpointer );
        }
        while ( !modificationEventListener.awaitModificationNotification() );

        String fileName = TransactionLogFiles.DEFAULT_NAME + ".0";
        DeletionLatchEventListener deletionListener = new DeletionLatchEventListener( fileName );
        fileWatcher.addFileWatchEventListener( deletionListener );
        deleteFile( storeDir, fileName );
        deletionListener.awaitDeletionNotification();

        AssertableLogProvider.LogMatcher logMatcher =
                AssertableLogProvider.inLog( DefaultFileDeletionEventListener.class )
                        .info( containsString( fileName ) );
        logProvider.assertNone( logMatcher );
    }

    @Test( timeout = TEST_TIMEOUT )
    public void notifyWhenWholeStoreDirectoryRemoved() throws IOException, InterruptedException
    {
        assumeFalse( SystemUtils.IS_OS_WINDOWS );

        String fileName = MetaDataStore.DEFAULT_NAME;
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

        String storeDirectoryName = TestDirectory.DATABASE_DIRECTORY;
        DeletionLatchEventListener eventListener = new DeletionLatchEventListener( storeDirectoryName );
        fileWatcher.addFileWatchEventListener( eventListener );
        FileUtils.deleteRecursively( storeDir );

        eventListener.awaitDeletionNotification();

        logProvider.assertContainsMessageContaining(
                "'" + storeDirectoryName + "' which belongs to the store was deleted while database was running." );
    }

    @Test( timeout = TEST_TIMEOUT )
    public void shouldLogWhenDisabled()
    {
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        GraphDatabaseService db = null;
        try
        {
            db = new TestGraphDatabaseFactory().setInternalLogProvider( logProvider )
                    .setFileSystem( new NonWatchableFileSystemAbstraction() )
                    .newEmbeddedDatabaseBuilder( testDirectory.directory( "failed-start-db" ) )
                    .setConfig( GraphDatabaseSettings.filewatcher_enabled, "false" )
                    .newGraphDatabase();

            logProvider.assertContainsMessageContaining( "File watcher disabled by configuration." );
        }
        finally
        {
            shutdownDatabaseSilently( db );
        }
    }

    private void shutdownDatabaseSilently( GraphDatabaseService databaseService )
    {
        if ( databaseService != null )
        {
            try
            {
                databaseService.shutdown();
            }
            catch ( Exception expected )
            {
                // ignored
            }
        }
    }

    private void dropAllIndexes( GraphDatabaseService database )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            for ( IndexDefinition definition : database.schema().getIndexes() )
            {
                definition.drop();
            }
            transaction.success();
        }
    }

    private void createIndexes( GraphDatabaseService database, String propertyName, Label testLabel )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            database.schema().indexFor( testLabel ).on( propertyName ).create();
            transaction.success();
        }

        try ( Transaction ignored = database.beginTx() )
        {
            database.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
        }
    }

    private void forceCheckpoint( CheckPointer checkPointer ) throws IOException
    {
        checkPointer.forceCheckPoint( new SimpleTriggerInfo( "testForceCheckPoint" ) );
    }

    private String getExplicitIndexDirectory( File storeDir )
    {
        File schemaIndexDirectory = LuceneDataSource.getLuceneIndexStoreDirectory( storeDir );
        Path relativeIndexPath = storeDir.toPath().relativize( schemaIndexDirectory.toPath() );
        return relativeIndexPath.getName( 0 ).toString();
    }

    private void createNode( GraphDatabaseService database, String propertyName, Label testLabel )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( testLabel );
            node.setProperty( propertyName, "value" );
            transaction.success();
        }
    }

    private CheckPointer getCheckpointer( GraphDatabaseService database )
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( CheckPointer.class );
    }

    private FileWatcher getFileWatcher( GraphDatabaseService database )
    {
        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) database).getDependencyResolver();
        return dependencyResolver.resolveDependency( FileSystemWatcherService.class ).getFileWatcher();
    }

    private void deleteFile( File storeDir, String fileName )
    {
        File metadataStore = new File( storeDir, fileName );
        FileUtils.deleteFile( metadataStore );
    }

    private void deleteStoreDirectory( File storeDir, String directoryName ) throws IOException
    {
        File directory = new File( storeDir, directoryName );
        FileUtils.deleteRecursively( directory );
    }

    private void createNode( GraphDatabaseService database )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            database.createNode();
            transaction.success();
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
        private List<String> deletedFiles = new ArrayList<>();

        @Override
        public void fileDeleted( String fileName )
        {
            deletedFiles.add( fileName );
        }

        void assertDoesNotHaveAnyDeletions()
        {
            assertThat( "Should not have any deletions registered", deletedFiles, Matchers.empty() );
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
        public void fileModified( String fileName )
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
        public void fileDeleted( String fileName )
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
