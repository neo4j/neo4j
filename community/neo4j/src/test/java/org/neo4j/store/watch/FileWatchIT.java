/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.store.watch;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.io.fs.watcher.event.FileWatchEventListener;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertTrue;

public class FileWatchIT
{

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public CleanupRule cleanupRule = new CleanupRule();

    @Test
    public void notifyAboutStoreFileDeletion() throws Exception
    {
        File storeDir = testDirectory.graphDbDir();
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        GraphDatabaseService database =
                new TestGraphDatabaseFactory().setInternalLogProvider( logProvider ).newEmbeddedDatabase( storeDir );
        cleanupRule.add( database );

        CountDownLatch modificationLatch = new CountDownLatch( 1 );
        CountDownLatch deletionLatch = new CountDownLatch( 1 );
        GraphDatabaseAPI databaseAPI = (GraphDatabaseAPI) database;
        DependencyResolver dependencyResolver = databaseAPI.getDependencyResolver();
        FileWatcher fileWatcher = dependencyResolver.resolveDependency( FileWatcher.class );
        fileWatcher.addFileWatchEventListener( new TestLatchEventListener( deletionLatch, modificationLatch ) );

        createTestNode( database );
        modificationLatch.await();

        deleteMetadataStore( storeDir );
        deletionLatch.await();

        logProvider.assertContainsMessageContaining(
                "Store file '" + MetaDataStore.DEFAULT_NAME + "' was deleted while database was online." );
    }

    @Test
    public void notifyWhenFileWatchingFailToStart()
    {
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        GraphDatabaseService database = new TestGraphDatabaseFactory()
                                            .setInternalLogProvider( logProvider )
                                            .setFileSystem( new NonWatchableFileSystemAbstraction() )
                                            .newEmbeddedDatabase( testDirectory.graphDbDir() );
        cleanupRule.add( database );

        logProvider.assertContainsMessageContaining( "Can not create file watcher for current file system. " +
                "File monitoring capabilities for store files will be disabled." );
    }


    private void deleteMetadataStore( File storeDir )
    {
        File metadataStore = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        FileUtils.deleteFile( metadataStore );
    }

    private void createTestNode( GraphDatabaseService database )
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
            throw new IOException( "You can't watch me!");
        }
    }

    private static class TestLatchEventListener implements FileWatchEventListener
    {
        private final CountDownLatch deletionLatch;
        private final CountDownLatch modificationLatch;

        TestLatchEventListener( CountDownLatch deletionLatch, CountDownLatch modificationLatch )
        {
            this.deletionLatch = deletionLatch;
            this.modificationLatch = modificationLatch;
        }

        @Override
        public void fileDeleted( String fileName )
        {
            assertTrue( fileName.endsWith( MetaDataStore.DEFAULT_NAME ) );
            deletionLatch.countDown();
        }

        @Override
        public void fileModified( String fileName )
        {
            modificationLatch.countDown();
        }
    }
}
