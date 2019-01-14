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
package db;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DatabaseStartupTest
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void startTheDatabaseWithWrongVersionShouldFailWithUpgradeNotAllowed() throws Throwable
    {
        // given
        // create a store
        File storeDir = testDirectory.graphDbDir();
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
        db.shutdown();

        // mess up the version in the metadatastore
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
                PageCache pageCache = StandalonePageCacheFactory.createPageCache( fileSystem ) )
        {
            MetaDataStore.setRecord( pageCache, new File( storeDir, MetaDataStore.DEFAULT_NAME ),
                    MetaDataStore.Position.STORE_VERSION, MetaDataStore.versionStringToLong( "bad" ));
        }

        // when
        try
        {
            new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
            fail( "It should have failed." );
        }
        catch ( RuntimeException ex )
        {
            // then
            assertTrue( ex.getCause() instanceof LifecycleException );
            assertTrue( ex.getCause().getCause() instanceof IllegalArgumentException );
            assertEquals( "Unknown store version 'bad'", ex.getCause().getCause().getMessage() );
        }
    }

    @Test
    public void startTheDatabaseWithWrongVersionShouldFailAlsoWhenUpgradeIsAllowed() throws Throwable
    {
        // given
        // create a store
        File storeDir = testDirectory.graphDbDir();
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
        db.shutdown();

        // mess up the version in the metadatastore
        String badStoreVersion = "bad";
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              PageCache pageCache = StandalonePageCacheFactory.createPageCache( fileSystem ) )
        {
            MetaDataStore.setRecord( pageCache, new File( storeDir, MetaDataStore.DEFAULT_NAME ),
                    MetaDataStore.Position.STORE_VERSION, MetaDataStore.versionStringToLong( badStoreVersion ) );
        }

        // when
        try
        {
            new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                    .setConfig( GraphDatabaseSettings.allow_upgrade, "true" ).newGraphDatabase();
            fail( "It should have failed." );
        }
        catch ( RuntimeException ex )
        {
            // then
            assertTrue( ex.getCause() instanceof LifecycleException );
            assertTrue( ex.getCause().getCause() instanceof StoreUpgrader.UnexpectedUpgradingStoreVersionException );
        }
    }
}
