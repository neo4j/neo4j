/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package db;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DatabaseStartupTest
{
    @Rule
    public final TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

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
        try ( PageCache pageCache = StandalonePageCacheFactory.createPageCache( new DefaultFileSystemAbstraction() ))
        {
            MetaDataStore.setRecord( pageCache, new File(storeDir, MetaDataStore.DEFAULT_NAME ),
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
            assertTrue( ex.getCause().getCause() instanceof UpgradeNotAllowedByConfigurationException );
            assertEquals( "Failed to start Neo4j with an older data store version. To enable automatic upgrade, " +
                          "please set configuration parameter \"allow_store_upgrade=true\"",
                    ex.getCause().getCause().getMessage());
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
        try ( PageCache pageCache = StandalonePageCacheFactory.createPageCache( new DefaultFileSystemAbstraction() ))
        {
            MetaDataStore.setRecord( pageCache, new File(storeDir, MetaDataStore.DEFAULT_NAME ),
                    MetaDataStore.Position.STORE_VERSION, MetaDataStore.versionStringToLong( "bad" ));
        }

        // when
        try
        {
            new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                    .setConfig( GraphDatabaseSettings.allow_store_upgrade, "true" ).newGraphDatabase();
            fail( "It should have failed." );
        }
        catch ( RuntimeException ex )
        {
            // then
            assertTrue( ex.getCause() instanceof LifecycleException );
            assertTrue( ex.getCause().getCause() instanceof StoreUpgrader.UnexpectedUpgradingStoreVersionException );
            assertThat( ex.getCause().getCause().getMessage(),
                    containsString( "has a store version number that we cannot upgrade from." ) );
        }
    }
}
