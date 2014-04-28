/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIntMap;
import org.neo4j.collection.primitive.PrimitiveLongIntVisitor;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.api.impl.index.LuceneIndexMigrator.Monitor;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.index.ProviderMeta;
import org.neo4j.kernel.api.index.util.FolderLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.store.SchemaCache;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.storemigration.StoreMigrationTool;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.Unzip;

import static java.lang.Boolean.TRUE;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.allow_store_upgrade;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.api.impl.index.DirectoryFactory.PERSISTENT;
import static org.neo4j.kernel.api.impl.index.IndexWriterFactories.standard;
import static org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProviderFactory.KEY;
import static org.neo4j.kernel.api.index.ProviderMeta.DEFAULT_NAME;
import static org.neo4j.kernel.api.index.SchemaIndexProvider.getRootDirectory;
import static org.neo4j.kernel.impl.storemigration.UpgradeConfiguration.ALLOW_UPGRADE;

/**
 * Uses a legacy and migrates it to the current format.
 */
public class LuceneIndexMigratorTest
{
    @Test
    public void shouldOnlyMigrateLegacyIndexesInNeedOfMigration() throws Throwable
    {
        // GIVEN legacy store
        // with the neostore part of it already migrated (since this is a unit test, where the unit is
        // the lucene migrator)
        migrateNeoStore( storeDir );

        // and the index migrator's dependencies satisfied
        StoreUpgrader migrationProcess = new StoreUpgrader( ALLOW_UPGRADE, fileSystem, StoreUpgrader.NO_MONITOR );
        NeoStore neoStore = openNeoStore( storeDir ); // neoStore closed in @After
        migrationProcess.satisfyDependency( SchemaCache.class, new SchemaCache( neoStore.getSchemaStore() ) );
        migrationProcess.satisfyDependency( PropertyAccessor.class,
                new NeoStoreIndexStoreView( LockService.NO_LOCK_SERVICE, neoStore ) );

        // WHEN
        File rootDirectory = getRootDirectory( storeDir, KEY );
        CapturingMonitor monitor = new CapturingMonitor();
        LifeSupport life = new LifeSupport();
        ProviderMeta meta = life.add( new ProviderMeta( fileSystem, new File( rootDirectory, DEFAULT_NAME ) ) );
        life.start();
        LuceneIndexMigrator migrator = new LuceneIndexMigrator( rootDirectory, new FolderLayout( rootDirectory ),
                PERSISTENT, standard(), new LuceneDocumentStructure(), monitor, meta );
        migrationProcess.addParticipant( migrator );
        migrationProcess.migrateIfNeeded( storeDir );
        neoStore.close();
        life.shutdown();
        assertEquals( "Unexpected number of indexes migrated", 2, monitor.indexes.size() );
        monitor.updates.visitEntries( new PrimitiveLongIntVisitor()
        {
            @Override
            public void visited( long indexId, int value )
            {
                int expectedNumberOfPathcedDocuments =
                        indexId == 1 ? 0
                      : indexId == 2 ? 5
                                     : 3;
                assertEquals( "Unexpected number of entries updated for index " + indexId,
                        expectedNumberOfPathcedDocuments, value );
            }
        } );

        // THEN
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir.getPath() );
        try
        {
            assertIndexContents( db );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldMigrateLuceneIndexesAsPartOfNormalDbMigration() throws Exception
    {
        // WHEN
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir.getAbsolutePath() )
                .setConfig( GraphDatabaseSettings.allow_store_upgrade, Boolean.TRUE.toString() )
                .newGraphDatabase();
        try
        {
            assertIndexContents( db );
        }
        finally
        {
            db.shutdown();
        }
    }

    /*
     * What does this test do here? StoreMigrationTool is part of kernel... well, it just so happens
     * that the tool should migrate indexes as well. So having a test here (previously untested
     * at the time of adding this) increases reliability of it.
     */
    @Test
    public void shouldHaveMigrationToolMigrateDatabaseWithIndexes() throws Exception
    {
        // WHEN
        new StoreMigrationTool().run( storeDir.getAbsolutePath(),
                new Config( stringMap( allow_store_upgrade.name(), TRUE.toString() ) ), StringLogger.DEV_NULL,
                StoreUpgrader.NO_MONITOR );

        // THEN
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir.getAbsolutePath() );
        try
        {
            assertIndexContents( db );
        }
        finally
        {
            db.shutdown();
        }
    }

    private void assertIndexContents( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertIndexContains( db, labelOne, keyOne, "Alistair" );
            assertIndexContains( db, labelTwo, keyTwo, 1L, lossyLongValueOne, lossyLongValueTwo,
                    lossyDoubleValueOne, lossyDoubleValueTwo );
            assertIndexContains( db, labelThree, keyThree, "Mattias", lossyLongValueOne, lossyLongValueTwo );
        }
    }

    private static class CapturingMonitor implements Monitor
    {
        private final PrimitiveLongSet indexes = Primitive.longSet();
        private final PrimitiveLongIntMap updates = Primitive.longIntMap();

        @Override
        public void indexNeedsMigration( long indexId )
        {
            indexes.add( indexId );
        }

        @Override
        public void indexMigrated( long indexId, int numberOfEntriesUpdated )
        {
            assertTrue( indexes.contains( indexId ) );
            updates.put( indexId, numberOfEntriesUpdated );
        }
    }

    private void assertIndexContains( GraphDatabaseService db, Label label, String propertyKey, Object... values )
    {
        boolean found = false;
        for ( IndexDefinition index : db.schema().getIndexes( label ) )
        {
            if ( single( index.getPropertyKeys() ).equals( propertyKey ) )
            {
                found = true;
                break;
            }
        }
        assertTrue( "Unable to find index for label " + label + " ON " + propertyKey, found );

        for ( Object value : values )
        {
            assertNotNull( single( db.findNodesByLabelAndProperty( label, propertyKey, value ) ) );
        }
    }

    private void migrateNeoStore( File storeDir )
    {
        StoreUpgrader upgrader = new StoreUpgrader( ALLOW_UPGRADE, fileSystem, StoreUpgrader.NO_MONITOR );
        upgrader.addParticipant( new StoreMigrator( new SilentMigrationProgressMonitor(), fileSystem,
                idGeneratorFactory ) );
        upgrader.migrateIfNeeded( storeDir );
    }

    private NeoStore openNeoStore( File storeDir )
    {
        File neoStoreFileName = new File( storeDir, NeoStore.DEFAULT_NAME );
        StoreFactory storeFactory = new StoreFactory( neoStoreFileName, StringLogger.DEV_NULL );
        neoStore = storeFactory.newNeoStore( neoStoreFileName );
        return neoStore;
    }

    private File storeDir;
    private NeoStore neoStore;
    private final DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
    private final DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory();

    @Before
    public void before() throws IOException
    {
        storeDir = Unzip.unzip( getClass(), "2.0-lossy-index-db.zip" );
    }

    @After
    public void after()
    {
        if ( neoStore != null )
        {
            neoStore.close();
        }
    }

    /* ========================================================================================
     * Code used to create the legacy database (when executed on the legacy version of course).
     * Even if it's not used, please keep it around for reference.
     * ======================================================================================== */

    private static final Label labelOne = label( "One" ), labelTwo = label( "Two" ), labelThree = label( "Three" );
    private static final String keyOne = "name", keyTwo = "numbers", keyThree = "mixed";
    private static final long lossyLongValueOne = 2147483647000577536L, lossyLongValueTwo = 2147483647000577465L;
    private static final double
            lossyDoubleValueOne = 10000000000000000000.0D,
            lossyDoubleValueTwo = 9223372036854775807.0D;

    public void createLegacyDb() throws Exception
    {
        // GIVEN
        String path = "lossy-index-db";
        FileUtils.deleteRecursively( new File( path ) );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( path );
        try
        {
            createIndex( db, labelOne, keyOne );
            createIndex( db, labelTwo, keyTwo );
            createIndex( db, labelThree, keyThree );
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
                tx.success();
            }

            createNodes( db, labelOne, keyOne, "Ben", "Alistair", "Johan" );
            createNodes( db, labelTwo, keyTwo, 1L, 10324L, lossyLongValueOne, lossyLongValueTwo,
                    lossyDoubleValueOne, lossyDoubleValueTwo );
            createNodes( db, labelThree, keyThree, "Mattias", "Johan", 10324L, lossyLongValueOne, lossyLongValueTwo );
        }
        finally
        {
            db.shutdown();
        }
    }

    private void createNodes( GraphDatabaseService db, Label label, String propertyKey, Object... propertyValues )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( Object propertyValue : propertyValues )
            {
                db.createNode( label ).setProperty( propertyKey, propertyValue );
            }
            tx.success();
        }
    }

    private void createIndex( GraphDatabaseService db, Label label, String string )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( string ).create();
            tx.success();
        }
    }

}
