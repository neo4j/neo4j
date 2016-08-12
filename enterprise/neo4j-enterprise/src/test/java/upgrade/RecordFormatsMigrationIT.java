/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package upgrade;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.function.Consumer;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Exceptions;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.UnexpectedUpgradingStoreFormatException;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class RecordFormatsMigrationIT
{
    private static final Label LABEL = Label.label( "Centipede" );
    private static final String PROPERTY = "legs";
    private static final int VALUE = 42;

    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    @Rule
    public final TestDirectory testDir = TestDirectory.testDirectory( fs );

    @Test
    public void migrateStandardToHighLimit() throws IOException
    {
        executeAndStopDb( startStandardFormatDb(), this::createNode );
        assertStandardStore();

        executeAndStopDb( startHighLimitFormatDb(), this::assertNodeExists );
        assertHighLimitStore();
    }

    @Test
    public void migrateHighLimitToStandard() throws IOException
    {
        executeAndStopDb( startHighLimitFormatDb(), this::createNode );
        assertHighLimitStore();

        try
        {
            startStandardFormatDb();
            fail( "Should not be possible to downgrade" );
        }
        catch ( Exception e )
        {
            assertThat( Exceptions.rootCause( e ), instanceOf( UnexpectedUpgradingStoreFormatException.class ) );
        }
        assertHighLimitStore();
    }

    private void createNode( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node start = db.createNode( LABEL );
            start.setProperty( PROPERTY, VALUE );
            tx.success();
        }
    }

    private void assertNodeExists( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertNotNull( db.findNode( LABEL, PROPERTY, VALUE ) );
            tx.success();
        }
    }

    private GraphDatabaseService startStandardFormatDb()
    {
        return startDb( StandardV3_0.NAME );
    }

    private GraphDatabaseService startHighLimitFormatDb()
    {
        return startDb( HighLimit.NAME );
    }

    private GraphDatabaseService startDb( String recordFormatName )
    {
        return new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( testDir.graphDbDir() )
                .setConfig( GraphDatabaseSettings.allow_store_upgrade, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.record_format, recordFormatName )
                .newGraphDatabase();
    }

    private void assertStandardStore() throws IOException
    {
        assertStoreFormat( StandardV3_0.RECORD_FORMATS );
    }

    private void assertHighLimitStore() throws IOException
    {
        assertStoreFormat( HighLimit.RECORD_FORMATS );
    }

    private void assertStoreFormat( RecordFormats expected ) throws IOException
    {
        Config config = new Config( stringMap( GraphDatabaseSettings.pagecache_memory.name(), "8m" ) );
        try ( PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs, config ) )
        {
            RecordFormats actual = RecordFormatSelector.selectForStoreOrConfig( config, testDir.graphDbDir(), fs,
                    pageCache, NullLogProvider.getInstance() );
            assertNotNull( actual );
            assertEquals( expected.storeVersion(), actual.storeVersion() );
        }
    }

    private static void executeAndStopDb( GraphDatabaseService db, Consumer<GraphDatabaseService> action )
    {
        try
        {
            action.accept( db );
        }
        finally
        {
            db.shutdown();
        }
    }
}
