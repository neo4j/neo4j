/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.upgrade;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.function.Consumer;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Exceptions;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.highlimit.v300.HighLimitV3_0_0;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.UnexpectedUpgradingStoreFormatException;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class RecordFormatsMigrationIT
{
    private static final Label LABEL = Label.label( "Centipede" );
    private static final String PROPERTY = "legs";
    private static final int VALUE = 42;

    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final TestDirectory testDirectory = TestDirectory.testDirectory( fileSystemRule.get() );

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDirectory ).around( fileSystemRule );

    @Test
    public void migrateLatestStandardToLatestHighLimit()
    {
        executeAndStopDb( startStandardFormatDb(), this::createNode );
        assertLatestStandardStore();

        executeAndStopDb( startHighLimitFormatDb(), this::assertNodeExists );
        assertLatestHighLimitStore();
    }

    @Test
    public void migrateHighLimitV3_0ToLatestHighLimit()
    {
        executeAndStopDb( startDb( HighLimitV3_0_0.NAME ), this::createNode );
        assertStoreFormat( HighLimitV3_0_0.RECORD_FORMATS );

        executeAndStopDb( startHighLimitFormatDb(), this::assertNodeExists );
        assertLatestHighLimitStore();
    }

    @Test
    public void migrateHighLimitToStandard()
    {
        executeAndStopDb( startHighLimitFormatDb(), this::createNode );
        assertLatestHighLimitStore();

        try
        {
            startStandardFormatDb();
            fail( "Should not be possible to downgrade" );
        }
        catch ( Exception e )
        {
            assertThat( Exceptions.rootCause( e ), instanceOf( UnexpectedUpgradingStoreFormatException.class ) );
        }
        assertLatestHighLimitStore();
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
        return startDb( Standard.LATEST_NAME );
    }

    private GraphDatabaseService startHighLimitFormatDb()
    {
        return startDb( HighLimit.NAME );
    }

    private GraphDatabaseService startDb( String recordFormatName )
    {
        return new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() )
                .setConfig( GraphDatabaseSettings.allow_upgrade, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.record_format, recordFormatName )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .newGraphDatabase();
    }

    private void assertLatestStandardStore()
    {
        assertStoreFormat( Standard.LATEST_RECORD_FORMATS );
    }

    private void assertLatestHighLimitStore()
    {
        assertStoreFormat( HighLimit.RECORD_FORMATS );
    }

    private void assertStoreFormat( RecordFormats expected )
    {
        Config config = Config.defaults( GraphDatabaseSettings.pagecache_memory, "8m" );
        try ( PageCache pageCache = ConfigurableStandalonePageCacheFactory.createPageCache( fileSystemRule.get(), config ) )
        {
            RecordFormats actual = RecordFormatSelector.selectForStoreOrConfig( config, testDirectory.graphDbDir(),
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
