/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;

import org.neo4j.backup.OnlineBackup;
import org.neo4j.commandline.admin.AdminTool;
import org.neo4j.commandline.admin.BlockerLocator;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.RealOutsideWorld;
import org.neo4j.ext.udc.UdcSettings;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.metrics.MetricsSettings;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EnterpriseDatabaseRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.util.concurrent.BinaryLatch;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static org.neo4j.metrics.MetricsTestHelper.readLongValue;
import static org.neo4j.metrics.source.db.PageCacheMetrics.PC_PAGE_FAULTS;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class PageCacheWarmupEnterpriseEditionIT extends PageCacheWarmupTestSupport
{
    private final AssertableLogProvider logProvider = new AssertableLogProvider( true );

    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final EnterpriseDatabaseRule db = new EnterpriseDatabaseRule( testDirectory )
    {
        @Override
        protected void configure( GraphDatabaseFactory databaseFactory )
        {
            super.configure( databaseFactory );
            ((TestGraphDatabaseFactory) databaseFactory).setInternalLogProvider( logProvider );
        }
    }.startLazily();

    private static void verifyEventuallyWarmsUp( long pagesInMemory, File metricsDirectory ) throws Exception
    {
        assertEventually( "Metrics report should include page cache page faults",
                () -> readLongValue( metricsCsv( metricsDirectory, PC_PAGE_FAULTS ) ),
                greaterThanOrEqualTo( pagesInMemory ), 20, SECONDS );
    }

    @Test
    public void warmupMustReloadHotPagesAfterRestartAndFaultsMustBeVisibleViaMetrics() throws Exception
    {
        File metricsDirectory = testDirectory.directory( "metrics" );
        db.withSetting( MetricsSettings.metricsEnabled, Settings.FALSE )
          .withSetting( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
          .withSetting( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" );
        db.ensureStarted();

        createTestData( db );
        long pagesInMemory = waitForCacheProfile( db.getMonitors() );

        db.restartDatabase(
                MetricsSettings.neoPageCacheEnabled.name(), Settings.TRUE,
                MetricsSettings.csvEnabled.name(), Settings.TRUE,
                MetricsSettings.csvInterval.name(), "100ms",
                MetricsSettings.csvPath.name(), metricsDirectory.getAbsolutePath() );

        verifyEventuallyWarmsUp( pagesInMemory, metricsDirectory );
    }

    @Test
    public void cacheProfilesMustBeIncludedInOnlineBackups() throws Exception
    {
        int backupPort = PortAuthority.allocatePort();
        db.withSetting( MetricsSettings.metricsEnabled, Settings.FALSE )
          .withSetting( UdcSettings.udc_enabled, Settings.FALSE )
          .withSetting( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
          .withSetting( OnlineBackupSettings.online_backup_server, "localhost:" + backupPort )
          .withSetting( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" );
        db.ensureStarted();

        createTestData( db );
        long pagesInMemory = waitForCacheProfile( db.getMonitors() );

        BinaryLatch latch = pauseProfile( db.getMonitors() ); // We don't want torn profile files in this test.

        File metricsDirectory = testDirectory.cleanDirectory( "metrics" );
        File backupDir = testDirectory.cleanDirectory( "backup" );
        assertTrue( OnlineBackup.from( "localhost", backupPort ).backup( backupDir ).isConsistent() );
        latch.release();
        DatabaseRule.RestartAction useBackupDir = ( fs, storeDir ) ->
        {
            fs.deleteRecursively( storeDir.databaseDirectory() );
            fs.copyRecursively( backupDir, storeDir.databaseDirectory() );
        };
        db.restartDatabase( useBackupDir,
                OnlineBackupSettings.online_backup_enabled.name(), Settings.FALSE,
                MetricsSettings.neoPageCacheEnabled.name(), Settings.TRUE,
                MetricsSettings.csvEnabled.name(), Settings.TRUE,
                MetricsSettings.csvInterval.name(), "100ms",
                MetricsSettings.csvPath.name(), metricsDirectory.getAbsolutePath() );

        verifyEventuallyWarmsUp( pagesInMemory, metricsDirectory );
    }

    @Test
    public void cacheProfilesMustNotInterfereWithOnlineBackups() throws Exception
    {
        // Here we are testing that the file modifications done by the page cache profiler,
        // does not make online backup throw any exceptions.
        int backupPort = PortAuthority.allocatePort();
        db.withSetting( MetricsSettings.metricsEnabled, Settings.FALSE )
          .withSetting( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
          .withSetting( OnlineBackupSettings.online_backup_server, "localhost:" + backupPort )
          .withSetting( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "1ms" );
        db.ensureStarted();

        createTestData( db );
        waitForCacheProfile( db.getMonitors() );

        for ( int i = 0; i < 20; i++ )
        {
            String backupDir = testDirectory.cleanDirectory( "backup" ).getAbsolutePath();
            assertTrue( OnlineBackup.from( "localhost", backupPort ).full( backupDir ).isConsistent() );
        }
    }

    @Test
    public void cacheProfilesMustBeIncludedInOfflineBackups() throws Exception
    {
        db.withSetting( MetricsSettings.metricsEnabled, Settings.FALSE )
          .withSetting( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
          .withSetting( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" );
        db.ensureStarted();
        createTestData( db );
        long pagesInMemory = waitForCacheProfile( db.getMonitors() );

        db.shutdownAndKeepStore();

        AdminTool adminTool = new AdminTool(
                CommandLocator.fromServiceLocator(),
                BlockerLocator.fromServiceLocator(),
                new RealOutsideWorld()
                {
                    @Override
                    public void exit( int status )
                    {
                        assertThat( "exit code", status, is( 0 ) );
                    }
                },
                true );
        File databaseDir = db.databaseLayout().databaseDirectory();
        File data = testDirectory.cleanDirectory( "data" );
        File databases = new File( data, "databases" );
        File graphdb = testDirectory.databaseDir( databases );
        FileUtils.copyRecursively( databaseDir, graphdb );
        FileUtils.deleteRecursively( databaseDir );
        Path homePath = data.toPath().getParent();
        File dumpDir = testDirectory.cleanDirectory( "dump-dir" );
        adminTool.execute( homePath, homePath, "dump", "--database=" + GraphDatabaseSettings.DEFAULT_DATABASE_NAME, "--to=" + dumpDir );

        FileUtils.deleteRecursively( graphdb );
        File dumpFile = new File( dumpDir, "graph.db.dump" );
        adminTool.execute( homePath, homePath, "load", "--database=" + GraphDatabaseSettings.DEFAULT_DATABASE_NAME, "--from=" + dumpFile );
        FileUtils.copyRecursively( graphdb, databaseDir );
        FileUtils.deleteRecursively( graphdb );

        File metricsDirectory = testDirectory.cleanDirectory( "metrics" );
        db.withSetting( MetricsSettings.neoPageCacheEnabled, Settings.TRUE )
          .withSetting( MetricsSettings.csvEnabled, Settings.TRUE )
          .withSetting( MetricsSettings.csvInterval, "100ms" )
          .withSetting( MetricsSettings.csvPath, metricsDirectory.getAbsolutePath() );
        db.ensureStarted();

        verifyEventuallyWarmsUp( pagesInMemory, metricsDirectory );
    }

    @Test
    public void logPageCacheWarmupStartCompletionMessages() throws Exception
    {
        File metricsDirectory = testDirectory.directory( "metrics" );
        db.withSetting( MetricsSettings.metricsEnabled, Settings.FALSE )
                .withSetting( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .withSetting( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" );
        db.ensureStarted();

        createTestData( db );
        long pagesInMemory = waitForCacheProfile( db.getMonitors() );

        db.restartDatabase(
                MetricsSettings.neoPageCacheEnabled.name(), Settings.TRUE,
                MetricsSettings.csvEnabled.name(), Settings.TRUE,
                MetricsSettings.csvInterval.name(), "100ms",
                MetricsSettings.csvPath.name(), metricsDirectory.getAbsolutePath() );

        verifyEventuallyWarmsUp( pagesInMemory, metricsDirectory );

        logProvider.assertContainsMessageContaining( "Page cache warmup started." );
        logProvider.assertContainsMessageContaining( "Page cache warmup completed. %d pages loaded. Duration: %s." );
    }
}
