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
import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.ext.udc.UdcSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.metrics.MetricsSettings;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EnterpriseDatabaseRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

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
    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public EnterpriseDatabaseRule db = new EnterpriseDatabaseRule().startLazily();
    private TestDirectory dir = db.getTestDirectory();

    private void verifyEventuallyWarmsUp( long pagesInMemory, File metricsDirectory ) throws Exception
    {
        assertEventually( "Metrics report should include page cache page faults",
                () -> readLongValue( metricsCsv( metricsDirectory, PC_PAGE_FAULTS ) ),
                greaterThanOrEqualTo( pagesInMemory ), 20, SECONDS );
    }

    @Test
    public void warmupMustReloadHotPagesAfterRestartAndFaultsMustBeVisibleViaMetrics() throws Exception
    {
        File metricsDirectory = dir.directory( "metrics" );
        db.setConfig( MetricsSettings.metricsEnabled, Settings.FALSE )
          .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
          .setConfig( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" );
        db.ensureStarted();

        createTestData( db );
        long pagesInMemory = waitForCacheProfile( db );

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
        db.setConfig( MetricsSettings.metricsEnabled, Settings.FALSE )
          .setConfig( UdcSettings.udc_enabled, Settings.FALSE )
          .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
          .setConfig( OnlineBackupSettings.online_backup_server, "localhost:" + backupPort )
          .setConfig( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" );
        db.ensureStarted();

        createTestData( db );
        long pagesInMemory = waitForCacheProfile( db );

        BinaryLatch latch = pauseProfile( db ); // We don't want torn profile files in this test.

        File metricsDirectory = dir.cleanDirectory( "metrics" );
        File backupDir = dir.cleanDirectory( "backup" );
        assertTrue( OnlineBackup.from( "localhost", backupPort ).backup( backupDir, null, true, "8m" ).isConsistent() );
        latch.release();
        DatabaseRule.RestartAction useBackupDir = ( fs, storeDir ) ->
        {
            fs.deleteRecursively( storeDir );
            fs.copyRecursively( backupDir, storeDir );
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
        db.setConfig( MetricsSettings.metricsEnabled, Settings.FALSE )
          .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
          .setConfig( OnlineBackupSettings.online_backup_server, "localhost:" + backupPort )
          .setConfig( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "1ms" );
        db.ensureStarted();

        createTestData( db );
        waitForCacheProfile( db );

        for ( int i = 0; i < 20; i++ )
        {
            File backupDir = dir.cleanDirectory( "backup" );
            assertTrue( OnlineBackup.from( "localhost", backupPort ).backup( backupDir, null, true, "8m" ).isConsistent() );
        }
    }

    @Test
    public void cacheProfilesMustBeIncludedInOfflineBackups() throws Exception
    {
        db.setConfig( MetricsSettings.metricsEnabled, Settings.FALSE )
          .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
          .setConfig( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" );
        db.ensureStarted();
        createTestData( db );
        long pagesInMemory = waitForCacheProfile( db );

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
        File storeDir = db.getStoreDir();
        File data = dir.cleanDirectory( "data" );
        File databases = new File( data, "databases" );
        File graphdb = new File( databases, "graph.db" );
        assertTrue( graphdb.mkdirs() );
        FileUtils.copyRecursively( storeDir, graphdb );
        FileUtils.deleteRecursively( storeDir );
        Path homePath = data.toPath().getParent();
        File dumpDir = dir.cleanDirectory( "dump-dir" );
        adminTool.execute( homePath, homePath, "dump", "--database=graph.db", "--to=" + dumpDir );

        FileUtils.deleteRecursively( graphdb );
        File dumpFile = new File( dumpDir, "graph.db.dump" );
        adminTool.execute( homePath, homePath, "load", "--database=graph.db", "--from=" + dumpFile );
        FileUtils.copyRecursively( graphdb, storeDir );
        FileUtils.deleteRecursively( graphdb );

        File metricsDirectory = dir.cleanDirectory( "metrics" );
        db.ensureStarted(
                OnlineBackupSettings.online_backup_enabled.name(), Settings.FALSE,
                MetricsSettings.neoPageCacheEnabled.name(), Settings.TRUE,
                MetricsSettings.csvEnabled.name(), Settings.TRUE,
                MetricsSettings.csvInterval.name(), "100ms",
                MetricsSettings.csvPath.name(), metricsDirectory.getAbsolutePath() );

        verifyEventuallyWarmsUp( pagesInMemory, metricsDirectory );
    }
}
