/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.backup.OnlineBackup;
import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.pagecache.PageCacheWarmerMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.metrics.MetricsSettings;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EnterpriseDatabaseRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertTrue;
import static org.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static org.neo4j.metrics.MetricsTestHelper.readLongValue;
import static org.neo4j.metrics.source.db.PageCacheMetrics.PC_PAGE_FAULTS;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class PageCacheWarmupEnterpriseEditionIT
{
    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public EnterpriseDatabaseRule db = new EnterpriseDatabaseRule().startLazily();
    private TestDirectory dir = db.getTestDirectory();

    private void createTestData()
    {
        try ( Transaction tx = db.beginTx() )
        {
            Label label = Label.label( "Label" );
            RelationshipType relationshipType = RelationshipType.withName( "REL" );
            long[] largeValue = new long[1024];
            for ( int i = 0; i < 1000; i++ )
            {
                Node node = db.createNode( label );
                node.setProperty( "Niels", "Borh" );
                node.setProperty( "Albert", largeValue );
                for ( int j = 0; j < 30; j++ )
                {
                    Relationship rel = node.createRelationshipTo( node, relationshipType );
                    rel.setProperty( "Max", "Planck" );
                }
            }
            tx.success();
        }
    }

    private long waitForCacheProfile()
    {
        AtomicLong pageCount = new AtomicLong();
        BinaryLatch profileLatch = new BinaryLatch();
        db.resolveDependency( Monitors.class ).addMonitorListener( new PageCacheWarmerMonitor()
        {
            @Override
            public void warmupCompleted( long elapsedMillis, long pagesLoaded )
            {
            }

            @Override
            public void profileCompleted( long elapsedMillis, long pagesInMemory )
            {
                pageCount.set( pagesInMemory );
                profileLatch.release();
            }
        } );
        profileLatch.await();
        return pageCount.get();
    }

    @Test
    public void warmupMustReloadHotPagesAfterRestartAndFaultsMustBeVisibleViaMetrics() throws Exception
    {
        File metricsDirectory = dir.directory( "metrics" );
        db.setConfig( MetricsSettings.metricsEnabled, Settings.FALSE )
          .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
          .setConfig( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" );
        db.ensureStarted();

        createTestData();
        long pagesInMemory = waitForCacheProfile();

        db.restartDatabase(
                MetricsSettings.neoPageCacheEnabled.name(), Settings.TRUE,
                MetricsSettings.csvEnabled.name(), Settings.TRUE,
                MetricsSettings.csvInterval.name(), "100ms",
                MetricsSettings.csvPath.name(), metricsDirectory.getAbsolutePath() );

        assertEventually( "Metrics report should include page cache page faults",
                () -> readLongValue( metricsCsv( metricsDirectory, PC_PAGE_FAULTS ) ),
                greaterThanOrEqualTo( pagesInMemory ), 20, SECONDS );
    }

    @Test
    public void cacheProfilesMustBeIncludedInOnlineBackups() throws Exception
    {
        int backupPort = PortAuthority.allocatePort();
        db.setConfig( MetricsSettings.metricsEnabled, Settings.FALSE )
          .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
          .setConfig( OnlineBackupSettings.online_backup_server, "localhost:" + backupPort )
          .setConfig( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" );
        db.ensureStarted();

        createTestData();
        long pagesInMemory = waitForCacheProfile();

        File metricsDirectory = dir.cleanDirectory( "metrics" );
        File backupDir = dir.cleanDirectory( "backup" );
        assertTrue( OnlineBackup.from( "localhost", backupPort ).backup( backupDir ).isConsistent() );
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
                MetricsSettings.csvPath.name(), metricsDirectory.getAbsolutePath());

        assertEventually( "Metrics report should include page cache page faults",
                () -> readLongValue( metricsCsv( metricsDirectory, PC_PAGE_FAULTS ) ),
                greaterThanOrEqualTo( pagesInMemory ), 5, SECONDS );
    }
}
