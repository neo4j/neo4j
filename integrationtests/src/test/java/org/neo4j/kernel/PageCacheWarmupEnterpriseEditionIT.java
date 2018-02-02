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
import org.junit.rules.RuleChain;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

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
import org.neo4j.test.rule.EnterpriseDatabaseRule;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static org.neo4j.metrics.MetricsTestHelper.readLongValue;
import static org.neo4j.metrics.source.db.PageCacheMetrics.PC_PAGE_FAULTS;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class PageCacheWarmupEnterpriseEditionIT
{
    private TestDirectory dir = TestDirectory.testDirectory();
    private EnterpriseDatabaseRule db = new EnterpriseDatabaseRule().startLazily();
    @Rule
    public RuleChain rules = RuleChain.outerRule( dir ).around( db );

    @Test
    public void warmupMustReloadHotPagesAfterRestartAndFaultsMustBeVisibleViaMetrics() throws Exception
    {
        File metricsDirectory = dir.directory( "metrics" );
        db.setConfig( MetricsSettings.metricsEnabled, Settings.FALSE )
          .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
          .setConfig( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" );
        db.ensureStarted();

        try ( Transaction tx = db.beginTx() )
        {
            Label label = Label.label( "Label" );
            RelationshipType relationshipType = RelationshipType.withName( "REL" );
            long[] largeValue = new long[1024];
            for ( int i = 0; i < 100; i++ )
            {
                Node node = db.createNode( label );
                node.setProperty( "Niels", "Borh" );
                node.setProperty( "Albert", largeValue );
                for ( int j = 0; j < 10; j++ )
                {
                    Relationship rel = node.createRelationshipTo( node, relationshipType );
                    rel.setProperty( "Max", "Planck" );
                }
            }
            tx.success();
        }

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

        db.restartDatabase(
                MetricsSettings.neoPageCacheEnabled.name(), Settings.TRUE,
                MetricsSettings.csvEnabled.name(), Settings.TRUE,
                MetricsSettings.csvInterval.name(), "100ms",
                MetricsSettings.csvPath.name(), metricsDirectory.getAbsolutePath() );

        long pagesInMemory = pageCount.get();
        assertEventually( "Metrics report should include page cache page faults",
                () -> readLongValue( metricsCsv( metricsDirectory, PC_PAGE_FAULTS ) ),
                greaterThanOrEqualTo( pagesInMemory ), 5, SECONDS );
    }
}
