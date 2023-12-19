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
package org.neo4j.metrics;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.metrics.source.cluster.ClusterMetrics;
import org.neo4j.metrics.source.db.CheckPointingMetrics;
import org.neo4j.metrics.source.db.CypherMetrics;
import org.neo4j.metrics.source.db.EntityCountMetrics;
import org.neo4j.metrics.source.db.TransactionMetrics;
import org.neo4j.metrics.source.jvm.ThreadMetrics;
import org.neo4j.test.ha.ClusterRule;

import static java.lang.System.currentTimeMillis;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.check_point_interval_time;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.cypher_min_replan_interval;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.ha.ClusterManager.clusterOfSize;
import static org.neo4j.metrics.MetricsSettings.csvEnabled;
import static org.neo4j.metrics.MetricsSettings.csvPath;
import static org.neo4j.metrics.MetricsSettings.graphiteInterval;
import static org.neo4j.metrics.MetricsSettings.metricsEnabled;
import static org.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static org.neo4j.metrics.MetricsTestHelper.readLongValueAndAssert;

public class MetricsKernelExtensionFactoryIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withSharedSetting( GraphDatabaseSettings.record_id_batch_size, "1" );

    private HighlyAvailableGraphDatabase db;
    private File outputPath;

    @Before
    public void setup()
    {
        outputPath = clusterRule.directory( "metrics" );
        Map<String, String> config = stringMap(
                MetricsSettings.neoEnabled.name(), Settings.TRUE,
                metricsEnabled.name(), Settings.TRUE,
                csvEnabled.name(), Settings.TRUE,
                cypher_min_replan_interval.name(), "0m",
                csvPath.name(), outputPath.getAbsolutePath(),
                check_point_interval_time.name(), "100ms",
                graphiteInterval.name(), "1s",
                OnlineBackupSettings.online_backup_enabled.name(), Settings.FALSE
        );
        db = clusterRule.withSharedConfig( config ).withCluster( clusterOfSize( 1 ) ).startCluster().getMaster();
        addNodes( 1 ); // to make sure creation of label and property key tokens do not mess up with assertions in tests
    }

    @Test
    public void shouldShowTxCommittedMetricsWhenMetricsEnabled() throws Throwable
    {
        // GIVEN
        long lastCommittedTransactionId = db.getDependencyResolver().resolveDependency( TransactionIdStore.class )
                .getLastCommittedTransactionId();

        // Create some activity that will show up in the metrics data.
        addNodes( 1000 );
        File metricsFile = metricsCsv( outputPath, TransactionMetrics.TX_COMMITTED );

        // WHEN
        // We should at least have a "timestamp" column, and a "neo4j.transaction.committed" column
        long committedTransactions = readLongValueAndAssert( metricsFile,
                ( newValue, currentValue ) -> newValue >= currentValue );

        // THEN
        assertThat( committedTransactions, greaterThanOrEqualTo( lastCommittedTransactionId ) );
        assertThat( committedTransactions, lessThanOrEqualTo( lastCommittedTransactionId + 1001L ) );
    }

    @Test
    public void shouldShowEntityCountMetricsWhenMetricsEnabled() throws Throwable
    {
        // GIVEN
        // Create some activity that will show up in the metrics data.
        addNodes( 1000 );
        File metricsFile = metricsCsv( outputPath, EntityCountMetrics.COUNTS_NODE );

        // WHEN
        // We should at least have a "timestamp" column, and a "neo4j.transaction.committed" column
        long committedTransactions = readLongValueAndAssert( metricsFile,
                ( newValue, currentValue ) -> newValue >= currentValue );

        // THEN
        assertThat( committedTransactions, lessThanOrEqualTo( 1001L ) );
    }

    @Test
    public void shouldShowClusterMetricsWhenMetricsEnabled() throws Throwable
    {
        // GIVEN
        // Create some activity that will show up in the metrics data.
        addNodes( 1000 );
        File metricsFile = metricsCsv( outputPath, ClusterMetrics.IS_MASTER );

        // WHEN
        // We should at least have a "timestamp" column, and a "neo4j.transaction.committed" column
        long committedTransactions = readLongValueAndAssert( metricsFile,
                ( newValue, currentValue ) -> newValue >= currentValue );

        // THEN
        assertThat( committedTransactions, equalTo( 1L ) );
    }

    @Test
    public void showReplanEvents() throws Throwable
    {
        // GIVEN
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "match (n:Label {name: 'Pontus'}) return n.name" ).close();
            tx.success();
        }

        //add some data, should make plan stale
        addNodes( 10 );

        // WHEN
        for ( int i = 0; i < 10; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.execute( "match (n:Label {name: 'Pontus'}) return n.name" ).close();
                tx.success();
            }
            addNodes( 1 );
        }

        File replanCountMetricFile = metricsCsv( outputPath, CypherMetrics.REPLAN_EVENTS );
        File replanWaitMetricFile = metricsCsv( outputPath, CypherMetrics.REPLAN_WAIT_TIME );

        // THEN see that the replan metric have pickup up at least one replan event
        // since reporting happens in an async fashion then give it some time and check now and then
        long endTime = currentTimeMillis() + TimeUnit.SECONDS.toMillis( 10 );
        long events = 0;
        while ( currentTimeMillis() < endTime && events == 0 )
        {
            readLongValueAndAssert( replanWaitMetricFile, ( newValue, currentValue ) -> newValue >= currentValue );
            events = readLongValueAndAssert( replanCountMetricFile, ( newValue, currentValue ) -> newValue >= currentValue );
            if ( events == 0 )
            {
                Thread.sleep( 300 );
            }
        }
        assertThat( events, greaterThan( 0L ) );
    }

    @Test
    public void shouldUseEventBasedReportingCorrectly() throws Throwable
    {
        // GIVEN
        addNodes( 100 );

        // WHEN
        CheckPointer checkPointer = db.getDependencyResolver().resolveDependency( CheckPointer.class );
        checkPointer.checkPointIfNeeded( new SimpleTriggerInfo( "test" ) );

        // wait for the file to be written before shutting down the cluster
        File metricFile = metricsCsv( outputPath, CheckPointingMetrics.CHECK_POINT_DURATION );

        long result = readLongValueAndAssert( metricFile, ( newValue, currentValue ) -> newValue >= 0 );

        // THEN
        assertThat( result, greaterThanOrEqualTo( 0L ) );
    }

    @Test
    public void shouldShowMetricsForThreads() throws Throwable
    {
        // WHEN
        addNodes( 100 );

        // wait for the file to be written before shutting down the cluster
        File threadTotalFile = metricsCsv( outputPath, ThreadMetrics.THREAD_TOTAL );
        File threadCountFile = metricsCsv( outputPath, ThreadMetrics.THREAD_COUNT );

        long threadTotalResult = readLongValueAndAssert( threadTotalFile, ( newValue, currentValue ) -> newValue >= 0 );
        long threadCountResult = readLongValueAndAssert( threadCountFile, ( newValue, currentValue ) -> newValue >= 0 );

        // THEN
        assertThat( threadTotalResult, greaterThanOrEqualTo( 0L ) );
        assertThat( threadCountResult, greaterThanOrEqualTo( 0L ) );
    }

    @Test
    public void mustBeAbleToStartWithNullTracer()
    {
        // Start the database
        File disabledTracerDb = clusterRule.directory( "disabledTracerDb" );
        GraphDatabaseBuilder builder = new EnterpriseGraphDatabaseFactory().newEmbeddedDatabaseBuilder( disabledTracerDb );
        GraphDatabaseService nullTracerDatabase =
                builder.setConfig( MetricsSettings.neoEnabled, Settings.TRUE ).setConfig( csvEnabled, Settings.TRUE )
                        .setConfig( csvPath, outputPath.getAbsolutePath() )
                        .setConfig( GraphDatabaseFacadeFactory.Configuration.tracer, "null" ) // key point!
                        .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                        .newGraphDatabase();
        try ( Transaction tx = nullTracerDatabase.beginTx() )
        {
            Node node = nullTracerDatabase.createNode();
            node.setProperty( "all", "is well" );
            tx.success();
        }
        finally
        {
            nullTracerDatabase.shutdown();
        }
        // We assert that no exception is thrown during startup or the operation of the database.
    }

    private void addNodes( int numberOfNodes )
    {
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( Label.label( "Label" ) );
                node.setProperty( "name", UUID.randomUUID().toString() );
                tx.success();
            }
        }
    }
}
