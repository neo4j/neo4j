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
package org.neo4j.metrics;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiPredicate;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.metrics.source.cluster.ClusterMetrics;
import org.neo4j.metrics.source.db.CheckPointingMetrics;
import org.neo4j.metrics.source.db.CypherMetrics;
import org.neo4j.metrics.source.db.EntityCountMetrics;
import org.neo4j.metrics.source.db.TransactionMetrics;
import org.neo4j.test.ha.ClusterRule;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.check_point_interval_time;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.cypher_min_replan_interval;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.ha.ClusterManager.clusterOfSize;
import static org.neo4j.metrics.MetricsSettings.csvEnabled;
import static org.neo4j.metrics.MetricsSettings.csvPath;
import static org.neo4j.metrics.MetricsSettings.graphiteInterval;
import static org.neo4j.metrics.MetricsSettings.metricsEnabled;
import static org.neo4j.test.Assert.assertEventually;

public class MetricsKernelExtensionFactoryIT
{
    private static final int TIME_STAMP = 0;
    private static final int METRICS_VALUE = 1;

    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() );

    private File outputPath;
    private HighlyAvailableGraphDatabase db;

    @Before
    public void setup() throws Throwable
    {
        outputPath = clusterRule.directory( "metrics" );
        Map<String,String> config = stringMap(
                MetricsSettings.neoEnabled.name(), Settings.TRUE,
                metricsEnabled.name(), Settings.TRUE,
                csvEnabled.name(), Settings.TRUE,
                cypher_min_replan_interval.name(), "0m",
                csvPath.name(), outputPath.getAbsolutePath(),
                check_point_interval_time.name(), "100ms",
                graphiteInterval.name(), "1s"
        );
        db = clusterRule.withSharedConfig( config ).withProvider( clusterOfSize( 1 ) ).startCluster().getMaster();
    }

    @Test
    public void shouldShowTxCommittedMetricsWhenMetricsEnabled() throws Throwable
    {
        // GIVEN
        long lastCommittedTransactionId = db.getDependencyResolver().resolveDependency( NeoStores.class )
                .getMetaDataStore().getLastCommittedTransactionId();

        // Create some activity that will show up in the metrics data.
        addNodes( 1000 );
        File metricsFile = new File( outputPath, TransactionMetrics.TX_COMMITTED + ".csv" );

        // WHEN
        // We should at least have a "timestamp" column, and a "neo4j.transaction.committed" column
        long committedTransactions = readLongValueAndAssert( metricsFile,
                ( newValue, currentValue ) -> newValue >= currentValue );

        // THEN
        assertThat( committedTransactions, greaterThanOrEqualTo( lastCommittedTransactionId ) );
        assertThat( committedTransactions, lessThanOrEqualTo( lastCommittedTransactionId + 1000L ) );
    }

    @Test
    public void shouldShowEntityCountMetricsWhenMetricsEnabled() throws Throwable
    {
        // GIVEN
        // Create some activity that will show up in the metrics data.
        addNodes( 1000 );
        File metricsFile = new File( outputPath, EntityCountMetrics.COUNTS_NODE + ".csv" );

        // WHEN
        // We should at least have a "timestamp" column, and a "neo4j.transaction.committed" column
        long committedTransactions = readLongValueAndAssert( metricsFile,
                ( newValue, currentValue ) -> newValue >= currentValue );

        // THEN
        assertThat( committedTransactions, lessThanOrEqualTo( 1000L ) );
    }

    @Test
    public void shouldShowClusterMetricsWhenMetricsEnabled() throws Throwable
    {
        // GIVEN
        // Create some activity that will show up in the metrics data.
        addNodes( 1000 );
        File metricsFile = new File( outputPath, ClusterMetrics.IS_MASTER + ".csv" );

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
            db.execute( "match (n:Label {name: 'Pontus'}) return n.name" );
            tx.success();
        }
        //add some data, should make plan stale
        addNodes( 10 );
        File metricFile = new File( outputPath, CypherMetrics.REPLAN_EVENTS + ".csv" );

        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "match (n:Label {name: 'Pontus'}) return n.name" );
            tx.success();
        }

        long events = readLongValueAndAssert( metricFile, ( newValue, currentValue ) -> newValue >= currentValue );

        // THEN
        assertThat( events, is( 1L ) );
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
        File metricFile = new File( outputPath, CheckPointingMetrics.CHECK_POINT_DURATION + ".csv" );

        long result = readLongValueAndAssert( metricFile, ( newValue, currentValue ) -> newValue > 0 );

        // THEN
        assertThat( result, greaterThanOrEqualTo( 0L ) );
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

    private long readLongValueAndAssert( File metricFile, BiPredicate<Integer,Integer> assumption ) throws Throwable
    {
        // let's wait until the file is in place (since the reporting is async that might take a while)
        assertEventually( "Metrics file should exist", metricFile::exists, is( true ), 20, SECONDS );

        try ( BufferedReader reader = new BufferedReader( new FileReader( metricFile ) ) )
        {
            String[] headers = reader.readLine().split( "," );
            assertThat( headers.length, is( 2 ) );
            assertThat( headers[TIME_STAMP], is( "t" ) );
            assertThat( headers[METRICS_VALUE], is( "value" ) );

            // Now we can verify that the number of committed transactions should never decrease.
            int currentValue = 0;
            String line;
            while ( (line = reader.readLine()) != null )
            {
                String[] fields = line.split( "," );
                int newValue = Integer.parseInt( fields[1] );
                assertTrue( "assertion failed on " + newValue + " " + currentValue,
                        assumption.test( newValue, currentValue ) );
                currentValue = newValue;
            }
            return currentValue;
        }
    }
}
