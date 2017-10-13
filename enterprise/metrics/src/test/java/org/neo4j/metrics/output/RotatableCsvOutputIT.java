/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.metrics.output;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.metrics.source.db.TransactionMetrics;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.neo4j.metrics.MetricsSettings.csvPath;
import static org.neo4j.metrics.MetricsSettings.csvRotationThreshold;
import static org.neo4j.metrics.MetricsTestHelper.readLongValueAndAssert;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class RotatableCsvOutputIT
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    private File outputPath;
    private GraphDatabaseService database;

    @Before
    public void setup() throws Throwable
    {
        outputPath = testDirectory.directory( "metrics" );
        database = new EnterpriseGraphDatabaseFactory().newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() )
                .setConfig( csvPath, outputPath.getAbsolutePath() )
                .setConfig( csvRotationThreshold, "21" )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .newGraphDatabase();
    }

    @After
    public void tearDown() throws Exception
    {
        database.shutdown();
    }

    @Test
    public void rotateMetricsFile() throws InterruptedException, IOException
    {
        try ( Transaction transaction = database.beginTx() )
        {
            database.createNode();
            transaction.success();
        }
        File metricsFile = metricsCsv( outputPath, TransactionMetrics.TX_COMMITTED );
        long committedTransactions = readLongValueAndAssert( metricsFile,
                ( newValue, currentValue ) -> newValue >= currentValue );
        assertEquals( 1, committedTransactions );

        File metricsFile2 = metricsCsv( outputPath, TransactionMetrics.TX_COMMITTED, 1 );
        try ( Transaction transaction = database.beginTx() )
        {
            database.createNode();
            transaction.success();
        }
        long oldCommittedTransactions = readLongValueAndAssert( metricsFile2,
                ( newValue, currentValue ) -> newValue >= currentValue );
        assertEquals( 1, oldCommittedTransactions );
    }

    private static File metricsCsv( File dbDir, String metric ) throws InterruptedException
    {
        return metricsCsv( dbDir, metric, 0 );
    }

    private static File metricsCsv( File dbDir, String metric, long index ) throws InterruptedException
    {
        File csvFile = new File( dbDir, index > 0 ? metric + ".csv." + index : metric + ".csv" );
        assertEventually( "Metrics file should exist", csvFile::exists, is( true ), 40, SECONDS );
        return csvFile;
    }
}
