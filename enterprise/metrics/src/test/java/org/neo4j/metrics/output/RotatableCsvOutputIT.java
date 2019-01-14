/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.metrics.output;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.function.BiPredicate;

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
import static org.junit.Assert.fail;
import static org.neo4j.metrics.MetricsSettings.csvMaxArchives;
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
    private static final BiPredicate<Long,Long> MONOTONIC = ( newValue, currentValue ) -> newValue >= currentValue;
    private static final int MAX_ARCHIVES = 20;

    @Before
    public void setup()
    {
        outputPath = testDirectory.directory( "metrics" );
        database = new EnterpriseGraphDatabaseFactory().newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() )
                .setConfig( csvPath, outputPath.getAbsolutePath() )
                .setConfig( csvRotationThreshold, "21" )
                .setConfig( csvMaxArchives, String.valueOf( MAX_ARCHIVES ) )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .newGraphDatabase();
    }

    @After
    public void tearDown()
    {
        database.shutdown();
    }

    @Test
    public void rotateMetricsFile() throws InterruptedException, IOException
    {
        // Commit a transaction and wait for rotation to happen
        doTransaction();
        waitForRotation( outputPath, TransactionMetrics.TX_COMMITTED );

        // Latest file should now have recorded the transaction
        File metricsFile = metricsCsv( outputPath, TransactionMetrics.TX_COMMITTED );
        long committedTransactions = readLongValueAndAssert( metricsFile, MONOTONIC );
        assertEquals( 1, committedTransactions );

        // Commit yet another transaction and wait for rotation to happen again
        doTransaction();
        waitForRotation( outputPath, TransactionMetrics.TX_COMMITTED );

        // Latest file should now have recorded the new transaction
        File metricsFile2 = metricsCsv( outputPath, TransactionMetrics.TX_COMMITTED );
        long committedTransactions2 = readLongValueAndAssert( metricsFile2, MONOTONIC );
        assertEquals( 2, committedTransactions2 );
    }

    private void doTransaction()
    {
        try ( Transaction transaction = database.beginTx() )
        {
            database.createNode();
            transaction.success();
        }
    }

    private static void waitForRotation( File dbDir, String metric ) throws InterruptedException
    {
        // Find highest missing file
        int i = 0;
        while ( getMetricFile( dbDir, metric, i ).exists() )
        {
            i++;
        }

        if ( i >= MAX_ARCHIVES )
        {
            fail( "Test did not finish before " + MAX_ARCHIVES + " rotations, which means we have rotated away from the " +
                    "file we want to assert on." );
        }

        // wait for file to exists
        metricsCsv( dbDir, metric, i );
    }

    private static File metricsCsv( File dbDir, String metric ) throws InterruptedException
    {
        return metricsCsv( dbDir, metric, 0 );
    }

    private static File metricsCsv( File dbDir, String metric, long index ) throws InterruptedException
    {
        File csvFile = getMetricFile( dbDir, metric, index );
        assertEventually( "Metrics file should exist", csvFile::exists, is( true ), 40, SECONDS );
        return csvFile;
    }

    private static File getMetricFile( File dbDir, String metric, long index )
    {
        return new File( dbDir, index > 0 ? metric + ".csv." + index : metric + ".csv" );
    }
}
