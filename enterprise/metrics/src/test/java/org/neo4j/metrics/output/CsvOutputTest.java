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
package org.neo4j.metrics.output;

import com.codahale.metrics.MetricRegistry;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.nio.file.Files;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.spi.SimpleKernelContext;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.logging.NullLog;
import org.neo4j.metrics.MetricsSettings;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class CsvOutputTest
{
    private final LifeRule life = new LifeRule();
    private final TestDirectory directory = TestDirectory.testDirectory();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final JobScheduler jobScheduler = new OnDemandJobScheduler();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( directory ).around( fileSystemRule ).around( life );
    private KernelContext kernelContext;

    @Before
    public void setup()
    {
        File storeDir = directory.directory();
        kernelContext = new SimpleKernelContext( storeDir, DatabaseInfo.UNKNOWN, new Dependencies() );
    }

    @Test
    public void shouldHaveRelativeMetricsCsvPathBeRelativeToNeo4jHome() throws Exception
    {
        // GIVEN
        File home = directory.absolutePath();
        Config config = config(
                MetricsSettings.csvEnabled.name(), "true",
                MetricsSettings.csvInterval.name(), "10ms",
                MetricsSettings.csvPath.name(), "the-metrics-dir",
                GraphDatabaseSettings.neo4j_home.name(), home.getAbsolutePath() );
        life.add( createCsvOutput( config ) );

        // WHEN
        life.start();

        // THEN
        waitForFileToAppear( new File( home, "the-metrics-dir" ) );
    }

    @Test
    public void shouldHaveAbsoluteMetricsCsvPathBeAbsolute() throws Exception
    {
        // GIVEN
        File outputFPath = Files.createTempDirectory( "output" ).toFile();
        Config config = config(
                MetricsSettings.csvEnabled.name(), "true",
                MetricsSettings.csvInterval.name(), "10ms",
                MetricsSettings.csvPath.name(), outputFPath.getAbsolutePath() );
        life.add( createCsvOutput( config ) );

        // WHEN
        life.start();

        // THEN
        waitForFileToAppear( outputFPath );
    }

    private CsvOutput createCsvOutput( Config config )
    {
        return new CsvOutput( config, new MetricRegistry(), NullLog.getInstance(), kernelContext, fileSystemRule, jobScheduler );
    }

    private Config config( String... keysValues )
    {
        return Config.defaults( stringMap( keysValues ) );
    }

    private void waitForFileToAppear( File file ) throws InterruptedException
    {
        long end = currentTimeMillis() + SECONDS.toMillis( 10 );
        while ( !file.exists() )
        {
            Thread.sleep( 10 );
            if ( currentTimeMillis() > end )
            {
                fail( file + " didn't appear" );
            }
        }
    }
}
