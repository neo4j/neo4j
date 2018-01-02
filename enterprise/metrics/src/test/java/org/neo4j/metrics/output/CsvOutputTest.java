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
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.spi.SimpleKernelContext;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.logging.NullLog;
import org.neo4j.metrics.MetricsSettings;
import org.neo4j.test.TargetDirectory;
import org.neo4j.udc.UsageDataKeys;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class CsvOutputTest
{
    @Rule
    public final LifeRule life = new LifeRule();
    @Rule
    public final TargetDirectory.TestDirectory directory = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldHaveMetricsCsvPathEndUpRelativeToGraphDbDirectoryIfRelativePathSpecified() throws Exception
    {
        // GIVEN
        File storeDir = directory.directory();
        String name = "metrics.csv";
        CsvOutput output = life.add( new CsvOutput( config(
                MetricsSettings.csvEnabled.name(), "true",
                MetricsSettings.csvInterval.name(), "10ms",
                MetricsSettings.csvPath.name(), name ),
                new MetricRegistry(), NullLog.getInstance(), kerneContext( storeDir ) ) );

        // WHEN
        life.start();

        // THEN
        waitForFileToAppear( new File( storeDir, name ) );
    }

    @Test
    public void shouldHaveRelativeMetricsCsvPathBeRelativeToGraphDbDirectory() throws Exception
    {
        // GIVEN
        File storeDir = directory.directory();
        CsvOutput output = life.add( new CsvOutput( config(
                MetricsSettings.csvEnabled.name(), "true",
                MetricsSettings.csvInterval.name(), "10ms",
                MetricsSettings.csvPath.name(), "test.csv" ),
                new MetricRegistry(), NullLog.getInstance(), kerneContext( storeDir ) ) );

        // WHEN
        life.start();

        // THEN
        waitForFileToAppear( new File( storeDir, "test.csv" ) );
    }

    @Test
    public void shouldHaveAbsoluteMetricsCsvPathBeAbsolute() throws Exception
    {
        // GIVEN
        File storeDir = directory.directory();
        File file = File.createTempFile( "neo4j", "csvoutput" );
        CsvOutput output = life.add( new CsvOutput( config(
                MetricsSettings.csvEnabled.name(), "true",
                MetricsSettings.csvInterval.name(), "10ms",
                MetricsSettings.csvPath.name(), file.getAbsolutePath() ),
                new MetricRegistry(), NullLog.getInstance(), kerneContext( storeDir ) ) );

        // WHEN
        life.start();

        // THEN
        waitForFileToAppear( file );
    }

    private KernelContext kerneContext( final File storeDir )
    {
        return new SimpleKernelContext( new DefaultFileSystemAbstraction(), storeDir, UsageDataKeys.OperationalMode.single );
    }

    private Config config( String... keysValues )
    {
        return new Config( stringMap( keysValues ) );
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
