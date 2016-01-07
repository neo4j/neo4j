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
package org.neo4j.metrics.output;

import com.codahale.metrics.MetricRegistry;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.spi.SimpleKernelContext;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.logging.NullLog;
import org.neo4j.metrics.MetricsSettings;
import org.neo4j.test.TargetDirectory;

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

    private File storeDir;
    private KernelContext kernelContext;

    @Before
    public void setup()
    {
        storeDir = directory.directory();
        kernelContext = new SimpleKernelContext( new DefaultFileSystemAbstraction(), storeDir, DatabaseInfo.UNKNOWN );
    }

    @Test
    public void shouldHaveMetricsCsvPathEndUpRelativeToGraphDbDirectoryIfRelativePathSpecified() throws Exception
    {
        // GIVEN
        String name = "metrics.csv";
        Config config = config(
                MetricsSettings.csvEnabled.name(), "true",
                MetricsSettings.csvInterval.name(), "10ms",
                MetricsSettings.csvPath.name(), name );
        life.add( new CsvOutput( config, new MetricRegistry(), NullLog.getInstance(), kernelContext ) );

        // WHEN
        life.start();

        // THEN
        waitForFileToAppear( new File( storeDir, name ) );
    }

    @Test
    public void shouldHaveRelativeMetricsCsvPathBeRelativeToGraphDbDirectory() throws Exception
    {
        // GIVEN
        Config config = config(
                MetricsSettings.csvEnabled.name(), "true",
                MetricsSettings.csvInterval.name(), "10ms",
                MetricsSettings.csvPath.name(), "test.csv" );
        life.add( new CsvOutput( config, new MetricRegistry(), NullLog.getInstance(), kernelContext ) );

        // WHEN
        life.start();

        // THEN
        waitForFileToAppear( new File( storeDir, "test.csv" ) );
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
        life.add( new CsvOutput( config, new MetricRegistry(), NullLog.getInstance(), kernelContext ) );

        // WHEN
        life.start();

        // THEN
        waitForFileToAppear( outputFPath );
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
