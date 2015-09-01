/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.metrics.MetricsSettings;

import static org.neo4j.metrics.MetricsSettings.csvEnabled;
import static org.neo4j.metrics.MetricsSettings.csvInterval;
import static org.neo4j.metrics.MetricsSettings.csvPath;

public class CsvOutput implements Closeable
{
    private ScheduledReporter csvReporter;

    public CsvOutput( Config config, MetricRegistry registry, Log logger )
    {
        if ( config.get( csvEnabled ) )
        {
            // Setup CSV reporting
            switch ( config.get( MetricsSettings.csvFile ) )
            {
            case single:
            {
                csvReporter = CsvReporterSingle.forRegistry( registry )
                        .convertRatesTo( TimeUnit.SECONDS )
                        .convertDurationsTo( TimeUnit.MILLISECONDS )
                        .filter( MetricFilter.ALL )
                        .build( config.get( csvPath ) );
                break;
            }

            case split:
            {
                if ( !config.get( csvPath ).exists() )
                {
                    if ( !config.get( csvPath ).mkdirs() )
                    {
                        throw new IllegalStateException( "Could not create path for CSV files:" + config.get(
                                csvPath ).getAbsolutePath() );
                    }
                }

                csvReporter = CsvReporter.forRegistry( registry )
                        .convertRatesTo( TimeUnit.SECONDS )
                        .convertDurationsTo( TimeUnit.MILLISECONDS )
                        .filter( MetricFilter.ALL )
                        .build( config.get( csvPath ) );
                break;
            }
            }

            // Start CSV reporter
            csvReporter.start( config.get( csvInterval ), TimeUnit.MILLISECONDS );

            logger.info( "Sending metrics to CSV file at " + config.get( csvPath ) );
        }
    }

    public void close() throws IOException
    {
        if ( csvReporter != null )
        {
            csvReporter.stop();
            csvReporter = null;
        }
    }
}
