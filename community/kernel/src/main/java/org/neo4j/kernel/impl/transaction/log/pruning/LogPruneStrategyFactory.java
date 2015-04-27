/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log.pruning;

import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.Clock;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.IllegalLogFormatException;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;

import static org.neo4j.kernel.configuration.Config.parseLongWithUnit;

public class LogPruneStrategyFactory
{
    public static final LogPruneStrategy NO_PRUNING = new LogPruneStrategy()
    {
        @Override
        public void prune()
        {
            // do nothing
        }

        @Override
        public String toString()
        {
            return "NO_PRUNING";
        }
    };

    static boolean decidePruneForIllegalLogFormat( IllegalLogFormatException e )
    {
        if ( e.wasNewerLogVersion() )
        {
            throw new RuntimeException( "Unable to read database logs, because it contains" +
                    " logs from a newer version of Neo4j.", e );
        }

        // Hit an old version log, consider this out of date.
        return true;
    }

    /**
     * Parses a configuration value for log specifying log pruning. It has one of these forms:
     * <ul>
     *   <li>all</li>
     *   <li>[number][unit] [type]</li>
     * </ul>
     * For example:
     * <ul>
     *   <li>100M size - For keeping last 100 megabytes of log data</li>
     *   <li>20 pcs - For keeping last 20 non-empty log files</li>
     *   <li>7 days - For keeping last 7 days worth of log data</li>
     *   <li>1k hours - For keeping last 1000 hours worth of log data</li>
     * </ul>
     */
    public static LogPruneStrategy fromConfigValue( FileSystemAbstraction fileSystem,
                                                    LogFileInformation logFileInformation,
                                                    PhysicalLogFiles files,
                                                    LogVersionRepository versionRepo,
                                                    String configValue )
    {
        String[] tokens = configValue.split( " " );
        if ( tokens.length == 0 )
        {
            throw new IllegalArgumentException( "Invalid log pruning configuration value '" + configValue + "'" );
        }

        final String boolOrNumber = tokens[0];

        if ( tokens.length == 1 )
        {
            switch ( boolOrNumber )
            {
                case "true":
                    return NO_PRUNING;
                case "false":
                    final TransactionCountThreshold thresholdToUse = new TransactionCountThreshold( 1 );
                    return new ThresholdBasedPruneStrategy( fileSystem, logFileInformation, files, versionRepo,
                            thresholdToUse );
                default:
                    throw new IllegalArgumentException( "Invalid log pruning configuration value '" + configValue +
                            "'. The form is 'all' or '<number><unit> <type>' for example '100k txs' " +
                            "for the latest 100 000 transactions" );
            }
        }

        String type = tokens[1];
        int number = (int) parseLongWithUnit( boolOrNumber );

        Threshold thresholdToUse;
        switch ( type )
        {
            case "files":
                thresholdToUse = new FileCountThreshold( number );
                break;
            case "size":
                thresholdToUse = new FileSizeThreshold( fileSystem, number );
                break;
            case "txs":
                thresholdToUse = new TransactionCountThreshold( number );
                break;
            case "hours":
                thresholdToUse = new TransactionTimespanThreshold( Clock.SYSTEM_CLOCK, TimeUnit.HOURS, number );
                break;
            case "days":
                thresholdToUse = new TransactionTimespanThreshold( Clock.SYSTEM_CLOCK, TimeUnit.DAYS, number );
                break;
            default:
                throw new IllegalArgumentException( "Invalid log pruning configuration value '" + configValue +
                        "'. Invalid type '" + type + "', valid are files, size, txs, hours, days." );
        }
        return new ThresholdBasedPruneStrategy( fileSystem, logFileInformation, files, versionRepo, thresholdToUse );
    }

}
