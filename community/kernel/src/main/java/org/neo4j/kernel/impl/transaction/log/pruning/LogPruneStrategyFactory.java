/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.pruning.ThresholdConfigParser.ThresholdConfigValue;
import org.neo4j.time.Clocks;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.neo4j.kernel.impl.transaction.log.pruning.ThresholdConfigParser.parse;

public class LogPruneStrategyFactory
{
    public static final LogPruneStrategy NO_PRUNING = new LogPruneStrategy()
    {
        @Override
        public void prune( long upToLogVersion )
        {
            // do nothing
        }

        @Override
        public String toString()
        {
            return "NO_PRUNING";
        }
    };

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
                                                    String configValue )
    {
        ThresholdConfigValue value = parse( configValue );

        if ( value == ThresholdConfigValue.NO_PRUNING )
        {
            return NO_PRUNING;
        }

        Threshold thresholdToUse = getThresholdByType( fileSystem, value, configValue );
        return new ThresholdBasedPruneStrategy( fileSystem, logFileInformation, files, thresholdToUse );
    }

    // visible for testing
    static Threshold getThresholdByType( FileSystemAbstraction fileSystem, ThresholdConfigValue value,
            String originalConfigValue )
    {
        long thresholdValue = value.value;

        switch ( value.type )
        {
            case "files":
                return new FileCountThreshold( thresholdValue );
            case "size":
                return new FileSizeThreshold( fileSystem, thresholdValue );
            case "txs":
            case "entries": // txs and entries are synonyms
                return new EntryCountThreshold( thresholdValue );
            case "hours":
                return new EntryTimespanThreshold( Clocks.systemClock(), HOURS, thresholdValue );
            case "days":
                return new EntryTimespanThreshold( Clocks.systemClock(), DAYS, thresholdValue );
            default:
                throw new IllegalArgumentException( "Invalid log pruning configuration value '" + originalConfigValue +
                        "'. Invalid type '" + value.type + "', valid are files, size, txs, entries, hours, days." );
        }
    }
}
