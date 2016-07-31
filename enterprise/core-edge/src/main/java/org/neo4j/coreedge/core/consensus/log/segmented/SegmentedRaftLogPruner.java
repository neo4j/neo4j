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
package org.neo4j.coreedge.core.consensus.log.segmented;


import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.impl.transaction.log.pruning.ThresholdConfigParser.ThresholdConfigValue;
import static org.neo4j.kernel.impl.transaction.log.pruning.ThresholdConfigParser.parse;

public class SegmentedRaftLogPruner
{
    private final ThresholdConfigValue parsedConfigOption;
    private final CoreLogPruningStrategy pruneStrategy;

    public SegmentedRaftLogPruner( String pruningStrategyConfig, LogProvider logProvider )
    {
        parsedConfigOption = parse( pruningStrategyConfig );
        pruneStrategy = getPruneStrategy( parsedConfigOption, logProvider );
    }

    private CoreLogPruningStrategy getPruneStrategy( ThresholdConfigValue configValue, LogProvider logProvider )
    {
        switch ( configValue.type )
        {
        case "size":
            return new SizeBasedLogPruningStrategy( parsedConfigOption.value );
        case "txs":
        case "entries": // txs and entries are synonyms
            return new EntryBasedLogPruningStrategy( parsedConfigOption.value, logProvider );
        case "hours": // hours and days are currently not supported as such, default to no prune
        case "days":
        case "false":
            return new NoPruningPruningStrategy();
        default:
            throw new IllegalArgumentException( "Invalid log pruning configuration value '" + configValue.value +
                    "'. Invalid type '" + configValue.type + "', valid are files, size, txs, entries, hours, days." );
        }
    }

    public long getIndexToPruneFrom( long safeIndex, Segments segments )
    {
        return Math.min( safeIndex, pruneStrategy.getIndexToKeep( segments ) );
    }
}
