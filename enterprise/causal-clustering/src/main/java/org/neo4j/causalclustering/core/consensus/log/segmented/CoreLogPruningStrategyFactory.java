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
package org.neo4j.causalclustering.core.consensus.log.segmented;

import org.neo4j.function.Factory;
import org.neo4j.kernel.impl.transaction.log.pruning.ThresholdConfigParser;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.impl.transaction.log.pruning.ThresholdConfigParser.parse;

public class CoreLogPruningStrategyFactory implements Factory<CoreLogPruningStrategy>
{
    private final String pruningStrategyConfig;
    private final LogProvider logProvider;

    public CoreLogPruningStrategyFactory( String pruningStrategyConfig, LogProvider logProvider )
    {
        this.pruningStrategyConfig = pruningStrategyConfig;
        this.logProvider = logProvider;
    }

    @Override
    public CoreLogPruningStrategy newInstance()
    {
        ThresholdConfigParser.ThresholdConfigValue thresholdConfigValue = parse( pruningStrategyConfig );

        String type = thresholdConfigValue.type;
        long value = thresholdConfigValue.value;
        switch ( type )
        {
        case "size":
            return new SizeBasedLogPruningStrategy( value );
        case "txs":
        case "entries": // txs and entries are synonyms
            return new EntryBasedLogPruningStrategy( value, logProvider );
        case "hours": // hours and days are currently not supported as such, default to no prune
        case "days":
            throw new IllegalArgumentException(
                    "Time based pruning not supported yet for the segmented raft log, got '" + type + "'." );
        case "false":
            return new NoPruningPruningStrategy();
        default:
            throw new IllegalArgumentException( "Invalid log pruning configuration value '" + value +
                    "'. Invalid type '" + type + "', valid are files, size, txs, entries, hours, days." );
        }
    }
}
