/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import java.time.Clock;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThreshold.or;

/**
 * The {@code periodic} check point threshold policy uses the {@link GraphDatabaseSettings#check_point_interval_time}
 * and {@link GraphDatabaseSettings#check_point_interval_tx} to decide when check points processes should be started.
 */
@Service.Implementation( CheckPointThresholdPolicy.class )
public class PeriodicThresholdPolicy extends CheckPointThresholdPolicy
{
    public PeriodicThresholdPolicy()
    {
        super( "periodic" );
    }

    @Override
    public CheckPointThreshold createThreshold(
            Config config, Clock clock, LogPruning logPruning, LogProvider logProvider )
    {
        int txThreshold = config.get( GraphDatabaseSettings.check_point_interval_tx );
        final CountCommittedTransactionThreshold countCommittedTransactionThreshold =
                new CountCommittedTransactionThreshold( txThreshold );

        long timeMillisThreshold = config.get( GraphDatabaseSettings.check_point_interval_time ).toMillis();
        TimeCheckPointThreshold timeCheckPointThreshold = new TimeCheckPointThreshold( timeMillisThreshold, clock );

        return or( countCommittedTransactionThreshold, timeCheckPointThreshold );
    }
}
