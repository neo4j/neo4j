/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.impl.enterprise.transaction.log.checkpoint;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThresholdPolicy;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.logging.LogProvider;
import org.neo4j.time.SystemNanoClock;

@Service.Implementation( CheckPointThresholdPolicy.class )
public class ContinuousThresholdPolicy extends CheckPointThresholdPolicy
{
    public ContinuousThresholdPolicy()
    {
        super( "continuous" );
    }

    @Override
    public CheckPointThreshold createThreshold(
            Config config, SystemNanoClock clock, LogPruning logPruning, LogProvider logProvider )
    {
        return new ContinuousCheckPointThreshold();
    }
}
