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
package org.neo4j.causalclustering.stresstests;

import java.util.Map;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.kernel.configuration.Settings;

class ClusterConfiguration
{
    private ClusterConfiguration()
    {
        // no instances
    }

    static Map<String,String> enableRaftMessageLogging( Map<String,String> settings )
    {
        settings.put( CausalClusteringSettings.raft_messages_log_enable.name(), Settings.TRUE );
        return settings;
    }

    static Map<String,String> configureRaftLogRotationAndPruning( Map<String,String> settings )
    {
        settings.put( CausalClusteringSettings.raft_log_rotation_size.name(), "1K" );
        settings.put( CausalClusteringSettings.raft_log_pruning_frequency.name(), "250ms" );
        settings.put( CausalClusteringSettings.raft_log_pruning_strategy.name(), "keep_none" );
        return settings;
    }
}
