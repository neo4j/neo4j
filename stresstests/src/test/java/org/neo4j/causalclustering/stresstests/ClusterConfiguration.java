/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.stresstests;

import java.util.Map;
import java.util.function.IntFunction;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.configuration.Settings;

import static org.neo4j.kernel.configuration.Settings.TRUE;

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

    static Map<String,IntFunction<String>> configureBackup( Map<String,IntFunction<String>> settings,
            IntFunction<SocketAddress> address )
    {
        settings.put( OnlineBackupSettings.online_backup_enabled.name(), id -> TRUE );
        settings.put( OnlineBackupSettings.online_backup_server.name(), id -> address.apply( id ).toString() );
        return settings;
    }
}
