/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cluster;

import static org.neo4j.graphdb.factory.GraphDatabaseSetting.ANY;
import static org.neo4j.graphdb.factory.GraphDatabaseSetting.TRUE;
import static org.neo4j.helpers.Settings.BOOLEAN;
import static org.neo4j.helpers.Settings.DURATION;
import static org.neo4j.helpers.Settings.HOSTNAME_PORT;
import static org.neo4j.helpers.Settings.INTEGER;
import static org.neo4j.helpers.Settings.MANDATORY;
import static org.neo4j.helpers.Settings.STRING;
import static org.neo4j.helpers.Settings.illegalValueMessage;
import static org.neo4j.helpers.Settings.list;
import static org.neo4j.helpers.Settings.matches;
import static org.neo4j.helpers.Settings.setting;

import java.util.List;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.helpers.HostnamePort;

/**
 * Settings for cluster members
 */
public class ClusterSettings
{
    @Description( "Id for a cluster instance. Must be unique within the cluster" )
    public static final Setting<Integer> server_id = setting( "ha.server_id", INTEGER, MANDATORY );

    @Description( "The name of a cluster" )
    public static final Setting<String> cluster_name = setting( "ha.cluster_name", STRING, "neo4j.ha",
            illegalValueMessage( "Must be a valid cluster name" , matches( ANY ) ) );

    @Description( "This is the list of potential cluster members" )
    public static final Setting<List<HostnamePort>> initial_hosts = setting( "ha.initial_hosts",
            list( ",", HOSTNAME_PORT ), MANDATORY );

    @Description( "Host name and port to use for the cluster server" )
    public static final Setting<HostnamePort> cluster_server = setting( "ha.cluster_server", HOSTNAME_PORT,
            "0.0.0.0:5001-5099" );

    @Description( "Whether to allow this instance to create a cluster if unable to join" )
    public static final Setting<Boolean> allow_init_cluster = setting( "ha.allow_init_cluster", BOOLEAN, TRUE );

    // Timeout settings
    @Description( "Default timeout used for clustering timeouts. Override with specific timeouts if necessary" )
    public static final Setting<Long> default_timeout = setting( "ha.default_timeout", DURATION, "5s" );

    @Description( "Timeout for heartbeats between cluster members. Should be at least twice that of the interval" )
    public static final Setting<Long> heartbeat_timeout = setting( "ha.heartbeat_timeout", DURATION, "11s" );

    @Description( "How often heartbeat messages should be sent" )
    public static final Setting<Long> heartbeat_interval = setting( "ha.heartbeat_interval", DURATION,
            default_timeout );

    @Description( "Timeout for broadcasting values in cluster. Must consider end-to-end duration of Paxos algorithm" )
    public static final Setting<Long> broadcast_timeout = setting( "ha.broadcast_timeout", DURATION, "30s" );

    @Description( "Timeout for joining a cluster" )
    public static final Setting<Long> join_timeout = setting( "ha.join_timeout", DURATION, broadcast_timeout );

    @Description( "Timeout for waiting for configuration from an existing cluster member" )
    public static final Setting<Long> configuration_timeout = setting( "ha.configuration_timeout", DURATION, "1s" );

    @Description( "Timeout for waiting for cluster leave to finish" )
    public static final Setting<Long> leave_timeout = setting( "ha.leave_timeout", DURATION, broadcast_timeout );

    @Description( "Default timeout for all Paxos timeouts" )
    public static final Setting<Long> paxos_timeout = setting( "ha.paxos_timeout", DURATION, default_timeout );

    @Description( "Timeout for Paxos phase 1" )
    public static final Setting<Long> phase1_timeout = setting( "ha.phase1_timeout", DURATION, paxos_timeout );

    @Description( "Timeout for Paxos phase 2" )
    public static final Setting<Long> phase2_timeout = setting( "ha.phase2_timeout", DURATION, paxos_timeout );

    @Description( "Timeout for learning values" )
    public static final Setting<Long> learn_timeout = setting( "ha.learn_timeout", DURATION, default_timeout );

    @Description( "Timeout for waiting for other members to finish a role election" )
    public static final Setting<Long> election_timeout = setting( "ha.election_timeout", DURATION, paxos_timeout );
}
