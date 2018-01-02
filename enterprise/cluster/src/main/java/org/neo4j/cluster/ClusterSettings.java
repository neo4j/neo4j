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
package org.neo4j.cluster;

import java.util.List;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.HostnamePort;

import static org.neo4j.kernel.configuration.Settings.ANY;
import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.DURATION;
import static org.neo4j.kernel.configuration.Settings.HOSTNAME_PORT;
import static org.neo4j.kernel.configuration.Settings.MANDATORY;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.INTEGER;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.configuration.Settings.illegalValueMessage;
import static org.neo4j.kernel.configuration.Settings.list;
import static org.neo4j.kernel.configuration.Settings.matches;
import static org.neo4j.kernel.configuration.Settings.min;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * Settings for cluster members
 */
@Description( "Cluster configuration settings" )
public class ClusterSettings
{
    public static final Function<String, InstanceId> INSTANCE_ID = new Function<String, InstanceId>()
    {
        @Override
        public InstanceId apply( String value )
        {
            try
            {
                return new InstanceId( Integer.parseInt( value ) );
            }
            catch ( NumberFormatException e )
            {
                throw new IllegalArgumentException( "not a valid integer value" );
            }
        }

        @Override
        public String toString()
        {
            return "an instance id, which has to be a valid integer";
        }
    };

    @Description( "Id for a cluster instance. Must be unique within the cluster." )
    public static final Setting<InstanceId> server_id = setting( "ha.server_id", INSTANCE_ID, MANDATORY );

    @Description( "The name of a cluster." )
    public static final Setting<String> cluster_name = setting( "ha.cluster_name", STRING, "neo4j.ha",
            illegalValueMessage( "must be a valid cluster name", matches( ANY ) ) );

    @Description( "A comma-separated list of other members of the cluster to join." )
    public static final Setting<List<HostnamePort>> initial_hosts = setting( "ha.initial_hosts",
            list( ",", HOSTNAME_PORT ), MANDATORY );

    @Description( "Host and port to bind the cluster management communication." )
    public static final Setting<HostnamePort> cluster_server = setting( "ha.cluster_server", HOSTNAME_PORT,
            "0.0.0.0:5001-5099" );

    @Description( "Whether to allow this instance to create a cluster if unable to join." )
    public static final Setting<Boolean> allow_init_cluster = setting( "ha.allow_init_cluster", BOOLEAN, TRUE );

    // Timeout settings

    /*
     * ha.heartbeat_interval
     * ha.paxos_timeout
     * ha.learn_timeout
     */
    @Description( "Default timeout used for clustering timeouts. Override  specific timeout settings with proper" +
            " values if necessary. This value is the default value for the ha.heartbeat_interval," +
            " ha.paxos_timeout and ha.learn_timeout settings." )
    public static final Setting<Long> default_timeout = setting( "ha.default_timeout", DURATION, "5s" );

    @Description( "How often heartbeat messages should be sent. Defaults to ha.default_timeout." )
    public static final Setting<Long> heartbeat_interval = setting( "ha.heartbeat_interval", DURATION,
            default_timeout );

    @Description( "Timeout for heartbeats between cluster members. Should be at least twice that of ha.heartbeat_interval." )
    public static final Setting<Long> heartbeat_timeout = setting( "ha.heartbeat_timeout", DURATION, "11s" );

    /*
     * ha.join_timeout
     * ha.leave_timeout
     */
    @Description( "Timeout for broadcasting values in cluster. Must consider end-to-end duration of Paxos algorithm." +
            " This value is the default value for the ha.join_timeout and ha.leave_timeout settings." )
    public static final Setting<Long> broadcast_timeout = setting( "ha.broadcast_timeout", DURATION, "30s" );

    @Description( "Timeout for joining a cluster. Defaults to ha.broadcast_timeout." )
    public static final Setting<Long> join_timeout = setting( "ha.join_timeout", DURATION, broadcast_timeout );

    @Description( "Timeout for waiting for configuration from an existing cluster member during cluster join." )
    public static final Setting<Long> configuration_timeout = setting( "ha.configuration_timeout", DURATION, "1s" );

    @Description( "Timeout for waiting for cluster leave to finish. Defaults to ha.broadcast_timeout." )
    public static final Setting<Long> leave_timeout = setting( "ha.leave_timeout", DURATION, broadcast_timeout );

    /*
     *  ha.phase1_timeout
     *  ha.phase2_timeout
     *  ha.election_timeout
     */
    @Description( "Default timeout for all Paxos timeouts. Defaults to ha.default_timeout. This value is the default" +
            " value for the ha.phase1_timeout, ha.phase2_timeout and ha.election_timeout settings." )
    public static final Setting<Long> paxos_timeout = setting( "ha.paxos_timeout", DURATION, default_timeout );

    @Description( "Timeout for Paxos phase 1. Defaults to ha.paxos_timeout." )
    public static final Setting<Long> phase1_timeout = setting( "ha.phase1_timeout", DURATION, paxos_timeout );

    @Description( "Timeout for Paxos phase 2. Defaults to ha.paxos_timeout." )
    public static final Setting<Long> phase2_timeout = setting( "ha.phase2_timeout", DURATION, paxos_timeout );

    @Description( "Timeout for learning values. Defaults to ha.default_timeout." )
    public static final Setting<Long> learn_timeout = setting( "ha.learn_timeout", DURATION, default_timeout );

    @Description( "Timeout for waiting for other members to finish a role election. Defaults to ha.paxos_timeout." )
    public static final Setting<Long> election_timeout = setting( "ha.election_timeout", DURATION, paxos_timeout );

    @Description( "Maximum number of servers to involve when agreeing to membership changes. " +
            "In very large clusters, the probability of half the cluster failing is low, but protecting against " +
            "any arbitrary half failing is expensive. Therefore you may wish to set this parameter to a value less " +
            "than the cluster size." )
    public static final Setting<Integer> max_acceptors = setting( "ha.max_acceptors", INTEGER, "21", min( 1 ) );

    public static final Setting<String> instance_name = setting("ha.instance_name", STRING, (String) null);
}
