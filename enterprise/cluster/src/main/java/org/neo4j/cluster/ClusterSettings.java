/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import static org.neo4j.graphdb.factory.GraphDatabaseSetting.HostnamePortSetting;
import static org.neo4j.graphdb.factory.GraphDatabaseSetting.TRUE;
import static org.neo4j.graphdb.factory.GraphDatabaseSetting.TimeSpanSetting;

import org.neo4j.graphdb.factory.Default;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSetting.BooleanSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSetting.IntegerSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSetting.StringSetting;

/**
 * Settings for high availability mode
 */
public class ClusterSettings
{
    @Default("20s")
    public static final TimeSpanSetting read_timeout = new TimeSpanSetting( "ha.read_timeout" );

    public static final IntegerSetting server_id = new IntegerSetting( "ha.server_id",
            "Must be a valid server id" );

    @Default(":6361")
    public static final HostnamePortSetting ha_server = new HostnamePortSetting( "ha.server" );

    @Default("neo4j.ha")
    public static final StringSetting cluster_name = new StringSetting( "ha.cluster_name", ANY,
            "Must be a valid cluster name" );

    @Default("")
    public static final StringSetting initial_hosts = new StringSetting(
            "ha.initial_hosts", GraphDatabaseSetting.ANY, "Must be a valid list of hosts" );

    @Default(":5001-5099")
    public static final HostnamePortSetting cluster_server = new HostnamePortSetting(
            "ha.cluster_server" );

    @Default(TRUE)
    public static final BooleanSetting allow_init_cluster = new BooleanSetting( "ha.allow_init_cluster" );
}