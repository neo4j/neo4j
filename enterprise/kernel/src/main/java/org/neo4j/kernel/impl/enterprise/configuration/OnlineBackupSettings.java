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
package org.neo4j.kernel.impl.enterprise.configuration;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.HostnamePort;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.HOSTNAME_PORT;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.configuration.Settings.prefixSetting;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * Settings for online backup
 */
@Description( "Online backup configuration settings" )
public class OnlineBackupSettings implements LoadableConfig
{
    @Description( "Enable support for running online backups" )
    public static final Setting<Boolean> online_backup_enabled = setting( "dbms.backup.enabled", BOOLEAN, TRUE );

    @Description( "Listening server for online backups. The protocol running varies depending on deployment. In a Causal Clustering environment this is the " +
            "same protocol that runs on causal_clustering.transaction_listen_address." )
    public static final Setting<HostnamePort> online_backup_server = setting( "dbms.backup.address", HOSTNAME_PORT, "127.0.0.1:6362-6372" );

    @Description( "Name of the SSL policy to be used by backup, as defined under the dbms.ssl.policy.* settings." +
            " If no policy is configured then the communication will not be secured." )
    public static final Setting<String> ssl_policy = prefixSetting( "dbms.backup.ssl_policy", STRING, NO_DEFAULT );
}
