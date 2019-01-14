/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
            "same protocol that runs on causal_clustering.transaction_listen_address. The port range is only respected in a HA or single instance deployment." +
            " In Causal Clustering a single port should be used" )
    public static final Setting<HostnamePort> online_backup_server = setting( "dbms.backup.address", HOSTNAME_PORT, "127.0.0.1:6362-6372" );

    @Description( "Name of the SSL policy to be used by backup, as defined under the dbms.ssl.policy.* settings." +
            " If no policy is configured then the communication will not be secured." )
    public static final Setting<String> ssl_policy = prefixSetting( "dbms.backup.ssl_policy", STRING, NO_DEFAULT );
}
