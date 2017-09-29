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
package org.neo4j.backup;

import org.neo4j.configuration.Description;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.HostnamePort;

@Deprecated
@Description( "Online backup configuration settings" )
public class OnlineBackupSettings extends org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings
{
    @Deprecated
    @Description( "Enable support for running online backups" )
    public static final Setting<Boolean> online_backup_enabled = org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_enabled;

    @Deprecated
    @Description( "Listening server for online backups" )
    public static final Setting<HostnamePort> online_backup_server = org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_server;
}
