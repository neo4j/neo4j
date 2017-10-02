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
package org.neo4j.helper;

import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import static org.neo4j.kernel.configuration.Settings.TRUE;

public class DatabaseConfiguration
{
    private DatabaseConfiguration()
    {
        // no instances
    }

    public static Map<String,String> configureTxLogRotationAndPruning( Map<String,String> settings, String txPrune )
    {
        settings.put( GraphDatabaseSettings.keep_logical_logs.name(), txPrune );
        settings.put( GraphDatabaseSettings.logical_log_rotation_threshold.name(), "1M" );
        return settings;
    }

    public static Map<String,String> configureBackup( Map<String,String> settings, String hostname, int port )
    {
        settings.put( OnlineBackupSettings.online_backup_enabled.name(), TRUE );
        settings.put( OnlineBackupSettings.online_backup_server.name(), hostname + ":" + port );
        return settings;
    }
}
