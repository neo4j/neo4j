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
