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

package org.neo4j.backup;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

import java.util.Map;

import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;

public class OnlineBackup
{
    private final String hostNameOrIp;
    private final int port;
    private BackupService.BackupOutcome outcome;

    public static OnlineBackup from( String hostNameOrIp, int port )
    {
        return new OnlineBackup( hostNameOrIp, port );
    }

    public static OnlineBackup from( String hostNameOrIp )
    {
        return new OnlineBackup( hostNameOrIp, BackupServer.DEFAULT_PORT );
    }

    private OnlineBackup( String hostNameOrIp, int port )
    {
        this.hostNameOrIp = hostNameOrIp;
        this.port = port;
    }

    public OnlineBackup full( String targetDirectory )
    {
        outcome = new BackupService().doFullBackup( hostNameOrIp, port, targetDirectory, true, defaultConfig() );
        return this;
    }

    public OnlineBackup full( String targetDirectory, boolean verification )
    {
        outcome = new BackupService().doFullBackup( hostNameOrIp, port, targetDirectory, verification,
                defaultConfig() );
        return this;
    }

    public OnlineBackup full( String targetDirectory, boolean verification, Config tuningConfiguration )
    {
        outcome = new BackupService().doFullBackup( hostNameOrIp, port, targetDirectory, verification,
                tuningConfiguration );
        return this;
    }

    public OnlineBackup incremental( String targetDirectory )
    {
        outcome = new BackupService().doIncrementalBackup( hostNameOrIp, port, targetDirectory, true );
        return this;
    }

    public OnlineBackup incremental( String targetDirectory, boolean verification )
    {
        outcome = new BackupService().doIncrementalBackup( hostNameOrIp, port, targetDirectory, verification );
        return this;
    }

    public OnlineBackup incremental( GraphDatabaseAPI targetDb )
    {
        outcome = new BackupService().doIncrementalBackup( hostNameOrIp, port, targetDb );
        return this;
    }

    public Map<String, Long> getLastCommittedTxs()
    {
        return outcome.getLastCommittedTxs();
    }

    private Config defaultConfig()
    {
        return new Config( stringMap(), GraphDatabaseSettings.class, ConsistencyCheckSettings.class );
    }
}
