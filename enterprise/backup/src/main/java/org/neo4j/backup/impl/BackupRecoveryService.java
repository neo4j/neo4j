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
package org.neo4j.backup.impl;

import java.nio.file.Path;
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.neo4j.backup.impl.BackupProtocolService.startTemporaryDb;

class BackupRecoveryService
{
    public void recoverWithDatabase( Path targetDirectory, PageCache pageCache, Config config )
    {
        Map<String,String> configParams = config.getRaw();
        configParams.put( GraphDatabaseSettings.logical_logs_location.name(), targetDirectory.toString() );
        configParams.put( GraphDatabaseSettings.pagecache_warmup_enabled.name(), Settings.FALSE );
        GraphDatabaseAPI targetDb = startTemporaryDb( targetDirectory, pageCache, configParams );
        targetDb.shutdown();
    }
}
