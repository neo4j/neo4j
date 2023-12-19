/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
