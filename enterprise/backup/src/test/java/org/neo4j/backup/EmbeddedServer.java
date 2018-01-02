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
package org.neo4j.backup;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

public class EmbeddedServer implements ServerInterface
{
    private GraphDatabaseService db;

    public EmbeddedServer( File storeDir, String serverAddress )
    {
        GraphDatabaseBuilder graphDatabaseBuilder = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir );
        graphDatabaseBuilder.setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE );
        graphDatabaseBuilder.setConfig( OnlineBackupSettings.online_backup_server, serverAddress );
        graphDatabaseBuilder.setConfig( GraphDatabaseSettings.pagecache_memory, "8m" );
        this.db = graphDatabaseBuilder.newGraphDatabase();
    }

    @Override
    public void shutdown()
    {
        db.shutdown();
    }

    @Override
    public void awaitStarted()
    {
    }
}
