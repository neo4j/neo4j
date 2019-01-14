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
