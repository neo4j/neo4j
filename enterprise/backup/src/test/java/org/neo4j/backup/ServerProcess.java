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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.Pair;
import org.neo4j.test.subprocess.SubProcess;

public class ServerProcess extends SubProcess<ServerInterface, Pair<String, String>> implements ServerInterface
{
    private volatile transient GraphDatabaseService db;

    @Override
    public void startup( Pair<String, String> config ) throws Throwable
    {
        String storeDir = config.first();
        String backupConfigValue = config.other();
        if ( backupConfigValue == null )
        {
            this.db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        }
        else
        {
            // TODO This is using the old config style - is this class even used anywhere!?
            this.db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir ).setConfig( "enable_online_backup", backupConfigValue ).newGraphDatabase();
        }
    }

    @Override
    public void awaitStarted()
    {
        while ( db == null )
        {
            try
            {
                Thread.sleep( 100 );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
        }
    }

    @Override
    public void shutdown( boolean normal )
    {
        db.shutdown();
        new Thread()
        {
            @Override
            public void run()
            {
                try
                {
                    Thread.sleep( 100 );
                }
                catch ( InterruptedException e )
                {
                    Thread.interrupted();
                }
                shutdownProcess();
            }
        }.start();
    }

    protected void shutdownProcess()
    {
        super.shutdown();
    }
}
