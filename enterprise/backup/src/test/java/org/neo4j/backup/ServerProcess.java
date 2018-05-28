/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.neo4j.helpers.collection.Pair;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.subprocess.SubProcess;

public class ServerProcess extends SubProcess<ServerInterface, Pair<File, String>> implements ServerInterface
{
    private transient volatile GraphDatabaseService db;

    @Override
    public void startup( Pair<File, String> config ) throws Throwable
    {
        File storeDir = config.first();
        String backupConfigValue = config.other();
        if ( backupConfigValue == null )
        {
            this.db = new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        }
        else
        {
            // TODO This is using the old config style - is this class even used anywhere!?
            this.db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir ).setConfig( "enable_online_backup", backupConfigValue ).newGraphDatabase();
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
