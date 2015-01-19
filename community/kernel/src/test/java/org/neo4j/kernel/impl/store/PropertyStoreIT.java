/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.ImpermanentDatabaseRule;

public class PropertyStoreIT
{
    public final
    @Rule
    EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    private EphemeralFileSystemAbstraction fileSystem;
    private File storeDir;

    public final
    @Rule
    DatabaseRule db = new ImpermanentDatabaseRule();

    private static final String LONG_STRING_VALUE =
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALONG!!";


    @Before
    public void setup() throws IOException
    {
        fileSystem = fsRule.get();
        storeDir = new File( "/tmp/foobar" );

        fileSystem.mkdir( storeDir.getParentFile() );
        fileSystem.create( storeDir );
    }

    @Test
    public void shouldWriteOutTheDynamicChainBeforeUpdatingThePropertyRecord()
            throws IOException, ExecutionException, InterruptedException
    {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        final long[] latestNodeId = new long[1];

        for ( int i = 0; i < 100_000; i++ )
        {
            executor.scheduleAtFixedRate( new Runnable()
            {
                @Override
                public void run()
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        Node node = db.createNode();
                        latestNodeId[0] = node.getId();
                        node.setProperty( "largeProperty", LONG_STRING_VALUE );
                        tx.success();
                    }
                }
            }, 5, 25, TimeUnit.MILLISECONDS );
        }

        for ( int i = 0; i < 100_000; i++ )
        {
            try ( Transaction tx = db.getGraphDatabaseAPI().beginTx() )
            {
                Node node = db.getGraphDatabaseService().getNodeById( latestNodeId[0] );

                for ( String propertyKey : node.getPropertyKeys() )
                {
                    node.getProperty( propertyKey );
                }
                tx.success();
            }
            catch ( NotFoundException e )
            {
                // acceptable!
            }
        }

        executor.shutdown();
        executor.awaitTermination( 2000, TimeUnit.MILLISECONDS );
    }
}
