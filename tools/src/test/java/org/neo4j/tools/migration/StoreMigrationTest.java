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
package org.neo4j.tools.migration;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.Unzip;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

public class StoreMigrationTest
{
    @Rule
    public final SuppressOutput mute = SuppressOutput.suppressAll();
    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();

    @Before
    public void setUp() throws IOException
    {
        Unzip.unzip( getClass(), "2.3-store.zip", testDir.graphDbDir() );
    }

    @Test
    public void storeMigrationToolShouldBeAbleToMigrateOldStore() throws IOException
    {
        StoreMigration.main( new String[]{testDir.graphDbDir().getAbsolutePath()} );

        // after migration we can open store and do something
        GraphDatabaseService database = new TestGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( testDir.graphDbDir() )
                .setConfig( GraphDatabaseSettings.logs_directory, testDir.directory( "logs" ).getAbsolutePath() )
                .newGraphDatabase();
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
            node.setProperty( "key", "value" );
            transaction.success();
        }
        finally
        {
            database.shutdown();
        }
    }
}
