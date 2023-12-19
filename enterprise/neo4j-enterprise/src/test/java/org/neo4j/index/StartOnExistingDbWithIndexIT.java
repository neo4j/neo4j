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
package org.neo4j.index;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

public class StartOnExistingDbWithIndexIT
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void startStopDatabaseWithIndex()
    {
        Label label = Label.label( "string" );
        String property = "property";
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        GraphDatabaseService db = prepareDb( label, property, logProvider );
        db.shutdown();
        db = getDatabase( logProvider );
        db.shutdown();

        logProvider.assertNoMessagesContaining( "Failed to open index" );
    }

    private GraphDatabaseService prepareDb( Label label, String propertyName, LogProvider logProvider )
    {
        GraphDatabaseService db = getDatabase( logProvider );
        try ( Transaction transaction = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( propertyName ).create();
            transaction.success();
        }
        waitIndexes( db );
        return db;
    }

    private GraphDatabaseService getDatabase( LogProvider logProvider )
    {
        return new TestEnterpriseGraphDatabaseFactory()
                .setInternalLogProvider( logProvider )
                .newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .newGraphDatabase();
    }

    private void waitIndexes( GraphDatabaseService db )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 5, TimeUnit.SECONDS );
            transaction.success();
        }
    }
}
