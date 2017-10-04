/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.ConsistencyCheckTool;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.junit.Assert.assertTrue;

public class CompositeConstraintIT
{

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Test
    public void compositeNodeKeyConstraintUpdate() throws Exception
    {
        File storeDir = testDirectory.graphDbDir();
        GraphDatabaseService database = new EnterpriseGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .newGraphDatabase();

        Label label = Label.label( "label" );

        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( label );
            node.setProperty( "b", (short) 3 );
            node.setProperty( "a", new double[]{0.6, 0.4, 0.2} );
            transaction.success();
        }

        try ( Transaction transaction = database.beginTx() )
        {
            String query =
                    format( "CREATE CONSTRAINT ON (n:%s) ASSERT (n.%s,n.%s) IS NODE KEY", label.name(), "a", "b" );
            database.execute( query );
            transaction.success();
        }

        awaitIndex( database );

        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( label );
            node.setProperty( "a", (short) 7 );
            node.setProperty( "b", new double[]{0.7, 0.5, 0.3} );
            transaction.success();
        }
        database.shutdown();

        ConsistencyCheckService.Result consistencyCheckResult = checkDbConsistency( storeDir );
        assertTrue( "Database is consistent", consistencyCheckResult.isSuccessful() );
    }

    private static ConsistencyCheckService.Result checkDbConsistency( File storeDir )
            throws ConsistencyCheckTool.ToolFailureException, IOException
    {
        return ConsistencyCheckTool.runConsistencyCheckTool( new String[]{storeDir.getAbsolutePath()},
                System.out, System.err );
    }

    private static void awaitIndex( GraphDatabaseService database )
    {
        try ( Transaction tx = database.beginTx() )
        {
            database.schema().awaitIndexesOnline( 2, TimeUnit.MINUTES );
            tx.success();
        }
    }
}
