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
package org.neo4j;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

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
            throws ConsistencyCheckTool.ToolFailureException
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
