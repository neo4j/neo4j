/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.consistency.checking.full;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.consistency.checking.full.ConsistencyFlags.DEFAULT;

@TestDirectoryExtension
public class FullCheckCompositeFusionIndex
{
    @Inject
    TestDirectory dir;

    // There was a bug for consistency checking of composite fusion indexes.
    // For a composite fusion index, it is just a btree which means we can get the values.
    // But a reader that always returned null as values were used, which meant that all entries in the index were reported as inconsistent.
    @Test
    void checkCompositeFusionIndexCorrectly() throws ConsistencyCheckIncompleteException
    {
        Label myLabel = Label.label( "myLabel" );
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder()
                .setDatabaseRootDirectory( dir.homePath() )
                .build();
        GraphDatabaseService neo4j = managementService.database( "neo4j" );

        try ( Transaction tx = neo4j.beginTx() )
        {
            tx.execute( "CREATE INDEX fusionIndex FOR (n:myLabel) ON (n.prop1, n.prop2) OPTIONS {indexProvider:'lucene+native-3.0'}" );
            tx.commit();
        }
        try ( Transaction tx = neo4j.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 5, TimeUnit.MINUTES );
            Node node = tx.createNode( myLabel );
            node.setProperty( "prop1", "hej" );
            node.setProperty( "prop2", "hi" );
            tx.commit();
        }

        managementService.shutdown();

        ConsistencyCheckService.Result result = runFullConsistencyCheck( dir.homePath() );
        assertTrue( result.isSuccessful() );
    }

    private ConsistencyCheckService.Result runFullConsistencyCheck( Path path )
            throws ConsistencyCheckIncompleteException
    {
        var config = Config.newBuilder().set( GraphDatabaseSettings.neo4j_home, path ).build();

        ConsistencyCheckService checkService = new ConsistencyCheckService();
        return checkService.runFullConsistencyCheck( Neo4jLayout.of( config ).databaseLayout( "neo4j" ),
                config, ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), false, DEFAULT );
    }
}
