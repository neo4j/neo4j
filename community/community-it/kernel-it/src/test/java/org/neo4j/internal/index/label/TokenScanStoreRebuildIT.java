/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.index.label;

import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.recordstorage.TestRelType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.TestLabels;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static org.neo4j.logging.LogAssertions.assertThat;

@DbmsExtension( configurationCallback = "configuration" )
class TokenScanStoreRebuildIT
{
    @Inject
    private GraphDatabaseService db;
    @Inject
    private DatabaseLayout layout;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private DbmsController controller;

    @ExtensionCallback
    void configuration( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store, true );
    }

    @Test
    void shouldReportCorrectEntityCountsOnRebuild()
    {
        // given
        int nbrOfNodes = 100;
        int nbrOfRelationships = 1_000;
        createSomeData( nbrOfNodes, nbrOfRelationships );

        // when
        AssertableLogProvider logProvider = new AssertableLogProvider();
        controller.restartDbms( builder ->
        {
            fs.deleteFile( layout.labelScanStore().toFile() );
            fs.deleteFile( layout.relationshipTypeScanStore().toFile() );
            builder.setInternalLogProvider( logProvider );
            return builder;
        } );

        // then
        assertThat( logProvider ).containsMessagesOnce(
                "No relationship type index found, this might just be first use. Preparing to rebuild.",
                "No label index found, this might just be first use. Preparing to rebuild.",
                "Rebuilding relationship type index, this may take a while",
                "Rebuilding label index, this may take a while",
                "Relationship type index rebuilt (roughly 1000 relationships)",
                "Label index rebuilt (roughly 100 nodes)"
                );
    }

    private void createSomeData( int nbrOfNodes, int nbrOfRelationships )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < nbrOfNodes; i++ )
            {
                Node node = tx.createNode( TestLabels.LABEL_ONE );
                if ( i == 0 )
                {
                    for ( int j = 0; j < nbrOfRelationships; j++ )
                    {
                        node.createRelationshipTo( node, TestRelType.LOOP );
                    }
                }
            }
            tx.commit();
        }
    }
}
