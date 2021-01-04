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
package org.neo4j.graphdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Set;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.index.schema.TokenScanStoreTest;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

/**
 * Tests functionality around missing or corrupted label scan store index, and that
 * the database should repair (i.e. rebuild) that automatically and just work.
 */
@DbmsExtension
@ExtendWith( RandomExtension.class )
public class LabelScanStoreChaosIT
{
    @Inject
    private RandomRule random;
    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private DbmsController controller;

    @Test
    void shouldRebuildDeletedLabelScanStoreOnStartup()
    {
        Node node1 = createLabeledNode( Labels.First );
        Node node2 = createLabeledNode( Labels.First );
        Node node3 = createLabeledNode( Labels.First );
        deleteNode( node2 ); // just to create a hole in the store

        controller.restartDbms( builder ->
        {
            fs.deleteFile( storeFile( db.databaseLayout() ) );
            return builder;
        } );

        assertEquals( asSet( node1, node3 ), getAllNodesWithLabel( Labels.First ) );
    }

    @Test
    void rebuildCorruptedLabelScanStoreToStartup()
    {
        Node node = createLabeledNode( Labels.First );

        controller.restartDbms( builder ->
        {
            scrambleFile( storeFile( db.databaseLayout() ) );
            return builder;
        } );

        assertEquals( asSet( node ), getAllNodesWithLabel( Labels.First ) );
    }

    private static java.nio.file.Path storeFile( DatabaseLayout databaseLayout )
    {
        return databaseLayout.labelScanStore();
    }

    private Node createLabeledNode( Label... labels )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( labels );
            tx.commit();
            return node;
        }
    }

    private Set<Node> getAllNodesWithLabel( Label label )
    {
        try ( Transaction tx = db.beginTx() )
        {
            return asSet( tx.findNodes( label ) );
        }
    }

    private void scrambleFile( java.nio.file.Path path )
    {
        TokenScanStoreTest.scrambleFile( random.random(), path );
    }

    private void deleteNode( Node node )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( node.getId() ).delete();
            tx.commit();
        }
    }

    private enum Labels implements Label
    {
        First, Second, Third
    }
}
