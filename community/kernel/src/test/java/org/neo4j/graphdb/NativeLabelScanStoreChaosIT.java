/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.graphdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import javax.annotation.Resource;

import org.neo4j.kernel.api.impl.labelscan.LabelScanStoreTest;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.test.extension.EmbeddedDatabaseExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.helpers.collection.Iterators.asSet;

/**
 * Tests functionality around missing or corrupted lucene label scan store index, and that
 * the database should repair (i.e. rebuild) that automatically and just work.
 */
@ExtendWith( {EmbeddedDatabaseExtension.class, RandomExtension.class} )
public class NativeLabelScanStoreChaosIT
{
    @Resource
    public EmbeddedDatabaseRule dbRule;
    @Resource
    public RandomRule randomRule;

    @Test
    public void shouldRebuildDeletedLabelScanStoreOnStartup() throws Exception
    {
        // GIVEN
        Node node1 = createLabeledNode( Labels.First );
        Node node2 = createLabeledNode( Labels.First );
        Node node3 = createLabeledNode( Labels.First );
        deleteNode( node2 ); // just to create a hole in the store

        // WHEN
        dbRule.restartDatabase( deleteTheLabelScanStoreIndex() );

        // THEN
        assertEquals( asSet( node1, node3 ), getAllNodesWithLabel( Labels.First ) );
    }

    @Test
    public void rebuildCorruptedLabelScanStoreToStartup() throws Exception
    {
        Node node = createLabeledNode( Labels.First );

        dbRule.restartDatabase( corruptTheLabelScanStoreIndex() );

        assertEquals( asSet( node ), getAllNodesWithLabel( Labels.First ) );
    }

    private static File storeFile( File directory )
    {
        return new File( directory, NativeLabelScanStore.FILE_NAME );
    }

    private DatabaseRule.RestartAction corruptTheLabelScanStoreIndex()
    {
        return ( fs, directory ) -> scrambleFile( storeFile( directory ) );
    }

    private DatabaseRule.RestartAction deleteTheLabelScanStoreIndex()
    {
        return ( fs, directory ) -> fs.deleteFile( storeFile( directory ) );
    }

    private Node createLabeledNode( Label... labels )
    {
        try ( Transaction tx = dbRule.getGraphDatabaseAPI().beginTx() )
        {
            Node node = dbRule.getGraphDatabaseAPI().createNode( labels );
            tx.success();
            return node;
        }
    }

    private Set<Node> getAllNodesWithLabel( Label label )
    {
        try ( Transaction ignored = dbRule.getGraphDatabaseAPI().beginTx() )
        {
            return asSet( dbRule.getGraphDatabaseAPI().findNodes( label ) );
        }
    }

    private void scrambleFile( File file ) throws IOException
    {
        LabelScanStoreTest.scrambleFile( randomRule.random(), file );
    }

    private void deleteNode( Node node )
    {
        try ( Transaction tx = dbRule.getGraphDatabaseAPI().beginTx() )
        {
            node.delete();
            tx.success();
        }
    }

    private enum Labels implements Label
    {
        First, Second, Third
    }
}
