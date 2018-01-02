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

import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptySetOf;
import static org.neo4j.helpers.collection.IteratorUtil.single;

public class LabelScanStoreIT
{
    @Test
    public void shouldGetNodesWithCreatedLabel() throws Exception
    {
        // GIVEN
        Node node1 = createLabeledNode( Labels.First );
        Node node2 = createLabeledNode( Labels.Second );
        Node node3 = createLabeledNode( Labels.Third );
        Node node4 = createLabeledNode( Labels.First, Labels.Second, Labels.Third );
        Node node5 = createLabeledNode( Labels.First, Labels.Third );
        
        // THEN
        assertEquals(
                asSet( node1, node4, node5 ),
                asSet( getAllNodesWithLabel( Labels.First ) ) );
        assertEquals(
                asSet( node2, node4 ),
                asSet( getAllNodesWithLabel( Labels.Second ) ) );
        assertEquals(
                asSet( node3, node4, node5 ),
                asSet( getAllNodesWithLabel( Labels.Third ) ) );
    }
    
    @Test
    public void shouldGetNodesWithAddedLabel() throws Exception
    {
        // GIVEN
        Node node1 = createLabeledNode( Labels.First );
        Node node2 = createLabeledNode( Labels.Second );
        Node node3 = createLabeledNode( Labels.Third );
        Node node4 = createLabeledNode( Labels.First );
        Node node5 = createLabeledNode( Labels.First );
        
        // WHEN
        addLabels( node4, Labels.Second, Labels.Third );
        addLabels( node5, Labels.Third );
        
        // THEN
        assertEquals(
                asSet( node1, node4, node5 ),
                asSet( getAllNodesWithLabel( Labels.First ) ) );
        assertEquals(
                asSet( node2, node4 ),
                asSet( getAllNodesWithLabel( Labels.Second ) ) );
        assertEquals(
                asSet( node3, node4, node5 ),
                asSet( getAllNodesWithLabel( Labels.Third ) ) );
    }
    
    @Test
    public void shouldGetNodesAfterDeletedNodes() throws Exception
    {
        // GIVEN
        Node node1 = createLabeledNode( Labels.First, Labels.Second );
        Node node2 = createLabeledNode( Labels.First, Labels.Third );
        
        // WHEN
        deleteNode( node1 );
        
        // THEN
        assertEquals(
                asSet( node2 ),
                getAllNodesWithLabel( Labels.First ) );
        assertEquals(
                emptySetOf( Node.class ),
                getAllNodesWithLabel( Labels.Second ) );
        assertEquals(
                asSet( node2 ),
                getAllNodesWithLabel( Labels.Third ) );
    }
    
    @Test
    public void shouldGetNodesAfterRemovedLabels() throws Exception
    {
        // GIVEN
        Node node1 = createLabeledNode( Labels.First, Labels.Second );
        Node node2 = createLabeledNode( Labels.First, Labels.Third );
        
        // WHEN
        removeLabels( node1, Labels.First );
        removeLabels( node2, Labels.Third );
        
        // THEN
        assertEquals(
                asSet( node2 ),
                getAllNodesWithLabel( Labels.First ) );
        assertEquals(
                asSet( node1 ),
                getAllNodesWithLabel( Labels.Second ) );
        assertEquals(
                emptySetOf( Node.class ),
                getAllNodesWithLabel( Labels.Third ) );
    }

    @Test
    public void shouldHandleLargeAmountsOfNodesAddedAndRemovedInSameTx() throws Exception
    {
        // Given
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        int labelsToAdd = 80;
        int labelsToRemove = 40;

        // When
        Node node;
        try( Transaction tx = db.beginTx() )
        {
            node = db.createNode();

            // I create a lot of labels, enough to push the store to use two dynamic records
            for(int l=0;l<labelsToAdd;l++)
            {
                node.addLabel( label("Label-" + l) );
            }

            // and I delete some of them, enough to bring the number of dynamic records needed down to 1
            for(int l=0;l<labelsToRemove;l++)
            {
                node.removeLabel( label("Label-" + l) );
            }

            tx.success();
        }

        // Then
        try( Transaction ignore = db.beginTx() )
        {
            // All the labels remaining should be in the label scan store
            for(int l=labelsToAdd-1;l>=labelsToRemove;l--)
            {
                Label label = label( "Label-" + l );
                assertThat( "Should have founnd node when looking for label " + label,
                        single( db.findNodes( label ) ), equalTo( node ) );
            }
        }
    }
    
    private void removeLabels( Node node, Label... labels )
    {
        try ( Transaction tx = dbRule.getGraphDatabaseService().beginTx() )
        {
            for ( Label label : labels )
            {
                node.removeLabel( label );
            }
            tx.success();
        }
    }

    private void deleteNode( Node node )
    {
        try ( Transaction tx = dbRule.getGraphDatabaseService().beginTx() )
        {
            node.delete();
            tx.success();
        }
    }

    private Set<Node> getAllNodesWithLabel( Label label )
    {
        try ( Transaction tx = dbRule.getGraphDatabaseService().beginTx() )
        {
            return asSet( dbRule.getGraphDatabaseService().findNodes( label ) );
        }
    }

    private Node createLabeledNode( Label... labels )
    {
        try ( Transaction tx = dbRule.getGraphDatabaseService().beginTx() )
        {
            Node node = dbRule.getGraphDatabaseService().createNode( labels );
            tx.success();
            return node;
        }
    }
    
    private void addLabels( Node node, Label... labels )
    {
        try ( Transaction tx = dbRule.getGraphDatabaseService().beginTx() )
        {
            for ( Label label : labels )
            {
                node.addLabel( label );
            }
            tx.success();
        }
    }

    public final @Rule DatabaseRule dbRule = new ImpermanentDatabaseRule();
    
    private static enum Labels implements Label
    {
        First,
        Second,
        Third
    }
}
