/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.List;
import java.util.Set;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static java.util.Collections.emptySet;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.Iterators.single;

public class NativeLabelScanStoreUpdateIT
{
    @ClassRule
    public static final DatabaseRule dbRule = new ImpermanentDatabaseRule();
    @Rule
    public final TestName testName = new TestName();

    private Label First;
    private Label Second;
    private Label Third;

    @Before
    public void setupLabels()
    {
        First = Label.label( "First-" + testName.getMethodName() );
        Second = Label.label( "Second-" + testName.getMethodName() );
        Third = Label.label( "Third-" + testName.getMethodName() );
    }

    @Test
    public void shouldGetNodesWithCreatedLabel()
    {
        // GIVEN
        Node node1 = createLabeledNode( First );
        Node node2 = createLabeledNode( Second );
        Node node3 = createLabeledNode( Third );
        Node node4 = createLabeledNode( First, Second, Third );
        Node node5 = createLabeledNode( First, Third );

        // THEN
        assertEquals(
                asSet( node1, node4, node5 ),
                Iterables.asSet( getAllNodesWithLabel( First ) ) );
        assertEquals(
                asSet( node2, node4 ),
                Iterables.asSet( getAllNodesWithLabel( Second ) ) );
        assertEquals(
                asSet( node3, node4, node5 ),
                Iterables.asSet( getAllNodesWithLabel( Third ) ) );
    }

    @Test
    public void shouldGetNodesWithAddedLabel()
    {
        // GIVEN
        Node node1 = createLabeledNode( First );
        Node node2 = createLabeledNode( Second );
        Node node3 = createLabeledNode( Third );
        Node node4 = createLabeledNode( First );
        Node node5 = createLabeledNode( First );

        // WHEN
        addLabels( node4, Second, Third );
        addLabels( node5, Third );

        // THEN
        assertEquals(
                asSet( node1, node4, node5 ),
                Iterables.asSet( getAllNodesWithLabel( First ) ) );
        assertEquals(
                asSet( node2, node4 ),
                Iterables.asSet( getAllNodesWithLabel( Second ) ) );
        assertEquals(
                asSet( node3, node4, node5 ),
                Iterables.asSet( getAllNodesWithLabel( Third ) ) );
    }

    @Test
    public void shouldGetNodesAfterDeletedNodes()
    {
        // GIVEN
        Node node1 = createLabeledNode( First, Second );
        Node node2 = createLabeledNode( First, Third );

        // WHEN
        deleteNode( node1 );

        // THEN
        assertEquals(
                asSet( node2 ),
                getAllNodesWithLabel( First ) );
        assertEquals( emptySet(),
                getAllNodesWithLabel( Second ) );
        assertEquals(
                asSet( node2 ),
                getAllNodesWithLabel( Third ) );
    }

    @Test
    public void shouldGetNodesAfterRemovedLabels()
    {
        // GIVEN
        Node node1 = createLabeledNode( First, Second );
        Node node2 = createLabeledNode( First, Third );

        // WHEN
        removeLabels( node1, First );
        removeLabels( node2, Third );

        // THEN
        assertEquals(
                asSet( node2 ),
                getAllNodesWithLabel( First ) );
        assertEquals(
                asSet( node1 ),
                getAllNodesWithLabel( Second ) );
        assertEquals( emptySet(),
                getAllNodesWithLabel( Third ) );
    }

    @Test
    public void retrieveNodeIdsInAscendingOrder()
    {
        for ( int i = 0; i < 50; i++ )
        {
            createLabeledNode( Labels.First, Labels.Second );
            createLabeledNode( Labels.Second );
            createLabeledNode( Labels.First );
        }
        long nodeWithThirdLabel = createLabeledNode( Labels.Third ).getId();

        verifyFoundNodes( Labels.Third, "Expect to see 1 matched nodeId: " + nodeWithThirdLabel, nodeWithThirdLabel );

        Node nodeById = getNodeById( 1 );
        addLabels( nodeById, Labels.Third );

        verifyFoundNodes( Labels.Third, "Expect to see 2 matched nodeIds: 1, " + nodeWithThirdLabel, 1,
                nodeWithThirdLabel );
    }

    @Test
    public void shouldHandleLargeAmountsOfNodesAddedAndRemovedInSameTx()
    {
        // Given
        GraphDatabaseService db = dbRule;
        int labelsToAdd = 80;
        int labelsToRemove = 40;

        // When
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();

            // I create a lot of labels, enough to push the store to use two dynamic records
            for ( int l = 0; l < labelsToAdd; l++ )
            {
                node.addLabel( label( "Label-" + l ) );
            }

            // and I delete some of them, enough to bring the number of dynamic records needed down to 1
            for ( int l = 0; l < labelsToRemove; l++ )
            {
                node.removeLabel( label( "Label-" + l ) );
            }

            tx.success();
        }

        // Then
        try ( Transaction ignore = db.beginTx() )
        {
            // All the labels remaining should be in the label scan store
            for ( int l = labelsToAdd - 1; l >= labelsToRemove; l-- )
            {
                Label label = label( "Label-" + l );
                assertThat( "Should have founnd node when looking for label " + label,
                        single( db.findNodes( label ) ), equalTo( node ) );
            }
        }
    }

    private void verifyFoundNodes( Label label, String sizeMismatchMessage, long... expectedNodeIds )
    {
        try ( Transaction ignored = dbRule.beginTx() )
        {
            ResourceIterator<Node> nodes = dbRule.findNodes( label );
            List<Node> nodeList = Iterators.asList( nodes );
            assertThat( sizeMismatchMessage, nodeList, Matchers.hasSize( expectedNodeIds.length ) );
            int index = 0;
            for ( Node node : nodeList )
            {
                assertEquals( expectedNodeIds[index++], node.getId() );
            }
        }
    }

    private void removeLabels( Node node, Label... labels )
    {
        try ( Transaction tx = dbRule.beginTx() )
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
        try ( Transaction tx = dbRule.beginTx() )
        {
            node.delete();
            tx.success();
        }
    }

    private Set<Node> getAllNodesWithLabel( Label label )
    {
        try ( Transaction ignored = dbRule.beginTx() )
        {
            return asSet( dbRule.findNodes( label ) );
        }
    }

    private Node createLabeledNode( Label... labels )
    {
        try ( Transaction tx = dbRule.beginTx() )
        {
            Node node = dbRule.createNode( labels );
            tx.success();
            return node;
        }
    }

    private void addLabels( Node node, Label... labels )
    {
        try ( Transaction tx = dbRule.beginTx() )
        {
            for ( Label label : labels )
            {
                node.addLabel( label );
            }
            tx.success();
        }
    }

    private Node getNodeById( long id )
    {
        try ( Transaction ignored = dbRule.beginTx() )
        {
            return dbRule.getNodeById( id );
        }
    }

    private enum Labels implements Label
    {
        First,
        Second,
        Third
    }
}
