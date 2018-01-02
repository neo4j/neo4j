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
package org.neo4j.kernel.impl.api.store;

import java.util.HashSet;

import org.junit.Test;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import static org.neo4j.graphdb.Neo4jMatchers.containsOnly;
import static org.neo4j.graphdb.Neo4jMatchers.getPropertyKeys;
import static org.neo4j.helpers.collection.IteratorUtil.addToCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * Test read access to committed label data.
 */
public class DiskLayerLabelTest extends DiskLayerTest
{
    @Test
    public void should_be_able_to_list_labels_for_node() throws Exception
    {
        // GIVEN
        long nodeId;
        int labelId1, labelId2;
        try ( Transaction tx = db.beginTx() )
        {
            nodeId = db.createNode( label1, label2 ).getId();
            String labelName1 = label1.name(), labelName2 = label2.name();
            labelId1 = disk.labelGetForName( labelName1 );
            labelId2 = disk.labelGetOrCreateForName( labelName2 );
            tx.success();
        }

        // THEN
        Cursor<NodeItem> node = disk.acquireStatement().acquireSingleNodeCursor( nodeId );
        node.next();
        PrimitiveIntIterator readLabels = node.get().getLabels();
        assertEquals( new HashSet<>( asList( labelId1, labelId2 ) ),
                addToCollection( readLabels, new HashSet<Integer>() ) );
    }

    @Test
    public void should_be_able_to_get_label_name_for_label() throws Exception
    {
        // GIVEN
        String labelName = label1.name();
        int labelId = disk.labelGetOrCreateForName( labelName );

        // WHEN
        String readLabelName = disk.labelGetName( labelId );

        // THEN
        assertEquals( labelName, readLabelName );
    }

    /*
     * This test doesn't really belong here, but OTOH it does, as it has to do with this specific
     * store solution. It creates its own IGD to try reproduce to trigger the problem.
     */
    @Test
    public void labels_should_not_leak_out_as_properties() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase();
        Node node = createLabeledNode( db, map( "name", "Node" ), label1 );

        // WHEN THEN
        assertThat( getPropertyKeys( db, node ), containsOnly( "name" ) );

        db.shutdown();
    }

    @Test
    public void should_return_all_nodes_with_label() throws Exception
    {
        // GIVEN
        Node node1 = createLabeledNode( db, map( "name", "First", "age", 1L ), label1 );
        Node node2 = createLabeledNode( db, map( "type", "Node", "count", 10 ), label1, label2 );
        int labelId1 = disk.labelGetForName( label1.name() );
        int labelId2 = disk.labelGetForName( label2.name() );

        // WHEN
        PrimitiveLongIterator nodesForLabel1 = disk.nodesGetForLabel( state, labelId1 );
        PrimitiveLongIterator nodesForLabel2 = disk.nodesGetForLabel( state, labelId2 );

        // THEN
        assertEquals( asSet( node1.getId(), node2.getId() ), IteratorUtil.asSet( nodesForLabel1 ) );
        assertEquals( asSet( node2.getId() ), IteratorUtil.asSet( nodesForLabel2 ) );
    }
}
