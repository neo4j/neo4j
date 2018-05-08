/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.Test;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.helpers.Nodes;

import static org.eclipse.collections.impl.set.mutable.primitive.LongHashSet.newSetWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.containsOnly;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.getPropertyKeys;

/**
 * Test read access to committed label data.
 */
public class RecordStorageReaderLabelTest extends RecordStorageReaderTestBase
{
    @Test
    public void should_be_able_to_list_labels_for_node() throws Exception
    {
        // GIVEN
        long nodeId;
        int labelId1;
        int labelId2;
        try ( Transaction tx = db.beginTx() )
        {
            nodeId = db.createNode( label1, label2 ).getId();
            String labelName1 = label1.name();
            String labelName2 = label2.name();
            labelId1 = storageReader.labelGetForName( labelName1 );
            labelId2 = storageReader.labelGetOrCreateForName( labelName2 );
            tx.success();
        }

        // THEN
        NodeCursor nodeCursor = storageReader.allocateNodeCursor();
        storageReader.singleNode( nodeId, nodeCursor );
        assertTrue( nodeCursor.next() );
        assertEquals( newSetWith( labelId1, labelId2 ), asLongSet( nodeCursor.labels() ) );
    }

    @Test
    public void should_be_able_to_get_label_name_for_label() throws Exception
    {
        // GIVEN
        String labelName = label1.name();
        int labelId = storageReader.labelGetOrCreateForName( labelName );

        // WHEN
        String readLabelName = storageReader.labelGetName( labelId );

        // THEN
        assertEquals( labelName, readLabelName );
    }

    @Test
    public void labels_should_not_leak_out_as_properties()
    {
        // GIVEN
        Node node = createLabeledNode( db, map( "name", "Node" ), label1 );

        // WHEN THEN
        assertThat( getPropertyKeys( db, node ), containsOnly( "name" ) );
    }

    @Test
    public void should_return_all_nodes_with_label()
    {
        // GIVEN
        Node node1 = createLabeledNode( db, map( "name", "First", "age", 1L ), label1 );
        Node node2 = createLabeledNode( db, map( "type", "Node", "count", 10 ), label1, label2 );
        int labelId1 = storageReader.labelGetForName( label1.name() );
        int labelId2 = storageReader.labelGetForName( label2.name() );

        // WHEN
        LongIterator nodesForLabel1 = storageReader.nodesGetForLabel( labelId1 );
        LongIterator nodesForLabel2 = storageReader.nodesGetForLabel( labelId2 );

        // THEN
        assertEquals( asSet( node1.getId(), node2.getId() ), PrimitiveLongCollections.toSet( nodesForLabel1 ) );
        assertEquals( asSet( node2.getId() ), PrimitiveLongCollections.toSet( nodesForLabel2 ) );
    }

    private LongHashSet asLongSet( LabelSet labels )
    {
        LongHashSet set = newSetWith();
        Nodes.visitLabels( labels, set::add );
        return set;
    }
}
