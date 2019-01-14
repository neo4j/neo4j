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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.eclipse.collections.api.iterator.LongIterator;
import org.junit.Test;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.storageengine.api.StorageNodeCursor;

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
    public void shouldBeAbleToListLabelsForNode() throws Exception
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
            labelId1 = labelId( Label.label( labelName1 ) );
            labelId2 = labelId( Label.label( labelName2 ) );
            tx.success();
        }

        // THEN
        StorageNodeCursor nodeCursor = storageReader.allocateNodeCursor();
        nodeCursor.single( nodeId );
        assertTrue( nodeCursor.next() );
        assertEquals( newSetWith( labelId1, labelId2 ), newSetWith( nodeCursor.labels() ) );
    }

    @Test
    public void labelsShouldNotLeakOutAsProperties()
    {
        // GIVEN
        Node node = createLabeledNode( db, map( "name", "Node" ), label1 );

        // WHEN THEN
        assertThat( getPropertyKeys( db, node ), containsOnly( "name" ) );
    }

    @Test
    public void shouldReturnAllNodesWithLabel()
    {
        // GIVEN
        Node node1 = createLabeledNode( db, map( "name", "First", "age", 1L ), label1 );
        Node node2 = createLabeledNode( db, map( "type", "Node", "count", 10 ), label1, label2 );
        int labelId1 = labelId( label1 );
        int labelId2 = labelId( label2 );

        // WHEN
        LongIterator nodesForLabel1 = storageReader.nodesGetForLabel( labelId1 );
        LongIterator nodesForLabel2 = storageReader.nodesGetForLabel( labelId2 );

        // THEN
        assertEquals( asSet( node1.getId(), node2.getId() ), PrimitiveLongCollections.toSet( nodesForLabel1 ) );
        assertEquals( asSet( node2.getId() ), PrimitiveLongCollections.toSet( nodesForLabel2 ) );
    }
}
