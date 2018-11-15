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

import org.junit.Test;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.storageengine.api.StorageNodeCursor;

import static org.eclipse.collections.impl.set.mutable.primitive.LongHashSet.newSetWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
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
}
