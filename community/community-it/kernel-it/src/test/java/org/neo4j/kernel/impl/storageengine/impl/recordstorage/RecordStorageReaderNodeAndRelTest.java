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

import org.junit.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * Test reading committed node and relationships from disk.
 */
public class RecordStorageReaderNodeAndRelTest extends RecordStorageReaderTestBase
{
    @Test
    public void shouldTellIfNodeExists()
    {
        // Given
        long created = createLabeledNode( db, map() ).getId();
        long createdAndRemoved = createLabeledNode( db, map() ).getId();
        long neverExisted = createdAndRemoved + 99;

        try ( Transaction tx = db.beginTx() )
        {
            db.getNodeById( createdAndRemoved ).delete();
            tx.success();
        }

        // When & then
        assertTrue(  nodeExists( created ));
        assertFalse( nodeExists( createdAndRemoved ) );
        assertFalse( nodeExists( neverExisted ) );
    }

    @Test
    public void shouldTellIfRelExists()
    {
        // Given
        long node = createLabeledNode( db, map() ).getId();
        long created;
        long createdAndRemoved;
        long neverExisted;

        try ( Transaction tx = db.beginTx() )
        {
            created = db.createNode().createRelationshipTo( db.createNode(), withName( "Banana" ) ).getId();
            createdAndRemoved = db.createNode().createRelationshipTo( db.createNode(), withName( "Banana" ) ).getId();
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.getRelationshipById( createdAndRemoved ).delete();
            tx.success();
        }

        neverExisted = created + 99;

        // When & then
        assertTrue(  relationshipExists( node ));
        assertFalse( relationshipExists( createdAndRemoved ) );
        assertFalse( relationshipExists( neverExisted ) );
    }

    private boolean nodeExists( long id )
    {
        try ( StorageNodeCursor node = storageReader.allocateNodeCursor() )
        {
            node.single( id );
            return node.next();
        }
    }

    private boolean relationshipExists( long id )
    {
        try ( StorageRelationshipScanCursor relationship = storageReader.allocateRelationshipScanCursor() )
        {
            relationship.single( id );
            return relationship.next();
        }
    }
}
