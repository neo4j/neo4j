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
package org.neo4j.internal.recordstorage;

import org.junit.Test;

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
    public void shouldTellIfNodeExists() throws Exception
    {
        // Given
        long created = createNode( map() );
        long createdAndRemoved = createNode( map() );
        long neverExisted = createdAndRemoved + 99;

        deleteNode( createdAndRemoved );

        // When & then
        assertTrue(  nodeExists( created ));
        assertFalse( nodeExists( createdAndRemoved ) );
        assertFalse( nodeExists( neverExisted ) );
    }

    @Test
    public void shouldTellIfRelExists() throws Exception
    {
        // Given
        long node = createNode( map() );
        long created = createRelationship( createNode( map() ), createNode( map() ), withName( "Banana" ) );
        long createdAndRemoved = createRelationship( createNode( map() ), createNode( map() ), withName( "Banana" ) );
        long neverExisted = created + 99;

        deleteRelationship( createdAndRemoved );

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
