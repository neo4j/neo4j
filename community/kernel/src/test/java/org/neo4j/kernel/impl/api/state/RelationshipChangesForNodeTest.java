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
package org.neo4j.kernel.impl.api.state;

import org.junit.Test;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import static org.neo4j.kernel.impl.api.store.RelationshipIterator.EMPTY;
public class RelationshipChangesForNodeTest
{
    private static final int REL_0 = 0;
    private static final int REL_1 = 1;
    private static final int TYPE_SELF = 0;
    private static final int TYPE_DIR = 1;

    @Test
    public void testOutgoingRelsWithTypeAndLoop() throws Exception
    {
        RelationshipChangesForNode changes = new RelationshipChangesForNode(
                RelationshipChangesForNode.DiffStrategy.ADD, mock( RelationshipVisitor.Home.class ) );
        changes.addRelationship( REL_0, TYPE_SELF, Direction.BOTH );
        changes.addRelationship( REL_1, TYPE_DIR, Direction.OUTGOING );

        RelationshipIterator iterator = changes.augmentRelationships(
                Direction.OUTGOING, new int[]{TYPE_DIR}, EMPTY );
        assertEquals( true, iterator.hasNext() );
        assertEquals( REL_1, iterator.next() );
        assertEquals( "should have no next relationships but has ", false, iterator.hasNext() );
    }
    @Test
    public void testIncomingRelsWithTypeAndLoop() throws Exception
    {
        RelationshipChangesForNode changes = new RelationshipChangesForNode(
                RelationshipChangesForNode.DiffStrategy.ADD, mock( RelationshipVisitor.Home.class ) );
        changes.addRelationship( REL_0, TYPE_SELF, Direction.BOTH );
        changes.addRelationship( REL_1, TYPE_DIR, Direction.INCOMING );

        RelationshipIterator iterator = changes.augmentRelationships(
                Direction.INCOMING, new int[]{TYPE_DIR}, EMPTY );
        assertEquals( true, iterator.hasNext() );
        assertEquals( REL_1, iterator.next() );
        assertEquals( "should have no next relationships but has ", false, iterator.hasNext() );
    }
}
