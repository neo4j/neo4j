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
package org.neo4j.kernel.impl.api.state;

import org.hamcrest.Matcher;
import org.junit.Test;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.newapi.RelationshipDirection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.api.store.RelationshipIterator.EMPTY;
import static org.neo4j.storageengine.api.Direction.BOTH;
import static org.neo4j.storageengine.api.Direction.INCOMING;
import static org.neo4j.storageengine.api.Direction.OUTGOING;

public class RelationshipChangesForNodeTest
{
    private static final int REL_0 = 0;
    private static final int REL_1 = 1;
    private static final int TYPE_SELF = 0;
    private static final int TYPE_DIR = 1;

    @Test
    public void testOutgoingRelsWithTypeAndLoop()
    {
        RelationshipChangesForNode changes = new RelationshipChangesForNode(
                RelationshipChangesForNode.DiffStrategy.ADD, mock( RelationshipVisitor.Home.class ) );
        changes.addRelationship( REL_0, TYPE_SELF, BOTH );
        changes.addRelationship( REL_1, TYPE_DIR, OUTGOING );

        RelationshipIterator iterator = changes.augmentRelationships( OUTGOING, new int[]{TYPE_DIR}, EMPTY );
        assertEquals( true, iterator.hasNext() );
        assertEquals( REL_1, iterator.next() );
        assertEquals( "should have no next relationships but has ", false, iterator.hasNext() );
    }

    @Test
    public void testIncomingRelsWithTypeAndLoop()
    {
        RelationshipChangesForNode changes = new RelationshipChangesForNode(
                RelationshipChangesForNode.DiffStrategy.ADD, mock( RelationshipVisitor.Home.class ) );
        changes.addRelationship( REL_0, TYPE_SELF, BOTH );
        changes.addRelationship( REL_1, TYPE_DIR, INCOMING );

        RelationshipIterator iterator = changes.augmentRelationships( INCOMING, new int[]{TYPE_DIR}, EMPTY );
        assertEquals( true, iterator.hasNext() );
        assertEquals( REL_1, iterator.next() );
        assertEquals( "should have no next relationships but has ", false, iterator.hasNext() );
    }

    @Test
    public void shouldGetRelationships()
    {
        RelationshipChangesForNode changes = new RelationshipChangesForNode(
                RelationshipChangesForNode.DiffStrategy.ADD, mock( RelationshipVisitor.Home.class ) );

        final int TYPE = 2;

        changes.addRelationship( 1, TYPE, INCOMING );
        changes.addRelationship( 2, TYPE, OUTGOING );
        changes.addRelationship( 3, TYPE, OUTGOING );
        changes.addRelationship( 4, TYPE, BOTH );
        changes.addRelationship( 5, TYPE, BOTH );
        changes.addRelationship( 6, TYPE, BOTH );

        PrimitiveLongIterator rawRelationships = changes.getRelationships();
        assertThat( PrimitiveLongCollections.asArray( rawRelationships ), ids( 1, 2, 3, 4, 5, 6 ) );
    }

    @Test
    public void shouldGetRelationshipsByTypeAndDirection()
    {
        RelationshipChangesForNode changes = new RelationshipChangesForNode(
                RelationshipChangesForNode.DiffStrategy.ADD, mock( RelationshipVisitor.Home.class ) );

        final int TYPE = 2;
        final int DECOY_TYPE = 666;

        changes.addRelationship( 1, TYPE, INCOMING );
        changes.addRelationship( 2, TYPE, OUTGOING );
        changes.addRelationship( 3, TYPE, OUTGOING );
        changes.addRelationship( 4, TYPE, BOTH );
        changes.addRelationship( 5, TYPE, BOTH );
        changes.addRelationship( 6, TYPE, BOTH );

        changes.addRelationship( 10, DECOY_TYPE, INCOMING );
        changes.addRelationship( 11, DECOY_TYPE, OUTGOING );
        changes.addRelationship( 12, DECOY_TYPE, BOTH );

        PrimitiveLongIterator rawIncoming =
                changes.getRelationships( RelationshipDirection.INCOMING, TYPE );
        assertThat( PrimitiveLongCollections.asArray( rawIncoming ), ids( 1 ) );

        PrimitiveLongIterator rawOutgoing =
                changes.getRelationships( RelationshipDirection.OUTGOING, TYPE );
        assertThat( PrimitiveLongCollections.asArray( rawOutgoing ), ids( 2, 3 ) );

        PrimitiveLongIterator rawLoops =
                changes.getRelationships( RelationshipDirection.LOOP, TYPE );
        assertThat( PrimitiveLongCollections.asArray( rawLoops ), ids( 4, 5, 6 ) );
    }

    private Matcher<long[]> ids( long... ids )
    {
        return equalTo( ids );
    }
}
