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

import org.eclipse.collections.api.iterator.LongIterator;
import org.hamcrest.Matcher;
import org.junit.Test;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.storageengine.api.RelationshipDirection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;

public class RelationshipChangesForNodeTest
{

    @Test
    public void shouldGetRelationships()
    {
        RelationshipChangesForNode changes = new RelationshipChangesForNode(
                RelationshipChangesForNode.DiffStrategy.ADD );

        final int TYPE = 2;

        changes.addRelationship( 1, TYPE, INCOMING );
        changes.addRelationship( 2, TYPE, OUTGOING );
        changes.addRelationship( 3, TYPE, OUTGOING );
        changes.addRelationship( 4, TYPE, LOOP );
        changes.addRelationship( 5, TYPE, LOOP );
        changes.addRelationship( 6, TYPE, LOOP );

        LongIterator rawRelationships = changes.getRelationships();
        assertThat( PrimitiveLongCollections.asArray( rawRelationships ), ids( 1, 2, 3, 4, 5, 6 ) );
    }

    @Test
    public void shouldGetRelationshipsByTypeAndDirection()
    {
        RelationshipChangesForNode changes = new RelationshipChangesForNode(
                RelationshipChangesForNode.DiffStrategy.ADD );

        final int TYPE = 2;
        final int DECOY_TYPE = 666;

        changes.addRelationship( 1, TYPE, INCOMING );
        changes.addRelationship( 2, TYPE, OUTGOING );
        changes.addRelationship( 3, TYPE, OUTGOING );
        changes.addRelationship( 4, TYPE, LOOP );
        changes.addRelationship( 5, TYPE, LOOP );
        changes.addRelationship( 6, TYPE, LOOP );

        changes.addRelationship( 10, DECOY_TYPE, INCOMING );
        changes.addRelationship( 11, DECOY_TYPE, OUTGOING );
        changes.addRelationship( 12, DECOY_TYPE, LOOP );

        LongIterator rawIncoming =
                changes.getRelationships( RelationshipDirection.INCOMING, TYPE );
        assertThat( PrimitiveLongCollections.asArray( rawIncoming ), ids( 1 ) );

        LongIterator rawOutgoing =
                changes.getRelationships( RelationshipDirection.OUTGOING, TYPE );
        assertThat( PrimitiveLongCollections.asArray( rawOutgoing ), ids( 2, 3 ) );

        LongIterator rawLoops =
                changes.getRelationships( RelationshipDirection.LOOP, TYPE );
        assertThat( PrimitiveLongCollections.asArray( rawLoops ), ids( 4, 5, 6 ) );
    }

    private Matcher<long[]> ids( long... ids )
    {
        return equalTo( ids );
    }
}
