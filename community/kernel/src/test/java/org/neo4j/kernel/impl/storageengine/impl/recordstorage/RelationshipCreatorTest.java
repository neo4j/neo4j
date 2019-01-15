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

import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.NoOpClient;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.state.RecordChangeSet;

import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordBuilders.filterType;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordBuilders.firstIn;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordBuilders.firstOut;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordBuilders.from;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordBuilders.group;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordBuilders.firstLoop;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordBuilders.newChangeSet;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordBuilders.newRelGroupGetter;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordBuilders.nextRel;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordBuilders.node;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordBuilders.owningNode;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordBuilders.rel;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordBuilders.relGroup;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordBuilders.sCount;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordBuilders.sNext;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordBuilders.sPrev;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordBuilders.tCount;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordBuilders.tNext;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordBuilders.tPrev;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordBuilders.to;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordMatchers.containsChanges;

public class RelationshipCreatorTest
{
    private AbstractBaseRecord[] givenState;
    private RecordChangeSet changeset;
    private int denseNodeThreshold = 10;

    @Test
    public void newRelWithNoPriorRels() throws Exception
    {
        givenState(
                node( 0 ),
                node( 1 )
        );

        createRelationshipBetween( 0, 1 );

        assertThat( changeset, containsChanges(
                node( 0, nextRel( 0 ) ),
                node( 1, nextRel( 0 ) ),
                rel( 0, from( 0 ), to( 1 ), sCount( 1 ), tCount( 1 ) )
        ) );
    }

    @Test
    public void selfRelWithNoPriorRels() throws Exception
    {
        givenState(
                node( 0 )
        );

        createRelationshipBetween( 0, 0 );

        assertThat( changeset, containsChanges(
                node( 0, nextRel( 0 ) ),
                rel( 0, from( 0 ), to( 0 ), sCount( 1 ), tCount( 1 ) )
        ) );
    }

    @Test
    public void sourceHas1PriorRel() throws Exception
    {
        givenState(
                node( 0, nextRel( 0 ) ),
                node( 1, nextRel( 0 ) ),
                node( 2 ), // target node
                rel( 0, from( 0 ), to( 1 ), sCount( 1 ), tCount( 1 ) )
        );

        createRelationshipBetween( 0, 2 );

        assertThat( changeset, containsChanges(
                node( 0, nextRel( 1 ) ),
                node( 2, nextRel( 1 ) ),
                rel( 0, from( 0 ), to( 1 ), sPrev( 1 ), tCount( 1 ) ),
                rel( 1, from( 0 ), to( 2 ), sCount( 2 ), sNext( 0 ), tCount( 1 ) )
        ) );
    }

    @Test
    public void targetHas1PriorRel() throws Exception
    {
        givenState(
                node( 0, nextRel( 0 ) ),
                node( 1, nextRel( 0 ) ),
                node( 2 ), // source node
                rel( 0, from( 0 ), to( 1 ), sCount( 1 ), tCount( 1 ) )
        );

        createRelationshipBetween( 2, 0 );

        assertThat( changeset, containsChanges(
                node( 0, nextRel( 1 ) ),
                node( 2, nextRel( 1 ) ),
                rel( 0, from( 0 ), to( 1 ), sPrev( 1 ), tCount( 1 ) ),
                rel( 1, from( 2 ), to( 0 ), sCount( 1 ), tNext( 0 ), tCount( 2 ) )
        ) );
    }

    @Test
    public void sourceAndTargetShare1PriorRel() throws Exception
    {
        givenState(
                node( 0, nextRel( 0 ) ),
                node( 1, nextRel( 0 ) ),
                rel( 0, from( 0 ), to( 1 ), sCount( 1 ), tCount( 1 ) )
        );

        createRelationshipBetween( 0, 1 );

        assertThat( changeset, containsChanges(
                node( 0, nextRel( 1 ) ),
                node( 1, nextRel( 1 ) ),
                rel( 0, from( 0 ), to( 1 ), sPrev( 1 ), tPrev( 1 ) ),
                rel( 1, from( 0 ), to( 1 ), sCount( 2 ), sNext( 0 ), tCount( 2 ),
                        tNext( 0 ) )
        ) );
    }

    @Test
    public void selfRelWith1PriorRel() throws Exception
    {
        givenState(
                node( 0, nextRel( 0 ) ),
                rel( 0, from( 0 ), to( 0 ), sCount( 1 ), tCount( 1 ) )
        );

        createRelationshipBetween( 0, 0 );

        assertThat( changeset, containsChanges(
                node( 0, nextRel( 1 ) ),
                rel( 0, from( 0 ), to( 0 ), sPrev( 1 ), tPrev( 1 ) ),
                rel( 1, from( 0 ), to( 0 ), sCount( 2 ), sNext( 0 ), tCount( 2 ),
                        tNext( 0 ) )
        ) );
    }

    @Test
    public void selfRelUpgradesToDense() throws Exception
    {
        givenState(
                node( 0, nextRel( 0 ) ),
                rel( 0, from( 0 ), to( 0 ), sCount( 1 ), tCount( 1 ) )
        );

        denseNodeThreshold = 1;
        createRelationshipBetween( 0, 0 );

        assertThat( changeset, containsChanges(
            node( 0, group( 0 ) ),
            relGroup( 0, owningNode( 0 ), firstLoop( 1 ) ),
            rel( 0, from( 0 ), to( 0 ), sPrev( 1 ), tPrev( 1 ) ),
            rel( 1, from( 0 ), to( 0 ), sCount( 2 ), sNext( 0 ), tCount( 2 ), tNext( 0 ) )
        ) );
    }

    @Test
    public void sourceNodeUpdatesToDense() throws Exception
    {
        givenState(
                node( 0, nextRel( 0 ) ),
                node( 1 ),
                rel( 0, from( 0 ), to( 0 ), sCount( 1 ), tCount( 1 ) )
        );

        denseNodeThreshold = 1;
        createRelationshipBetween( 0, 1 );

        assertThat( changeset, containsChanges(
                node( 0, group( 0 ) ),
                node( 1, nextRel( 1 ) ),
                relGroup( 0, owningNode( 0 ), firstLoop( 0 ), firstOut( 1 ) ),
                rel( 0, from( 0 ), to( 0 ), sCount( 1 ), tCount( 1 ) ),
                rel( 1, from( 0 ), to( 1 ), sCount( 1 ), tCount( 1 ) )
        ) );
    }

    @Test
    public void targetNodeUpdatesToDense() throws Exception
    {
        givenState(
                node( 0, nextRel( 0 ) ),
                node( 1 ),
                rel( 0, from( 0 ), to( 0 ), sCount( 1 ), tCount( 1 ) )
        );

        denseNodeThreshold = 1;
        createRelationshipBetween( 1, 0 );

        assertThat( changeset, containsChanges(
                node( 0, group( 0 ) ),
                node( 1, nextRel( 1 ) ),
                relGroup( 0, owningNode( 0 ), firstLoop( 0 ), firstIn( 1 ) ),
                rel( 0, from( 0 ), to( 0 ), sCount( 1 ), tCount( 1 ) ),
                rel( 1, from( 1 ), to( 0 ), sCount( 1 ), tCount( 1 ) )
        ) );
    }

    private void givenState( AbstractBaseRecord... records )
    {
        givenState = records;
        changeset = newChangeSet( givenState );
    }

    private void createRelationshipBetween( long fromNode, long toNode )
    {
        Locks.Client locks = new NoOpClient();
        RelationshipCreator logic =
                new RelationshipCreator( newRelGroupGetter( givenState ), denseNodeThreshold );

        logic.relationshipCreate( nextRelId( givenState ), 0, fromNode, toNode, changeset, locks );
    }

    private long nextRelId( AbstractBaseRecord[] existingRecords )
    {
        return filterType( existingRecords, RelationshipRecord.class ).map(
                AbstractBaseRecord::getId ).max( Long::compareTo ).orElse( -1L ) + 1;
    }
}
