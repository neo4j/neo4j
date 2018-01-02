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
package org.neo4j.legacy.consistency.repair;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.legacy.consistency.repair.OwningNodeRelationshipChain;
import org.neo4j.legacy.consistency.repair.RecordSet;
import org.neo4j.legacy.consistency.repair.RelationshipChainExplorer;

import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;

public class OwningNodeRelationshipChainTest
{
    @Test
    public void shouldFindBothChainsThatTheRelationshipRecordShouldBelongTo() throws Exception
    {
        // given
        int node1 = 101, node1Rel = 1001;
        int node2 = 201, node2Rel = 2001;
        int sharedRel = 1000;
        int relType = 0;

        RecordSet<RelationshipRecord> node1RelChain = RecordSet.asSet(
                new RelationshipRecord( node1Rel, node1, node1 - 1, relType ),
                new RelationshipRecord( sharedRel, node1, node2, relType ),
                new RelationshipRecord( node1Rel + 1, node1 + 1, node1, relType ) );
        RecordSet<RelationshipRecord> node2RelChain = RecordSet.asSet(
                new RelationshipRecord( node2Rel, node2 - 1, node2, relType ),
                new RelationshipRecord( sharedRel, node1, node2, relType ),
                new RelationshipRecord( node2Rel + 1, node2, node2 + 1, relType ) );

        @SuppressWarnings("unchecked")
        RecordStore<NodeRecord> recordStore = mock( RecordStore.class );
        when( recordStore.forceGetRecord( node1 ) ).thenReturn(
                new NodeRecord( node1, false, node1Rel, NO_NEXT_PROPERTY.intValue() ) );
        when( recordStore.forceGetRecord( node2 ) ).thenReturn(
                new NodeRecord( node2, false, node2Rel, NO_NEXT_PROPERTY.intValue() ) );

        RelationshipChainExplorer relationshipChainExplorer = mock( RelationshipChainExplorer.class );
        when( relationshipChainExplorer.followChainFromNode( node1, node1Rel ) ).thenReturn( node1RelChain );
        when( relationshipChainExplorer.followChainFromNode( node2, node2Rel ) ).thenReturn( node2RelChain );

        OwningNodeRelationshipChain owningChainFinder =
                new OwningNodeRelationshipChain( relationshipChainExplorer, recordStore );

        // when
        RecordSet<RelationshipRecord> recordsInChains = owningChainFinder
                .findRelationshipChainsThatThisRecordShouldBelongTo( new RelationshipRecord( sharedRel, node1, node2,
                        relType ) );

        // then
        assertThat( recordsInChains, containsAllRecords( node1RelChain ) );
        assertThat( recordsInChains, containsAllRecords( node2RelChain ) );
    }

    private Matcher<RecordSet<RelationshipRecord>> containsAllRecords( final RecordSet<RelationshipRecord> expectedSet )
    {
        return new TypeSafeMatcher<RecordSet<RelationshipRecord>>()
        {
            @Override
            public boolean matchesSafely( RecordSet<RelationshipRecord> actualSet )
            {
                return actualSet.containsAll( expectedSet );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "RecordSet containing " ).appendValueList( "[", ",", "]", expectedSet );
            }
        };
    }


}
