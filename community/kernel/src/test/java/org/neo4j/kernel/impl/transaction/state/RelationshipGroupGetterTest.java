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
package org.neo4j.kernel.impl.transaction.state;

import org.junit.Test;
import org.mockito.InOrder;

import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.transaction.state.Loaders;
import org.neo4j.kernel.impl.transaction.state.RecordAccess;
import org.neo4j.kernel.impl.transaction.state.RelationshipGroupGetter;
import org.neo4j.kernel.impl.transaction.state.RelationshipGroupGetter.RelationshipGroupPosition;
import org.neo4j.unsafe.batchinsert.DirectRecordAccess;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RelationshipGroupGetterTest
{
    @Test
    public void shouldAbortLoadingGroupChainIfComeTooFar() throws Exception
    {
        // GIVEN a node with relationship group chain 2-->4-->10-->23
        RelationshipGroupStore store = mock( RelationshipGroupStore.class );
        RelationshipGroupGetter groupGetter = new RelationshipGroupGetter( store );
        RelationshipGroupRecord group_2 = new RelationshipGroupRecord( 0, 2 );
        RelationshipGroupRecord group_4 = new RelationshipGroupRecord( 1, 4 );
        RelationshipGroupRecord group_10 = new RelationshipGroupRecord( 2, 10 );
        RelationshipGroupRecord group_23 = new RelationshipGroupRecord( 3, 23 );
        linkAndMock( store, group_2, group_4, group_10, group_23 );
        NodeRecord node = new NodeRecord( 0, true, group_2.getId(), -1 );

        // WHEN trying to find relationship group 7
        RecordAccess<Long, RelationshipGroupRecord, Integer> access =
                new DirectRecordAccess<>( store, Loaders.relationshipGroupLoader( store ) );
        RelationshipGroupPosition result = groupGetter.getRelationshipGroup( node, 7, access );

        // THEN only groups 2, 4 and 10 should have been loaded
        InOrder verification = inOrder( store );
        verification.verify( store ).getRecord( group_2.getId() );
        verification.verify( store ).getRecord( group_4.getId() );
        verification.verify( store ).getRecord( group_10.getId() );
        verification.verifyNoMoreInteractions();

        // it should also be reported as not found
        assertNull( result.group() );
        // with group 4 as closes previous one
        assertEquals( group_4, result.closestPrevious().forReadingData() );
    }

    private void linkAndMock( RelationshipGroupStore store, RelationshipGroupRecord... groups )
    {
        for ( int i = 0; i < groups.length; i++ )
        {
            when( store.getRecord( groups[i].getId() ) ).thenReturn( groups[i] );
            if ( i > 0 )
            {
                groups[i].setPrev( groups[i-1].getId() );
            }
            if ( i < groups.length-1 )
            {
                groups[i].setNext( groups[i+1].getId() );
            }
        }
    }
}
