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
package org.neo4j.consistency.checking.full;

import org.junit.Test;

import org.neo4j.consistency.checking.full.QueueDistribution.QueueDistributor;
import org.neo4j.consistency.checking.full.QueueDistribution.RelationshipNodesQueueDistributor;
import org.neo4j.consistency.checking.full.RecordDistributor.RecordConsumer;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RecordDistributorTest
{
    @Test
    public void shouldDistributeRelationshipRecordsByNodeId() throws Exception
    {
        // GIVEN
        QueueDistributor<RelationshipRecord> distributor = new RelationshipNodesQueueDistributor( 5 );
        RecordConsumer<RelationshipRecord> consumer = mock( RecordConsumer.class );

        // WHEN/THEN
        RelationshipRecord relationship = relationship( 0, 0, 1 );
        distributor.distribute( relationship, consumer );
        verify( consumer, times( 1 ) ).accept( relationship, 0 );

        relationship = relationship( 1, 0, 7 );
        distributor.distribute( relationship, consumer );
        verify( consumer, times( 1 ) ).accept( relationship, 0 );
        verify( consumer, times( 1 ) ).accept( relationship, 1 );

        relationship = relationship( 3, 26, 11 );
        distributor.distribute( relationship, consumer );
        verify( consumer, times( 1 ) ).accept( relationship, 5 );
        verify( consumer, times( 1 ) ).accept( relationship, 2 );
    }

    private RelationshipRecord relationship( long id, long startNodeId, long endNodeId )
    {
        RelationshipRecord record = new RelationshipRecord( id );
        record.setInUse( true );
        record.setFirstNode( startNodeId );
        record.setSecondNode( endNodeId );
        return record;
    }
}
