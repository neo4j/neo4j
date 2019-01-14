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
package org.neo4j.unsafe.impl.batchimport;

import org.junit.Test;

import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;

public class CalculateDenseNodesStepTest
{
    @Test
    public void shouldNotProcessLoopsTwice() throws Exception
    {
        // GIVEN
        NodeRelationshipCache cache = mock( NodeRelationshipCache.class );
        try ( CalculateDenseNodesStep step = new CalculateDenseNodesStep( mock( StageControl.class ),
                DEFAULT, cache ) )
        {
            step.processors( 4 );
            step.start( 0 );

            // WHEN
            long id = 0;
            RelationshipRecord[] batch = batch(
                    relationship( id++, 1, 5 ),
                    relationship( id++, 3, 10 ),
                    relationship( id++, 2, 2 ), // <-- the loop
                    relationship( id++, 4, 1 ) );
            step.receive( 0, batch );
            step.endOfUpstream();
            while ( !step.isCompleted() )
            {
                // wait
            }

            // THEN
            verify( cache, times( 2 ) ).incrementCount( eq( 1L ) );
            verify( cache, times( 1 ) ).incrementCount( eq( 2L ) );
            verify( cache, times( 1 ) ).incrementCount( eq( 3L ) );
            verify( cache, times( 1 ) ).incrementCount( eq( 4L ) );
            verify( cache, times( 1 ) ).incrementCount( eq( 5L ) );
            verify( cache, times( 1 ) ).incrementCount( eq( 10L ) );
        }
    }

    private static RelationshipRecord[] batch( RelationshipRecord... relationships )
    {
        return relationships;
    }

    private static RelationshipRecord relationship( long id, long startNodeId, long endNodeId )
    {
        return new RelationshipRecord( id ).initialize( true, Record.NO_NEXT_PROPERTY.longValue(),
                startNodeId, endNodeId, 0, NULL_REFERENCE.longValue(), NULL_REFERENCE.longValue(),
                NULL_REFERENCE.longValue(), NULL_REFERENCE.longValue(), false, false );
    }
}
