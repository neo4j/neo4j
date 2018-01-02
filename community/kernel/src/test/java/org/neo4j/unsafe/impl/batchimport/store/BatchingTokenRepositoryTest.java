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
package org.neo4j.unsafe.impl.batchimport.store;

import org.junit.Test;

import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingLabelTokenRepository;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class BatchingTokenRepositoryTest
{
    @Test
    public void shouldDedupLabelIds() throws Exception
    {
        // GIVEN
        BatchingLabelTokenRepository repo = new BatchingLabelTokenRepository( null, 0 );

        // WHEN
        long[] ids = repo.getOrCreateIds( new String[] {"One", "Two", "One"} );

        // THEN
        assertTrue( NodeLabelsField.isSane( ids ) );
    }

    @Test
    public void shouldSortLabelIds() throws Exception
    {
        // GIVEN
        BatchingLabelTokenRepository repo = new BatchingLabelTokenRepository( null, 0 );
        long[] expected = new long[] {
                repo.getOrCreateId( "One" ),
                repo.getOrCreateId( "Two" ),
                repo.getOrCreateId( "Three" )
        };

        // WHEN
        long[] ids = repo.getOrCreateIds( new String[] {"Two", "One", "Three"} );

        // THEN
        assertArrayEquals( expected, ids );
        assertTrue( NodeLabelsField.isSane( ids ) );
    }
}
