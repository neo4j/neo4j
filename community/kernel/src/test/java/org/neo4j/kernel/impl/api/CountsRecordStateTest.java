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
package org.neo4j.kernel.impl.api;

import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.nodeKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.relationshipKey;

public class CountsRecordStateTest
{
    @Test
    public void shouldReportDifferencesBetweenDifferentStates() throws Exception
    {
        // given
        CountsRecordState oracle = new CountsRecordState();
        CountsRecordState victim = new CountsRecordState();
        oracle.incrementNodeCount( 17, 5 );
        victim.incrementNodeCount( 17, 3 );
        oracle.incrementNodeCount( 12, 9 );
        victim.incrementNodeCount( 12, 9 );
        oracle.incrementRelationshipCount( 1, 2, 3, 19 );
        victim.incrementRelationshipCount( 1, 2, 3, 22 );
        oracle.incrementRelationshipCount( 1, 4, 3, 25 );
        victim.incrementRelationshipCount( 1, 4, 3, 25 );

        // when
        Set<CountsRecordState.Difference> differences = asSet( oracle.verify( victim ) );

        // then
        assertEquals( differences, asSet(
                new CountsRecordState.Difference( nodeKey( 17 ), 0, 5, 0, 3 ),
                new CountsRecordState.Difference( relationshipKey( 1, 2, 3 ), 0, 19, 0, 22 )
        ) );
    }

    @Test
    public void shouldNotReportAnythingForEqualStates() throws Exception
    {
        // given
        CountsRecordState oracle = new CountsRecordState();
        CountsRecordState victim = new CountsRecordState();
        oracle.incrementNodeCount( 17, 5 );
        victim.incrementNodeCount( 17, 5 );
        oracle.incrementNodeCount( 12, 9 );
        victim.incrementNodeCount( 12, 9 );
        oracle.incrementRelationshipCount( 1, 4, 3, 25 );
        victim.incrementRelationshipCount( 1, 4, 3, 25 );

        // when
        List<CountsRecordState.Difference> differences = oracle.verify( victim );

        // then
        assertTrue( differences.toString(), differences.isEmpty() );
    }
}
