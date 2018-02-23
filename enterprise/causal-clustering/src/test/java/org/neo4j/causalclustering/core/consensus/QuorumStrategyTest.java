/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.core.consensus;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.neo4j.causalclustering.core.consensus.MajorityIncludingSelfQuorum.isQuorum;

public class QuorumStrategyTest
{
    @Test
    public void shouldDecideIfWeHaveAMajorityCorrectly()
    {
        // the assumption in these tests is that we always vote for ourselves
        assertTrue( isQuorum( 0, 1, 0 ) );

        assertFalse( isQuorum( 0, 2, 0 ) );
        assertTrue( isQuorum( 0, 2, 1 ) );

        assertFalse( isQuorum( 0, 3, 0 ) );
        assertTrue( isQuorum( 0, 3, 1 ) );
        assertTrue( isQuorum( 0, 3, 2 ) );

        assertFalse( isQuorum( 0, 4, 0 ) );
        assertFalse( isQuorum( 0, 4, 1 ) );
        assertTrue( isQuorum( 0, 4, 2 ) );
        assertTrue( isQuorum( 0, 4, 3 ) );

        assertFalse( isQuorum( 0, 5, 0 ) );
        assertFalse( isQuorum( 0, 5, 1 ) );
        assertTrue( isQuorum( 0, 5, 2 ) );
        assertTrue( isQuorum( 0, 5, 3 ) );
        assertTrue( isQuorum( 0, 5, 4 ) );
    }

    @Test
    public void shouldDecideIfWeHaveAMajorityCorrectlyUsingMinQuorum()
    {
        // Then
        assertFalse( isQuorum( 2, 1, 0 ) );

        assertFalse( isQuorum( 2, 2, 0 ) );
        assertTrue( isQuorum( 2, 2, 1 ) );

        assertFalse( isQuorum( 2, 3, 0 ) );
        assertTrue( isQuorum( 2, 3, 1 ) );
        assertTrue( isQuorum( 2, 3, 2 ) );

        assertFalse( isQuorum( 2, 4, 0 ) );
        assertFalse( isQuorum( 2, 4, 1 ) );
        assertTrue( isQuorum( 2, 4, 2 ) );
        assertTrue( isQuorum( 2, 4, 3 ) );

        assertFalse( isQuorum( 2, 5, 0 ) );
        assertFalse( isQuorum( 2, 5, 1 ) );
        assertTrue( isQuorum( 2, 5, 2 ) );
        assertTrue( isQuorum( 2, 5, 3 ) );
        assertTrue( isQuorum( 2, 5, 4 ) );
    }
}
