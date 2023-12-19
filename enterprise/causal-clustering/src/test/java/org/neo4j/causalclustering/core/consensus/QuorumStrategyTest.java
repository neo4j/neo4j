/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
