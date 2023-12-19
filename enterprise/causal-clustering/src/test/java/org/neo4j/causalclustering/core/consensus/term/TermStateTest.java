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
package org.neo4j.causalclustering.core.consensus.term;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TermStateTest
{
    @Test
    public void shouldStoreCurrentTerm()
    {
        // given
        TermState termState = new TermState();

        // when
        termState.update( 21 );

        // then
        assertEquals( 21, termState.currentTerm() );
    }

    @Test
    public void rejectLowerTerm()
    {
        // given
        TermState termState = new TermState();
        termState.update( 21 );

        // when
        try
        {
            termState.update( 20 );
            fail( "Should have thrown exception" );
        }
        catch ( IllegalArgumentException e )
        {
            // expected
        }
    }
}
