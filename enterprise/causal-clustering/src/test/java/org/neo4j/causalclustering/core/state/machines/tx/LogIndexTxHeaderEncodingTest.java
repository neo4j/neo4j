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
package org.neo4j.causalclustering.core.state.machines.tx;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding.decodeLogIndexFromTxHeader;
import static org.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader;

public class LogIndexTxHeaderEncodingTest
{
    @Test
    public void shouldEncodeIndexAsBytes()
    {
        long index = 123_456_789_012_567L;
        byte[] bytes = encodeLogIndexAsTxHeader( index );
        assertEquals( index, decodeLogIndexFromTxHeader( bytes ) );
    }

    @Test
    public void shouldThrowExceptionForAnEmptyByteArray()
    {
        // given
        try
        {
            // when
            decodeLogIndexFromTxHeader( new byte[0] );
            fail( "Should have thrown an exception because there's no way to decode this " );
        }
        catch ( IllegalArgumentException e )
        {
            // expected
            assertEquals( "Unable to decode RAFT log index from transaction header", e.getMessage() );
        }
    }
}
