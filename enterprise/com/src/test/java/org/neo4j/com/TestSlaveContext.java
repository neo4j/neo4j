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
package org.neo4j.com;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestSlaveContext
{
    @Test
    public void assertSimilarity()
    {
        // Different machine ids
        assertFalse( new RequestContext( 1234, 1, 2, 0, 0 ).equals( new RequestContext( 1234, 2, 2, 0, 0 ) ) );

        // Different event identifiers
        assertFalse( new RequestContext( 1234, 1, 10, 0, 0 ).equals( new RequestContext( 1234, 1, 20, 0, 0 ) ) );

        // Different session ids
        assertFalse( new RequestContext( 1001, 1, 5, 0, 0 ).equals( new RequestContext( 1101, 1, 5, 0, 0 ) ) );

        // Same everything
        assertEquals( new RequestContext( 12345, 4, 9, 0, 0 ), new RequestContext( 12345, 4, 9, 0, 0 ) );
    }
}
