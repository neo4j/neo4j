/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
