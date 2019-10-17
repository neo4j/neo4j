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
package org.neo4j.index.internal.gbptree;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class SizeEstimationMonitorTest
{
    @Test
    void shouldReportInconsistentIfTreeDepthsAreDifferent()
    {
        // given
        SizeEstimationMonitor monitor = new SizeEstimationMonitor();

        // when
        monitor.internalNode( 0, 10 );
        monitor.internalNode( 1, 15 );
        monitor.leafNode( 2, 5 );

        monitor.internalNode( 0, 9 );
        monitor.leafNode( 1, 7 );

        // then
        assertFalse( monitor.isConsistent() );
    }

    @Test
    void shouldCalculateEstimate()
    {
        // given
        SizeEstimationMonitor monitor = new SizeEstimationMonitor();

        // when
        // first sample
        monitor.internalNode( 0, 5 ); // which means a root with 5 keys (and 6 children pointers)
        monitor.internalNode( 1, 8 ); // which means an internal node at depth 1 with 8 keys and (9 children pointers)
        monitor.leafNode( 2, 10 );
        // second sample
        monitor.internalNode( 0, 5 );
        monitor.internalNode( 1, 10 ); // which means an internal node at depth 1 with 10 keys and (11 children pointers)
        monitor.leafNode( 2, 20 );

        // then
        double expectedEstimate = 6D * 10 * 15;
        assertEquals( (long) expectedEstimate, monitor.estimateNumberOfKeys() );
    }
}
