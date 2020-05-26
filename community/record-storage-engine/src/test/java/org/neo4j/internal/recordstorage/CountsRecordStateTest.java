/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.recordstorage;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

class CountsRecordStateTest
{
    @Test
    void trackCounts()
    {
        // given
        CountsRecordState counts = new CountsRecordState();
        counts.incrementNodeCount( 17, 5 );
        counts.incrementNodeCount( 12, 9 );
        counts.incrementRelationshipCount( 1, 2, 3, 19 );
        counts.incrementRelationshipCount( 1, 4, 3, 25 );

        assertEquals( 0, counts.nodeCount( 1, NULL ) );
        assertEquals( 5, counts.nodeCount( 17, NULL ) );
        assertEquals( 9, counts.nodeCount( 12, NULL ) );
        assertEquals( 19, counts.relationshipCount( 1, 2, 3, NULL ) );
        assertEquals( 25, counts.relationshipCount( 1, 4, 3, NULL ) );

        counts.incrementNodeCount( 17, 0 );
        counts.incrementNodeCount( 12, -2 );
        counts.incrementRelationshipCount( 1, 2, 3, 1 );
        counts.incrementRelationshipCount( 1, 4, 3, -25 );

        assertEquals( 5, counts.nodeCount( 17, NULL ) );
        assertEquals( 7, counts.nodeCount( 12, NULL ) );
        assertEquals( 20, counts.relationshipCount( 1, 2, 3, NULL ) );
        assertEquals( 0, counts.relationshipCount( 1, 4, 3, NULL ) );
    }
}
