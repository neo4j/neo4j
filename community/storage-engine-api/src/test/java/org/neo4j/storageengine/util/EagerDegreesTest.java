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
package org.neo4j.storageengine.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

@ExtendWith( RandomExtension.class )
class EagerDegreesTest
{
    @Inject
    private RandomRule random;

    @Test
    void shouldReplyZeroOnEmptyDegrees()
    {
        // given
        EagerDegrees degrees = new EagerDegrees();

        // when/then
        assertEquals( 0, degrees.degree( 0, OUTGOING ) );
        assertEquals( 0, degrees.degree( OUTGOING ) );
        assertEquals( 0, degrees.totalDegree() );
    }

    @Test
    void shouldReplyZeroForUnknownTypeOrDirection()
    {
        // given
        int type = 10;
        EagerDegrees degrees = new EagerDegrees();
        degrees.add( type, 20, 21, 22 );

        // when/then
        assertEquals( 0, degrees.degree( 5, OUTGOING ) );
        assertEquals( 0, degrees.degree( 3, INCOMING ) );
        assertEquals( 0, degrees.degree( 2, BOTH ) );
        assertEquals( 0, degrees.outgoingDegree( 2 ) );
        assertEquals( 0, degrees.incomingDegree( 1 ) );
        assertEquals( 0, degrees.totalDegree( 0 ) );
    }

    @Test
    void shouldGetDegreeForSingleType()
    {
        // given
        int type = 10;
        EagerDegrees degrees = new EagerDegrees();
        degrees.add( type, 20, 21, 22 );

        // when/then
        assertEquals( 42, degrees.outgoingDegree( type ) );
        assertEquals( 43, degrees.incomingDegree( type ) );
        assertEquals( 63, degrees.totalDegree( type ) );
        assertEquals( 63, degrees.totalDegree() );
    }

    @Test
    void shouldGetDegreeForMultipleType()
    {
        // given
        int numberOfTypes = random.nextInt( 3, 10 );
        int[][] expectedDegrees = new int[numberOfTypes][];
        int[] types = new int[numberOfTypes];
        for ( int i = 0, prevType = 0; i < expectedDegrees.length; i++ )
        {
            expectedDegrees[i] = new int[3];
            types[i] = prevType + random.nextInt( 1, 100 );
            prevType = types[i];
        }

        // when
        EagerDegrees degrees = new EagerDegrees();
        for ( int i = 0; i < 100; i++ )
        {
            int typeIndex = random.nextInt( numberOfTypes );
            int type = types[typeIndex];
            int outgoing = random.nextInt( 1, 10 );
            int incoming = random.nextInt( 1, 10 );
            int loop = random.nextInt( 1, 10 );
            switch ( random.nextInt( 4 ) )
            {
            case 0:  // add all
                degrees.add( type, outgoing, incoming, loop );
                expectedDegrees[typeIndex][0] += outgoing;
                expectedDegrees[typeIndex][1] += incoming;
                expectedDegrees[typeIndex][2] += loop;
                break;
            case 1:  // add outgoing
                degrees.addOutgoing( type, outgoing );
                expectedDegrees[typeIndex][0] += outgoing;
                break;
            case 2:  // add incoming
                degrees.addIncoming( type, incoming );
                expectedDegrees[typeIndex][1] += incoming;
                break;
            default: // add loop
                degrees.addLoop( type, loop );
                expectedDegrees[typeIndex][2] += loop;
                break;
            }
        }

        // then
        for ( int i = 0; i < numberOfTypes; i++ )
        {
            int type = types[i];
            assertEquals( expectedDegrees[i][0] + expectedDegrees[i][2], degrees.outgoingDegree( type ) );
            assertEquals( expectedDegrees[i][1] + expectedDegrees[i][2], degrees.incomingDegree( type ) );
            assertEquals( expectedDegrees[i][0] + expectedDegrees[i][1] + expectedDegrees[i][2], degrees.totalDegree( type ) );
        }
    }
}
