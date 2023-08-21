/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.storageengine.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class EagerDegreesTest {
    @Inject
    private RandomSupport random;

    @Test
    void shouldReplyZeroOnEmptyDegrees() {
        // given
        EagerDegrees degrees = new EagerDegrees();

        // when/then
        assertThat(degrees.degree(0, OUTGOING)).isZero();
        assertThat(degrees.degree(OUTGOING)).isZero();
        assertThat(degrees.totalDegree()).isZero();
    }

    @Test
    void shouldReplyZeroForUnknownTypeOrDirection() {
        // given
        int type = 10;
        EagerDegrees degrees = new EagerDegrees();
        degrees.add(type, 20, 21, 22);

        // when/then
        assertThat(degrees.degree(5, OUTGOING)).isZero();
        assertThat(degrees.degree(3, INCOMING)).isZero();
        assertThat(degrees.degree(2, BOTH)).isZero();
        assertThat(degrees.outgoingDegree(2)).isZero();
        assertThat(degrees.incomingDegree(1)).isZero();
        assertThat(degrees.totalDegree(0)).isZero();
        assertThat(degrees.hasType(type)).isTrue();
        assertThat(degrees.hasType(type + 1)).isFalse();
    }

    @Test
    void shouldGetDegreeForSingleType() {
        // given
        int type = 10;
        EagerDegrees degrees = new EagerDegrees();
        degrees.add(type, 20, 21, 22);

        // when/then
        assertThat(degrees.outgoingDegree(type)).isEqualTo(42);
        assertThat(degrees.incomingDegree(type)).isEqualTo(43);
        assertThat(degrees.totalDegree(type)).isEqualTo(63);
        assertThat(degrees.totalDegree()).isEqualTo(63);
    }

    @Test
    void shouldAddWithDirectionMethod() {
        // given
        int type = 99;
        int outgoing = 5;
        int incoming = 6;
        int loop = 7;
        EagerDegrees degrees = new EagerDegrees();

        // when
        degrees.add(type, RelationshipDirection.OUTGOING, outgoing);
        degrees.add(type, RelationshipDirection.INCOMING, incoming);
        degrees.add(type, RelationshipDirection.LOOP, loop);

        // then
        assertThat(degrees.rawOutgoingDegree(type)).isEqualTo(outgoing);
        assertThat(degrees.rawIncomingDegree(type)).isEqualTo(incoming);
        assertThat(degrees.rawLoopDegree(type)).isEqualTo(loop);
        assertThat(degrees.outgoingDegree()).isEqualTo(outgoing + loop);
        assertThat(degrees.incomingDegree()).isEqualTo(incoming + loop);
    }

    @Test
    void shouldGetDegreeForMultipleType() {
        // given
        int numberOfTypes = random.nextInt(3, 10);
        int[][] expectedDegrees = new int[numberOfTypes][];
        int[] types = new int[numberOfTypes];
        for (int i = 0, prevType = 0; i < expectedDegrees.length; i++) {
            expectedDegrees[i] = new int[3];
            types[i] = prevType + random.nextInt(1, 100);
            prevType = types[i];
        }

        // when
        EagerDegrees degrees = new EagerDegrees();
        for (int i = 0; i < 100; i++) {
            int typeIndex = random.nextInt(numberOfTypes);
            int type = types[typeIndex];
            int outgoing = random.nextInt(1, 10);
            int incoming = random.nextInt(1, 10);
            int loop = random.nextInt(1, 10);
            switch (random.nextInt(4)) {
                case 0: // add all
                    degrees.add(type, outgoing, incoming, loop);
                    expectedDegrees[typeIndex][0] += outgoing;
                    expectedDegrees[typeIndex][1] += incoming;
                    expectedDegrees[typeIndex][2] += loop;
                    break;
                case 1: // add outgoing
                    degrees.addOutgoing(type, outgoing);
                    expectedDegrees[typeIndex][0] += outgoing;
                    break;
                case 2: // add incoming
                    degrees.addIncoming(type, incoming);
                    expectedDegrees[typeIndex][1] += incoming;
                    break;
                default: // add loop
                    degrees.addLoop(type, loop);
                    expectedDegrees[typeIndex][2] += loop;
                    break;
            }
        }

        // then
        for (int i = 0; i < numberOfTypes; i++) {
            int type = types[i];
            assertThat(degrees.outgoingDegree(type)).isEqualTo(expectedDegrees[i][0] + expectedDegrees[i][2]);
            assertThat(degrees.incomingDegree(type)).isEqualTo(expectedDegrees[i][1] + expectedDegrees[i][2]);
            assertThat(degrees.totalDegree(type))
                    .isEqualTo(expectedDegrees[i][0] + expectedDegrees[i][1] + expectedDegrees[i][2]);
        }
    }
}
