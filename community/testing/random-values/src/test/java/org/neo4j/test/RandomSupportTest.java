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
package org.neo4j.test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

class RandomSupportTest {
    @Test
    void testWithProbability() {
        final var randomSupport = spy(RandomSupport.class);
        randomSupport.reset();

        when(randomSupport.nextDouble()).thenReturn(0.19);
        var runnable = mock(Runnable.class);
        assertThat(randomSupport.withProbability(0.2, runnable)).isTrue();
        verify(runnable, times(1)).run();

        when(randomSupport.nextDouble()).thenReturn(0.21);
        runnable = mock(Runnable.class);
        assertThat(randomSupport.withProbability(0.2, runnable)).isFalse();
        verify(runnable, times(0)).run();
    }
}
