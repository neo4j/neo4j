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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.transaction.log.LogPosition;

class CheckPointThresholdDescriptionTest {
    @Test
    void shouldCallConsumerProvidingTheDescriptionWhenThresholdIsTrue() {
        // Given
        String description = "description";
        AbstractCheckPointThreshold threshold = new TheAbstractCheckPointThreshold(true, description);

        final AtomicReference<String> calledWith = new AtomicReference<>();
        // When
        LogPosition logPosition = new LogPosition(99, 100);
        threshold.isCheckPointingNeeded(42, logPosition, calledWith::set);

        // Then
        assertEquals(description, calledWith.get());
    }

    @Test
    void shouldNotCallConsumerProvidingTheDescriptionWhenThresholdIsFalse() {
        AbstractCheckPointThreshold threshold = new TheAbstractCheckPointThreshold(false, null);

        LogPosition logPosition = new LogPosition(1, 100);
        assertDoesNotThrow(() -> threshold.isCheckPointingNeeded(42, logPosition, s -> {
            throw new IllegalStateException("nooooooooo!");
        }));
    }

    private static class TheAbstractCheckPointThreshold extends AbstractCheckPointThreshold {
        private final boolean reached;

        TheAbstractCheckPointThreshold(boolean reached, String description) {
            super(description);
            this.reached = reached;
        }

        @Override
        public void initialize(long appendIndex, LogPosition logPosition) {}

        @Override
        public void checkPointHappened(long appendIndex, LogPosition logPosition) {}

        @Override
        public long checkFrequencyMillis() {
            return DEFAULT_CHECKING_FREQUENCY_MILLIS;
        }

        @Override
        protected boolean thresholdReached(long lastAppendIndex, LogPosition logPosition) {
            return reached;
        }
    }
}
