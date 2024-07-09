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
package org.neo4j.internal.batchimport.input;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.defaultFormat;

import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.Monitor;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.kernel.impl.store.format.RecordFormats;

class EstimationSanityCheckerTest {
    @Test
    void shouldWarnAboutCountGettingCloseToCapacity() {
        // given
        RecordFormats formats = defaultFormat();
        Monitor monitor = mock(Monitor.class);
        Input.Estimates estimates = Input.knownEstimates(
                formats.node().getMaxId() - 1000,
                formats.relationship().getMaxId() - 1000,
                0,
                0,
                0,
                0,
                0); // we don't care about the rest of the estimates in this checking

        // when
        new EstimationSanityChecker(formats, monitor).sanityCheck(estimates);

        // then
        verify(monitor).mayExceedNodeIdCapacity(formats.node().getMaxId(), estimates.numberOfNodes());
        verify(monitor)
                .mayExceedRelationshipIdCapacity(formats.relationship().getMaxId(), estimates.numberOfRelationships());
    }

    @Test
    void shouldWarnAboutCountHigherThanCapacity() {
        // given
        RecordFormats formats = defaultFormat();
        Monitor monitor = mock(Monitor.class);
        Input.Estimates estimates = Input.knownEstimates(
                formats.node().getMaxId() * 2,
                formats.relationship().getMaxId() * 2,
                0,
                0,
                0,
                0,
                0); // we don't care about the rest of the estimates in this checking

        // when
        new EstimationSanityChecker(formats, monitor).sanityCheck(estimates);

        // then
        verify(monitor).mayExceedNodeIdCapacity(formats.node().getMaxId(), estimates.numberOfNodes());
        verify(monitor)
                .mayExceedRelationshipIdCapacity(formats.relationship().getMaxId(), estimates.numberOfRelationships());
    }

    @Test
    void shouldNotWantIfCountWayLowerThanCapacity() {
        // given
        RecordFormats formats = defaultFormat();
        Monitor monitor = mock(Monitor.class);
        Input.Estimates estimates = Input.knownEstimates(
                1000, 1000, 0, 0, 0, 0, 0); // we don't care about the rest of the estimates in this checking

        // when
        new EstimationSanityChecker(formats, monitor).sanityCheck(estimates);

        // then
        verifyNoMoreInteractions(monitor);
    }
}
