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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Panic;

class CheckpointerLifecycleTest {
    private final CheckPointer checkPointer = mock(CheckPointer.class);
    private final Panic databasePanic = mock(DatabaseHealth.class);
    private final CheckpointerLifecycle checkpointLifecycle = new CheckpointerLifecycle(checkPointer, databasePanic);

    @BeforeEach
    void setUp() {
        when(databasePanic.hasNoPanic()).thenReturn(true);
    }

    @Test
    void checkpointOnShutdown() throws Throwable {
        checkpointLifecycle.shutdown();

        verify(checkPointer).forceCheckPoint(any(TriggerInfo.class));
        verify(checkPointer).shutdown();
    }

    @Test
    void skipCheckpointOnShutdownByRequest() throws Throwable {
        checkpointLifecycle.setCheckpointOnShutdown(false);
        checkpointLifecycle.shutdown();

        verify(checkPointer).shutdown();
        verifyNoMoreInteractions(checkPointer);
    }
}
