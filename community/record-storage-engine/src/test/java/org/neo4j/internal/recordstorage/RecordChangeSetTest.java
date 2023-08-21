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
package org.neo4j.internal.recordstorage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import org.junit.jupiter.api.Test;
import org.neo4j.storageengine.api.cursor.StoreCursors;

class RecordChangeSetTest {
    @Test
    void shouldStartWithSetsInitializedAndEmpty() {
        // GIVEN
        RecordChangeSet changeSet = new RecordChangeSet(
                mock(Loaders.class), INSTANCE, RecordAccess.LoadMonitor.NULL_MONITOR, StoreCursors.NULL);

        // WHEN
        // nothing really

        // THEN
        assertEquals(0, changeSet.getNodeRecords().changeSize());
        assertEquals(0, changeSet.getPropertyRecords().changeSize());
        assertEquals(0, changeSet.getRelRecords().changeSize());
        assertEquals(0, changeSet.getSchemaRuleChanges().changeSize());
        assertEquals(0, changeSet.getRelGroupRecords().changeSize());
    }
}
