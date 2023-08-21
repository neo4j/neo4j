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

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.store.record.NodeRecord;

class IntegrityValidatorTest {

    @Test
    void deletingNodeWithRelationshipsIsNotAllowed() {
        // Given
        NodeRecord record = new NodeRecord(1L).initialize(false, -1L, false, 1L, 0);
        record.setInUse(false);

        // When
        assertThrows(Exception.class, () -> IntegrityValidator.validateNodeRecord(record));
    }
}
