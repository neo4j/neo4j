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
package org.neo4j.kernel.api;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.neo4j.kernel.database.DatabaseIdHelper;
import org.neo4j.kernel.database.NamedDatabaseId;

class DefaultElementIdMapperTest {

    @Test
    void elementIdForEntity() {
        var databaseId = DatabaseIdHelper.randomNamedDatabaseId();
        var idMapper = new DefaultElementIdMapperV1(databaseId);
        String databaseUUID = databaseId.databaseId().uuid().toString();
        assertEquals("5:" + databaseUUID + ":1", idMapper.relationshipElementId(1));
        assertEquals("5:" + databaseUUID + ":17857", idMapper.relationshipElementId(17857));
        assertEquals(
                "5:" + databaseUUID + ":" + (Long.MAX_VALUE - 10), idMapper.relationshipElementId(Long.MAX_VALUE - 10));

        assertEquals("4:" + databaseUUID + ":10", idMapper.nodeElementId(10));
        assertEquals("4:" + databaseUUID + ":47857", idMapper.nodeElementId(47857));
        assertEquals("4:" + databaseUUID + ":" + (Long.MAX_VALUE - 17), idMapper.nodeElementId(Long.MAX_VALUE - 17));
    }

    @Test
    void entityIdFromElementId() {
        var idMapper = new DefaultElementIdMapperV1(DatabaseIdHelper.randomNamedDatabaseId());
        long relationshipId = 17857;
        long nodeId = 784512;
        var relElementId = idMapper.relationshipElementId(relationshipId);
        var nodeElementId = idMapper.nodeElementId(nodeId);

        assertEquals(relationshipId, idMapper.relationshipId(relElementId));
        assertEquals(nodeId, idMapper.nodeId(nodeElementId));
    }

    @Test
    void failToDecodeIdsFromAnotherDatabase() {
        NamedDatabaseId expectedId = DatabaseIdHelper.randomNamedDatabaseId();
        var idMapper = new DefaultElementIdMapperV1(expectedId);

        String nonExpectedDbId =
                DatabaseIdHelper.randomNamedDatabaseId().databaseId().uuid().toString();
        assertThatThrownBy(() -> idMapper.relationshipId("5:" + nonExpectedDbId + ":1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not belong to the current database");
    }
}
