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
package org.neo4j.storageengine.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.io.fs.BufferBackedChannel;

class StoreIdTest {
    private final String ENGINE_1 = "storage-engine-1";
    private final String FORMAT_FAMILY_1 = "format-family-1";
    private final String ENGINE_2 = "storage-engine-2";
    private final String FORMAT_FAMILY_2 = "format-family-2";

    @Test
    void testCompatibilityCheck() {
        var storeId = new StoreId(1234, 789, ENGINE_1, FORMAT_FAMILY_1, 3, 7);
        assertTrue(storeId.isSameOrUpgradeSuccessor(new StoreId(1234, 789, ENGINE_1, FORMAT_FAMILY_1, 3, 7)));
        assertTrue(storeId.isSameOrUpgradeSuccessor(new StoreId(1234, 789, ENGINE_1, FORMAT_FAMILY_1, 3, 8)));
        assertTrue(storeId.isSameOrUpgradeSuccessor(new StoreId(1234, 789, ENGINE_1, FORMAT_FAMILY_1, 3, 15)));
        assertFalse(storeId.isSameOrUpgradeSuccessor(new StoreId(666, 789, ENGINE_1, FORMAT_FAMILY_1, 3, 7)));
        assertFalse(storeId.isSameOrUpgradeSuccessor(new StoreId(1234, 666, ENGINE_1, FORMAT_FAMILY_1, 3, 7)));
        assertFalse(storeId.isSameOrUpgradeSuccessor(new StoreId(1234, 789, ENGINE_1, FORMAT_FAMILY_1, 3, 6)));
        assertFalse(storeId.isSameOrUpgradeSuccessor(new StoreId(1234, 789, ENGINE_1, FORMAT_FAMILY_1, 4, 7)));
        assertFalse(storeId.isSameOrUpgradeSuccessor(new StoreId(1234, 789, ENGINE_1, FORMAT_FAMILY_1, 2, 7)));
        assertFalse(storeId.isSameOrUpgradeSuccessor(new StoreId(1234, 789, ENGINE_2, FORMAT_FAMILY_1, 3, 7)));
        assertFalse(storeId.isSameOrUpgradeSuccessor(new StoreId(1234, 789, ENGINE_1, FORMAT_FAMILY_2, 3, 7)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSerialization(boolean betaVersion) throws IOException {
        var buffer = new BufferBackedChannel(100);
        var storeId = new StoreId(1234, 789, ENGINE_1, FORMAT_FAMILY_1, betaVersion ? -3 : 3, 7);
        storeId.serialize(buffer);
        buffer.flip();
        var deserializedStoreId = StoreId.deserialize(buffer);
        assertEquals(storeId, deserializedStoreId);
    }

    @Test
    void betaVersionShouldGiveUserStringIndicatingBeta() {
        var storeId = new StoreId(1234, 789, ENGINE_1, FORMAT_FAMILY_1, -3, 7);
        assertThat(storeId.getStoreVersionUserString()).isEqualTo(ENGINE_1 + "-" + FORMAT_FAMILY_1 + "-3b.7");
    }
}
