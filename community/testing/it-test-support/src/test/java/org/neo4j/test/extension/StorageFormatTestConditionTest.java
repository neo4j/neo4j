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
package org.neo4j.test.extension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.test.extension.StorageFormatTestCondition.getRegex;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.test.extension.StorageFormatTestCondition.StorageFormat;

class StorageFormatTestConditionTest {

    @ParameterizedTest
    @ValueSource(strings = {"block", "multiversion_block"})
    void blockShouldMatchEveryFormatBackedByTheBlockStorageEngine(String overrideFormat) {
        assertThat(matches(StorageFormat.BLOCK, overrideFormat)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"aligned", "standard", "high_limit", "spd", "spd_block"})
    void blockShouldNotMatchAnyOtherFormat(String overrideFormat) {
        assertThat(matches(StorageFormat.BLOCK, overrideFormat)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {":multiversion_block", "multiversion_", "notablock", "blocks"})
    void blockShouldNotMatchNearMissesOfAFormatName(String overrideFormat) {
        assertThat(matches(StorageFormat.BLOCK, overrideFormat)).isFalse();
    }

    @Test
    void alignedShouldMatchOnlyAligned() {
        assertThat(matches(StorageFormat.ALIGNED, "aligned")).isTrue();
        assertThat(matches(StorageFormat.ALIGNED, "block")).isFalse();
        assertThat(matches(StorageFormat.ALIGNED, "multiversion_block")).isFalse();
        assertThat(matches(StorageFormat.ALIGNED, "standard")).isFalse();
    }

    @Test
    void spdShouldMatchOnlySpd() {
        assertThat(matches(StorageFormat.SPD, "spd")).isTrue();
        assertThat(matches(StorageFormat.SPD, "spd_block")).isFalse();
        assertThat(matches(StorageFormat.SPD, "block")).isFalse();
        assertThat(matches(StorageFormat.SPD, "aligned")).isFalse();
    }

    /**
     * Mirrors how the condition itself applies the regex to the store format override.
     */
    private static boolean matches(StorageFormat storageFormat, String overrideFormat) {
        return overrideFormat.matches(getRegex(storageFormat));
    }
}
