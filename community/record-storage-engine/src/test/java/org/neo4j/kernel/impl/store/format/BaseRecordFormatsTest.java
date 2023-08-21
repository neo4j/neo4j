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
package org.neo4j.kernel.impl.store.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.Iterators.array;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.storageengine.api.format.CapabilityType;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class BaseRecordFormatsTest {
    private static final RecordStorageCapability[] CAPABILITIES = RecordStorageCapability.values();
    private static final CapabilityType[] CAPABILITY_TYPES = CapabilityType.values();
    private static final RecordStorageCapability additiveCapability = Arrays.stream(CAPABILITIES)
            .filter(RecordStorageCapability::isAdditive)
            .findAny()
            .orElse(null);
    private static final RecordStorageCapability nonAdditiveCapability = Arrays.stream(CAPABILITIES)
            .filter(capability -> !capability.isAdditive())
            .findAny()
            .orElse(null);

    @Inject
    private RandomSupport random;

    @Test
    void shouldReportCompatibilityBetweenTwoEqualSetsOfCapabilities() {
        // given
        RecordStorageCapability[] capabilities =
                random.selection(CAPABILITIES, CAPABILITIES.length / 2, CAPABILITIES.length, false);

        // then
        assertCompatible(capabilities, capabilities);
    }

    @Test
    void shouldReportCompatibilityForAdditiveAdditionalCapabilities() {
        assumeTrue(additiveCapability != null);
        assumeTrue(nonAdditiveCapability != null);

        // given
        RecordStorageCapability[] from = array(nonAdditiveCapability);
        RecordStorageCapability[] to = array(nonAdditiveCapability, additiveCapability);

        // then
        assertCompatible(from, to);
    }

    @Test
    void shouldReportIncompatibilityForChangingAdditionalCapabilities() {
        assumeTrue(nonAdditiveCapability != null);
        RecordStorageCapability anotherNonAdditiveCapability = Arrays.stream(CAPABILITIES)
                .filter(capability -> !capability.isAdditive() && !capability.equals(nonAdditiveCapability))
                .findAny()
                .orElse(null);
        assumeTrue(anotherNonAdditiveCapability != null);

        // given
        RecordStorageCapability[] from = array(nonAdditiveCapability);
        RecordStorageCapability[] to = array(nonAdditiveCapability, anotherNonAdditiveCapability);

        // then
        assertNotCompatible(
                from,
                to,
                Arrays.stream(CAPABILITY_TYPES)
                        .filter(anotherNonAdditiveCapability::isType)
                        .toList());
    }

    @Test
    void shouldReportIncompatibilityForAdditiveRemovedCapabilities() {
        assumeTrue(additiveCapability != null);
        assumeTrue(nonAdditiveCapability != null);

        // given
        RecordStorageCapability[] from = array(nonAdditiveCapability, additiveCapability);
        RecordStorageCapability[] to = array(nonAdditiveCapability);

        // then
        assertNotCompatible(
                from,
                to,
                Arrays.stream(CAPABILITY_TYPES)
                        .filter(additiveCapability::isType)
                        .toList());
    }

    private static void assertCompatible(RecordStorageCapability[] from, RecordStorageCapability[] to) {
        for (CapabilityType type : CAPABILITY_TYPES) {
            assertTrue(format(from).hasCompatibleCapabilities(format(to), type));
        }
    }

    private static void assertNotCompatible(
            RecordStorageCapability[] from, RecordStorageCapability[] to, List<CapabilityType> incompatibleTypes) {
        RecordFormats formatFrom = format(from);
        RecordFormats formatTo = format(to);
        for (CapabilityType type : CAPABILITY_TYPES) {
            assertEquals(!incompatibleTypes.contains(type), formatFrom.hasCompatibleCapabilities(formatTo, type));
        }
    }

    private static RecordFormats format(RecordStorageCapability... capabilities) {
        RecordFormats formats = mock(BaseRecordFormats.class);
        when(formats.capabilities()).thenReturn(capabilities);
        when(formats.hasCompatibleCapabilities(any(RecordFormats.class), any(CapabilityType.class)))
                .thenCallRealMethod();
        return formats;
    }
}
